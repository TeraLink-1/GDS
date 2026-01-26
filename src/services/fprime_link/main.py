"""F Prime TCP/IP Link Handler with WebSocket Support.

This service implements a TCP/IP server that receives F Prime protocol frames
from flight software, parses and validates them, and publishes telemetry/events
to Redis streams. It also provides a WebSocket interface for bidirectional
communication, allowing commands to be sent to flight software and telemetry
to be received in real-time.
"""
import asyncio
import base64
import json
import logging
import os
import queue
import socket
import struct
import threading
import time
from typing import Dict, Optional, Set

import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from uvicorn import Config, Server

from gds_shared.envelope import (
    Envelope,
    MsgType,
    envelope_to_redis,
    get_redis_url,
    get_sat_id,
    get_gs_id,
)
from gds_shared.fprime_protocol import FPrimeFrame, FPrimeFrameParser, FPrimeFrameError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("fprime_link")

# Configuration
TCP_HOST = os.getenv("FPRIME_TCP_HOST", "0.0.0.0")
TCP_PORT = int(os.getenv("FPRIME_TCP_PORT", "50000"))
WS_PORT = int(os.getenv("FPRIME_WS_PORT", "50001"))
REDIS_URL = get_redis_url()
REDIS_STREAM_TM = "gds:tm"
REDIS_STREAM_RAW = "gds:raw_frame"

# Global state
redis_client = redis.Redis.from_url(REDIS_URL)
tcp_clients: Set[socket.socket] = set()
ws_clients: Set[WebSocket] = set()
tcp_lock = threading.Lock()
ws_lock = threading.Lock()
ws_message_queue: queue.Queue = queue.Queue()
ws_event_loop: Optional[asyncio.AbstractEventLoop] = None

# FastAPI app for WebSocket
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def parse_fprime_packet(payload: bytes) -> Dict:
    """Parse F Prime packet from payload.

    Extracts packet type and basic information from F Prime packet payload.
    F Prime packets contain a packet descriptor field that identifies the type.

    Args:
        payload: Raw packet payload bytes.

    Returns:
        Dictionary containing:
            - packet_type: String name of packet type (e.g., "TELEMETRY", "COMMAND")
            - descriptor: Numeric packet descriptor value
            - length: Payload length in bytes
            - raw_hex: Hexadecimal representation of payload
            - channel_id: (if telemetry) Channel identifier
            - event_id: (if log/event) Event identifier
    """
    if len(payload) < 1:
        return {"packet_type": "unknown", "descriptor": 0, "length": 0}
    
    # Packet descriptor is typically the first byte
    packet_descriptor = payload[0]
    
    # Map descriptor to packet type name
    packet_types = {
        0x00: "UNKNOWN",
        0x01: "COMMAND",
        0x02: "FILE",
        0x03: "LOG",  # Events
        0x04: "TELEMETRY",
        0x05: "PACKETIZED_TELEMETRY",
    }
    packet_type_name = packet_types.get(packet_descriptor, f"UNKNOWN_{packet_descriptor:02X}")
    
    result = {
        "packet_type": packet_type_name,
        "descriptor": packet_descriptor,
        "length": len(payload),
        "raw_hex": payload.hex(),
    }
    
    # For telemetry packets, try to extract basic info
    # Note: Full parsing requires F Prime dictionary with type definitions
    if packet_descriptor in (0x04, 0x05) and len(payload) >= 5:
        # Telemetry packets typically have:
        # - Descriptor (1 byte)
        # - Channel ID (4 bytes, U32)
        # - Time tag (optional)
        # - Value (variable)
        try:
            if len(payload) >= 5:
                channel_id = struct.unpack('>I', payload[1:5])[0]
                result["channel_id"] = channel_id
                result["has_telemetry"] = True
        except (struct.error, IndexError):
            pass
    
    # For log/event packets
    if packet_descriptor == 0x03 and len(payload) >= 5:
        try:
            if len(payload) >= 5:
                event_id = struct.unpack('>I', payload[1:5])[0]
                result["event_id"] = event_id
                result["has_event"] = True
        except (struct.error, IndexError):
            pass
    
    return result


def handle_fprime_frame(frame: FPrimeFrame, client_addr: tuple) -> None:
    """Handle a received F Prime frame.

    Processes the frame by parsing the packet, publishing raw frames to Redis,
    extracting telemetry if applicable, and broadcasting to WebSocket clients.

    Args:
        frame: Parsed F Prime frame object.
        client_addr: Tuple of (host, port) for the TCP client.
    """
    try:
        # Parse F Prime packet from payload
        packet_data = parse_fprime_packet(frame.payload)
        timestamp = int(time.time() * 1000)
        
        # Create envelope for raw frame (for archival/debugging)
        # Note: F Prime packets are binary, not JSON like the mock producer
        raw_envelope = Envelope(
            msg_type=MsgType.RAW_FRAME,
            sat_id=get_sat_id(),
            gs_id=get_gs_id(),
            seq=timestamp,
            body={
                "frame_format": "FPRIME_V1",
                "data_b64": base64.b64encode(frame.payload).decode('utf-8'),
                "packet_data": packet_data,
                "source": "FPRIME_TCP",
                "client_addr": f"{client_addr[0]}:{client_addr[1]}",
            }
        )
        
        # Publish raw frame to Redis stream (for archival)
        redis_client.xadd(REDIS_STREAM_RAW, envelope_to_redis(raw_envelope))
        logger.debug(f"Published F Prime raw frame from {client_addr}: {len(frame.payload)} bytes, type={packet_data.get('packet_type')}")
        
        # Extract and publish telemetry if this is a telemetry packet
        if packet_data.get("packet_type") in ("TELEMETRY", "PACKETIZED_TELEMETRY") and packet_data.get("has_telemetry"):
            try:
                channel_id = packet_data.get("channel_id", 0)
                
                # Extract value from telemetry packet
                # F Prime telemetry format: descriptor(1) + channel_id(4) + time_tag(optional) + value(variable)
                # For now, we'll create a basic telemetry point
                # Full parsing would require F Prime dictionary to know value type/size
                value = 0.0
                if len(frame.payload) > 5:
                    # Try to extract a numeric value (simplified - assumes float/double)
                    # Note: F Prime telemetry may have time tag, so value offset is variable
                    # This is a simplified parser - full implementation needs F Prime dictionary
                    try:
                        # Common case: 8-byte double after channel_id (skip time tag if present)
                        # Try double first (most common for telemetry)
                        if len(frame.payload) >= 13:
                            value = struct.unpack('>d', frame.payload[5:13])[0]
                        # Or 4-byte float (try float before integer to avoid false positives)
                        elif len(frame.payload) >= 9:
                            # Try float first
                            try:
                                value = float(struct.unpack('>f', frame.payload[5:9])[0])
                            except (struct.error, ValueError):
                                # If float fails, try signed integer
                                try:
                                    value = float(struct.unpack('>i', frame.payload[5:9])[0])
                                except (struct.error, ValueError):
                                    # Last resort: unsigned integer
                                    value = float(struct.unpack('>I', frame.payload[5:9])[0])
                    except (struct.error, ValueError, IndexError):
                        # If we can't parse, use channel_id as a placeholder value
                        value = float(channel_id)
                
                # Create telemetry point envelope
                tm_envelope = Envelope(
                    msg_type=MsgType.TM_POINT,
                    sat_id=get_sat_id(),
                    gs_id=get_gs_id(),
                    seq=timestamp,
                    body={
                        "mnemonic": f"FPRIME_CH_{channel_id}",
                        "value": value,
                        "source": "FPRIME_TCP",
                        "channel_id": channel_id,
                        "packet_type": packet_data.get("packet_type"),
                    }
                )
                
                # Publish directly to telemetry stream (bypassing decomm service)
                redis_client.xadd(REDIS_STREAM_TM, envelope_to_redis(tm_envelope))
                logger.info(f"Published F Prime telemetry: channel={channel_id}, value={value}")
                
            except Exception as e:
                logger.warning(f"Error extracting telemetry from F Prime packet: {e}")
        
        # Broadcast to WebSocket clients via queue
        try:
            message = {
                "type": "fprime_frame",
                "timestamp": timestamp,
                "payload_length": len(frame.payload),
                "packet_data": packet_data,
                "source": "tcp",
            }
            ws_message_queue.put(message, block=False)
        except queue.Full:
            logger.warning("WebSocket message queue full, dropping message")
        except Exception as e:
            logger.debug(f"Could not queue message for WebSocket clients: {e}")
        
    except Exception as e:
        logger.error(f"Error handling F Prime frame: {e}", exc_info=True)


async def broadcast_to_ws_clients(message: Dict) -> None:
    """Broadcast message to all connected WebSocket clients.

    Args:
        message: Dictionary to broadcast (will be JSON-encoded).
    """
    if not ws_clients:
        return
    
    message_json = json.dumps(message)
    disconnected = set()
    
    # Create a copy of clients to iterate over (to avoid modification during iteration)
    clients_copy = list(ws_clients)
    
    for ws in clients_copy:
        try:
            await ws.send_text(message_json)
        except Exception as e:
            logger.warning(f"Error sending to WebSocket client: {e}")
            disconnected.add(ws)
    
    # Remove disconnected clients
    if disconnected:
        async with ws_lock:
            ws_clients.difference_update(disconnected)


def send_fprime_frame_to_tcp(frame: FPrimeFrame) -> bool:
    """Send an F Prime frame to all connected TCP clients.

    Args:
        frame: F Prime frame to send.

    Returns:
        True if sent to at least one client, False if no clients connected.
    """
    frame_bytes = frame.to_bytes()
    sent = False
    
    with tcp_lock:
        disconnected = set()
        for client in tcp_clients:
            try:
                client.sendall(frame_bytes)
                sent = True
                logger.debug(f"Sent F Prime frame to TCP client: {len(frame_bytes)} bytes")
            except Exception as e:
                logger.warning(f"Error sending to TCP client: {e}")
                disconnected.add(client)
        
        # Remove disconnected clients
        tcp_clients.difference_update(disconnected)
    
    return sent


def handle_tcp_client(client_socket: socket.socket, client_addr: tuple) -> None:
    """Handle a TCP client connection.

    Receives F Prime frames from the client, parses them, and processes them.
    Runs in a separate thread per client connection.

    Args:
        client_socket: Client socket object.
        client_addr: Tuple of (host, port) for the client.
    """
    logger.info(f"F Prime TCP client connected: {client_addr}")
    
    with tcp_lock:
        tcp_clients.add(client_socket)
    
    parser = FPrimeFrameParser()
    
    try:
        while True:
            # Receive data
            data = client_socket.recv(4096)
            if not data:
                break
            
            # Parse frames
            frames = parser.feed(data)
            for frame in frames:
                handle_fprime_frame(frame, client_addr)
                
    except socket.error as e:
        logger.warning(f"TCP client {client_addr} error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error handling TCP client {client_addr}: {e}", exc_info=True)
    finally:
        with tcp_lock:
            tcp_clients.discard(client_socket)
        client_socket.close()
        logger.info(f"F Prime TCP client disconnected: {client_addr}")


def tcp_server_thread() -> None:
    """Run TCP server in a separate thread.

    Listens for incoming F Prime TCP connections and spawns a handler thread
    for each client. Runs until interrupted.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((TCP_HOST, TCP_PORT))
    server_socket.listen(5)
    server_socket.settimeout(1.0)  # Allow periodic checking for shutdown
    
    logger.info(f"F Prime TCP server listening on {TCP_HOST}:{TCP_PORT}")
    
    try:
        while True:
            try:
                client_socket, client_addr = server_socket.accept()
                # Handle each client in a separate thread
                client_thread = threading.Thread(
                    target=handle_tcp_client,
                    args=(client_socket, client_addr),
                    daemon=True
                )
                client_thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"Error accepting TCP connection: {e}", exc_info=True)
    except KeyboardInterrupt:
        logger.info("TCP server shutting down...")
    finally:
        server_socket.close()


@app.websocket("/fprime")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """WebSocket endpoint for F Prime bidirectional communication.

    Handles WebSocket connections for sending commands to flight software
    and receiving telemetry updates. Supports command messages and ping/pong.

    Args:
        websocket: WebSocket connection object.
    """
    await websocket.accept()
    
    async with ws_lock:
        ws_clients.add(websocket)
    
    logger.info(f"WebSocket client connected: {websocket.client}")
    
    try:
        while True:
            # Receive message from client
            message_text = await websocket.receive_text()
            
            try:
                message = json.loads(message_text)
                message_type = message.get("type")
                
                if message_type == "command":
                    # Handle command from WebSocket client
                    payload_hex = message.get("payload", "")
                    try:
                        payload_bytes = bytes.fromhex(payload_hex)
                        
                        # Create F Prime frame
                        frame = FPrimeFrame(payload_bytes)
                        
                        # Send to TCP clients
                        sent = send_fprime_frame_to_tcp(frame)
                        
                        # Send acknowledgment
                        await websocket.send_text(json.dumps({
                            "type": "command_ack",
                            "status": "sent" if sent else "no_clients",
                            "timestamp": int(time.time() * 1000),
                        }))
                        
                        logger.info(f"Command sent via WebSocket: {len(payload_bytes)} bytes")
                        
                    except ValueError as e:
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "message": f"Invalid payload hex: {e}",
                        }))
                    except Exception as e:
                        logger.error(f"Error sending command: {e}", exc_info=True)
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "message": str(e),
                        }))
                
                elif message_type == "ping":
                    # Respond to ping
                    await websocket.send_text(json.dumps({
                        "type": "pong",
                        "timestamp": int(time.time() * 1000),
                    }))
                
                else:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": f"Unknown message type: {message_type}",
                    }))
                    
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON",
                }))
            except Exception as e:
                logger.error(f"Error processing WebSocket message: {e}", exc_info=True)
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": str(e),
                }))
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected: {websocket.client}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
    finally:
        async with ws_lock:
            ws_clients.discard(websocket)


@app.get("/health")
async def health_check() -> dict:
    """Health check endpoint.

    Returns:
        Dictionary containing service status, client counts, and port information.
    """
    return {
        "status": "healthy",
        "tcp_clients": len(tcp_clients),
        "ws_clients": len(ws_clients),
        "tcp_port": TCP_PORT,
        "ws_port": WS_PORT,
    }


async def process_ws_message_queue() -> None:
    """Background task to process messages from queue and broadcast to WebSocket clients.

    Continuously processes messages from the thread-safe queue and broadcasts
    them to all connected WebSocket clients. Runs in the FastAPI event loop.
    """
    while True:
        try:
            # Get message from queue (with timeout to allow periodic checks)
            try:
                message = ws_message_queue.get(timeout=0.1)
                await broadcast_to_ws_clients(message)
            except queue.Empty:
                await asyncio.sleep(0.01)
                continue
        except Exception as e:
            logger.error(f"Error processing WebSocket message queue: {e}", exc_info=True)
            await asyncio.sleep(0.1)


@app.on_event("startup")
async def startup_event() -> None:
    """Startup event handler.

    Initializes the WebSocket message queue processor and starts the TCP server thread.
    """
    global ws_event_loop
    ws_event_loop = asyncio.get_running_loop()
    # Start background task to process message queue
    asyncio.create_task(process_ws_message_queue())


def run_uvicorn_server() -> None:
    """Run FastAPI/WebSocket server.

    Starts the Uvicorn ASGI server for the FastAPI application and WebSocket endpoints.
    """
    config = Config(app, host="0.0.0.0", port=WS_PORT, log_level="info")
    server = Server(config)
    server.run()


if __name__ == "__main__":
    # Start TCP server in background thread
    tcp_thread = threading.Thread(target=tcp_server_thread, daemon=True)
    tcp_thread.start()
    
    # Run WebSocket server in main thread
    logger.info(f"Starting F Prime Link service (TCP: {TCP_PORT}, WebSocket: {WS_PORT})")
    run_uvicorn_server()
