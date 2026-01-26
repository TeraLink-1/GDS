"""Telemetry gateway service providing REST API and WebSocket interface.

This service provides HTTP endpoints for historical telemetry queries and
WebSocket connections for real-time telemetry updates. It bridges between
the GDS internal message streams and external clients.
"""
import asyncio
import json
import threading
import logging
import os
import time

import psycopg2
import redis
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

from gds_shared.envelope import (
    envelope_from_redis,
    MsgType,
    get_redis_url,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telemetry_gateway")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URL = os.getenv("DB_URL", "postgresql://gds_user:gds_password@postgres:5432/gds")
REDIS_URL = get_redis_url()

# Global Redis client
r = redis.Redis.from_url(REDIS_URL)

# WebSocket clients
clients = set()
event_loop = None  # will be set on startup


def get_db_conn() -> psycopg2.extensions.connection:
    """Get a fresh database connection with retry logic.

    Returns:
        PostgreSQL connection object.

    Note:
        This function will block until a connection is established.
    """
    while True:
        try:
            conn = psycopg2.connect(DB_URL)
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"DB not ready yet: {e}, retrying in 1s")
            time.sleep(1.0)


@app.get("/telemetry/history/{mnemonic}")
def history(mnemonic: str) -> list[dict]:
    """Get historical telemetry data for a mnemonic.

    Args:
        mnemonic: Telemetry mnemonic identifier to query.

    Returns:
        List of telemetry points with timestamp and value, ordered by time descending.
        Limited to 500 most recent points.
    """
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT extract(epoch from ts) * 1000.0 AS ts_ms, value
                FROM telemetry_points
                WHERE mnemonic = %s
                ORDER BY ts DESC
                LIMIT 500
                """,
                (mnemonic,),
            )
            rows = cur.fetchall()
        return [
            {"id": mnemonic, "timestamp": int(ts_ms), "value": val}
            for ts_ms, val in rows
        ]
    finally:
        conn.close()


@app.websocket("/realtime")
async def realtime(ws: WebSocket) -> None:
    """WebSocket endpoint for real-time telemetry updates.

    Clients connecting to this endpoint will receive telemetry updates as they
    are published to the Redis stream. Connection remains open until client disconnects.

    Args:
        ws: WebSocket connection object.
    """
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            # We don't expect messages from client right now; just keep the connection open
            await ws.receive_text()
    except Exception:
        pass
    finally:
        clients.discard(ws)


def redis_rt(loop: asyncio.AbstractEventLoop) -> None:
    """Redis consumer thread for real-time telemetry broadcasting.

    Consumes telemetry points from Redis stream and broadcasts them to all
    connected WebSocket clients. Runs in a separate thread to avoid blocking
    the FastAPI event loop.

    Args:
        loop: Asyncio event loop for scheduling WebSocket sends.
    """
    group = "cg:openmct_rt"
    stream = "gds:tm"

    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
        logger.info("Created consumer group %s on %s", group, stream)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info("Group %s already exists on %s", group, stream)
        else:
            logger.exception("Error creating group: %s", e)
            return

    while True:
        try:
            resp = r.xreadgroup(
                group, "rt-1", {stream: ">"}, count=50, block=1000
            )
            if not resp:
                continue

            for _, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        env = envelope_from_redis(fields)
                        if env.msg_type != MsgType.TM_POINT:
                            r.xack(stream, group, msg_id)
                            continue

                        msg = json.dumps(
                            {
                                "type": "telemetry",
                                "id": env.body["mnemonic"],
                                "timestamp": env.seq,
                                "value": env.body["value"],
                            }
                        )

                        for ws in list(clients):
                            asyncio.run_coroutine_threadsafe(
                                ws.send_text(msg), loop
                            )
                        r.xack(stream, group, msg_id)
                    except Exception as e:
                        logger.exception("Error processing TM_POINT: %s", e)
                        r.xack(stream, group, msg_id)
        except Exception as e:
            logger.exception("Redis realtime loop error: %s", e)
            time.sleep(1.0)


@app.on_event("startup")
async def startup() -> None:
    """Startup event handler.

    Initializes the Redis consumer thread for real-time telemetry broadcasting.
    """
    global event_loop
    event_loop = asyncio.get_running_loop()
    thread = threading.Thread(
        target=redis_rt, args=(event_loop,), daemon=True
    )
    thread.start()
    logger.info("Started Redis realtime consumer thread")
