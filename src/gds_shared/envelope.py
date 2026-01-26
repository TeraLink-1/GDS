"""Message envelope system for inter-service communication.

This module provides standardized message formats for communication between
GDS microservices via Redis Streams. It includes message type definitions,
envelope serialization, and utility functions for creating common message types.
"""
import os
import json
import base64
import time
from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict


class MsgType(str, Enum):
    """Message type enumeration for GDS message envelopes.

    Attributes:
        RAW_FRAME: Raw frame data from flight software.
        TM_POINT: Telemetry point with mnemonic and value.
        ALARM: Alarm or alert message.
        CMD_REQ: Command request.
        CMD_ACK: Command acknowledgment.
        GS_CMD: Ground station command.
        GS_ACK: Ground station acknowledgment.
    """
    RAW_FRAME = "RAW_FRAME"
    TM_POINT = "TM_POINT"
    ALARM = "ALARM"
    CMD_REQ = "CMD_REQ"
    CMD_ACK = "CMD_ACK"
    GS_CMD = "GS_CMD"
    GS_ACK = "GS_ACK"


def get_sat_id() -> str:
    """Get satellite identifier from environment variable.

    Returns:
        Satellite identifier string, defaults to "TERALINK-1" if not set.
    """
    return os.getenv("SAT_ID", "TERALINK-1")


def get_gs_id() -> str:
    """Get ground station identifier from environment variable.

    Returns:
        Ground station identifier string, defaults to "GS-DEMO" if not set.
    """
    return os.getenv("GS_ID", "GS-DEMO")


def get_redis_url() -> str:
    """Get Redis connection URL from environment variable.

    Returns:
        Redis connection URL string, defaults to "redis://localhost:6379/0" if not set.
    """
    return os.getenv("REDIS_URL", "redis://localhost:6379/0")


@dataclass
class Envelope:
    """Message envelope for inter-service communication.

    Attributes:
        msg_type: Type of message (from MsgType enum).
        sat_id: Satellite identifier.
        gs_id: Ground station identifier.
        seq: Sequence number (typically timestamp in milliseconds).
        body: Message body containing type-specific data.
    """
    msg_type: MsgType
    sat_id: str
    gs_id: str
    seq: int
    body: Dict[str, Any]


def envelope_to_redis(env: Envelope) -> Dict[str, str]:
    """Convert envelope to Redis Stream format.

    Args:
        env: Envelope to serialize.

    Returns:
        Dictionary with "payload" key containing JSON-encoded envelope data.
    """
    return {"payload": json.dumps({
        "msg_type": env.msg_type,
        "sat_id": env.sat_id,
        "gs_id": env.gs_id,
        "seq": env.seq,
        "body": env.body,
        "ts_recv": int(time.time() * 1000),
        "schema_ver": 1
    })}


def envelope_from_redis(fields: Dict[bytes, bytes]) -> Envelope:
    """Parse envelope from Redis Stream format.

    Args:
        fields: Dictionary of Redis Stream field bytes.

    Returns:
        Parsed Envelope object.

    Raises:
        KeyError: If "payload" field is missing.
        json.JSONDecodeError: If payload is not valid JSON.
    """
    payload = json.loads(fields[b"payload"].decode())
    return Envelope(
        msg_type=MsgType(payload["msg_type"]),
        sat_id=payload["sat_id"],
        gs_id=payload["gs_id"],
        seq=payload["seq"],
        body=payload["body"],
    )


def build_mock_raw_frame(mnemonic: str, value: float) -> Envelope:
    """Build a mock raw frame envelope for testing.

    Args:
        mnemonic: Telemetry mnemonic identifier.
        value: Numeric telemetry value.

    Returns:
        Envelope with RAW_FRAME message type containing base64-encoded JSON data.
    """
    body = {
        "frame_format": "MOCK_JSON_V1",
        "data_b64": base64.b64encode(
            json.dumps({"mnemonic": mnemonic, "value_num": value}).encode()
        ).decode()
    }
    return Envelope(
        msg_type=MsgType.RAW_FRAME,
        sat_id=get_sat_id(),
        gs_id=get_gs_id(),
        seq=int(time.time()*1000),
        body=body,
    )


def build_tm_point(mnemonic: str, value: float) -> Envelope:
    """Build a telemetry point envelope.

    Args:
        mnemonic: Telemetry mnemonic identifier.
        value: Numeric telemetry value.

    Returns:
        Envelope with TM_POINT message type.
    """
    body = {
        "mnemonic": mnemonic,
        "value": value,
        "source": "DECOMM",
        "body_ver": 1
    }
    return Envelope(
        msg_type=MsgType.TM_POINT,
        sat_id=get_sat_id(),
        gs_id=get_gs_id(),
        seq=int(time.time()*1000),
        body=body,
    )
