"""Decommutation service for processing raw frames.

This service consumes RAW_FRAME messages from Redis streams, extracts telemetry
data, and publishes TM_POINT messages. Currently handles JSON-encoded mock data.
"""
import json
import base64
import logging
import redis
from gds_shared.envelope import (
    envelope_from_redis, envelope_to_redis,
    build_tm_point, get_redis_url, MsgType
)

logging.basicConfig(level=logging.INFO)
r = redis.Redis.from_url(get_redis_url())

STREAM_IN = "gds:raw_frame"
STREAM_OUT = "gds:tm"
GROUP = "cg:decomm"

def ensure_group() -> None:
    """Ensure Redis consumer group exists for the input stream."""
    try:
        r.xgroup_create(STREAM_IN, GROUP, id="0", mkstream=True)
    except Exception:
        pass

ensure_group()

while True:
    entries = r.xreadgroup(GROUP, "decomm-1", {STREAM_IN: ">"}, 10, 1000)
    for _, msgs in entries:
        for msg_id, fields in msgs:
            env = envelope_from_redis(fields)
            if env.msg_type != MsgType.RAW_FRAME:
                r.xack(STREAM_IN, GROUP, msg_id)
                continue

            data = json.loads(base64.b64decode(env.body["data_b64"]))

            out = build_tm_point(data["mnemonic"], data["value_num"])
            r.xadd(STREAM_OUT, envelope_to_redis(out))

            r.xack(STREAM_IN, GROUP, msg_id)
            logging.info(f"Decommed frame -> {data}")
