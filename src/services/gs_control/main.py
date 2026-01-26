"""Mock ground station control service.

This service simulates ground station command execution by consuming GS_CMD
messages and publishing GS_ACK acknowledgments. Used for testing and development.
"""
import logging
import redis
from gds_shared.envelope import (
    envelope_from_redis, envelope_to_redis,
    MsgType, get_redis_url, Envelope
)

logging.basicConfig(level=logging.INFO)
r = redis.Redis.from_url(get_redis_url())

STREAM_IN = "gds:gs_cmd"
STREAM_OUT = "gds:gs_ack"
GROUP = "cg:gs_control"

try: r.xgroup_create(STREAM_IN, GROUP, id="0", mkstream=True)
except Exception: pass

while True:
    entries = r.xreadgroup(GROUP, "gs-1", {STREAM_IN: ">"}, 10, 1000)
    for _, msgs in entries:
        for msg_id, fields in msgs:
            env = envelope_from_redis(fields)

            ack = Envelope(
                msg_type=MsgType.GS_ACK,
                sat_id=env.sat_id,
                gs_id=env.gs_id,
                seq=env.seq,
                body={
                    "cmd_id": env.body["cmd_id"],
                    "cmd_kind": env.body["cmd_kind"],
                    "state": "DONE",
                    "detail": "mock-executed"
                }
            )

            r.xadd(STREAM_OUT, envelope_to_redis(ack))
            r.xack(STREAM_IN, GROUP, msg_id)

            logging.info(f"ACK GS cmd {env.body['cmd_kind']}")
