"""Archiver service for persisting telemetry to PostgreSQL.

This service consumes TM_POINT messages from Redis streams and stores them
in the PostgreSQL database for historical analysis and querying.
"""
import logging
import os
import time

import psycopg2
import redis

from gds_shared.envelope import (
    envelope_from_redis,
    MsgType,
    get_redis_url,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("archiver")

REDIS_URL = get_redis_url()
DB_URL = os.getenv("DB_URL", "postgresql://gds_user:gds_password@postgres:5432/gds")

r = redis.Redis.from_url(REDIS_URL)

STREAMS = ["gds:tm"]
GROUP = "cg:archiver"


def get_db_conn() -> psycopg2.extensions.connection:
    """Get a database connection with retry logic.

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


def ensure_groups() -> None:
    """Ensure Redis consumer groups exist for all streams.

    Creates consumer groups if they don't exist, logging appropriate messages
    for success or if groups already exist.
    """
    for s in STREAMS:
        try:
            r.xgroup_create(s, GROUP, id="0", mkstream=True)
            logger.info("Created group %s on %s", GROUP, s)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info("Group %s already exists on %s", GROUP, s)
            else:
                logger.exception("Error creating group: %s", e)


def main() -> None:
    """Main archiver loop.

    Continuously reads telemetry points from Redis streams and persists them
    to PostgreSQL. Handles database reconnection automatically.
    """
    ensure_groups()
    conn = get_db_conn()
    cur = conn.cursor()

    while True:
        try:
            resp = r.xreadgroup(
                GROUP,
                "archiver-1",
                {s: ">" for s in STREAMS},
                count=50,
                block=1000,
            )
            if not resp:
                continue

            for stream_name, msgs in resp:
                sname = stream_name.decode()
                for msg_id, fields in msgs:
                    try:
                        env = envelope_from_redis(fields)
                        if env.msg_type == MsgType.TM_POINT:
                            cur.execute(
                                """
                                INSERT INTO telemetry_points (mnemonic, ts, value, sat_id, source)
                                VALUES (%s, NOW(), %s, %s, %s)
                                """,
                                (
                                    env.body["mnemonic"],
                                    env.body["value"],
                                    env.sat_id,
                                    env.body.get("source", "DECOMM"),
                                ),
                            )
                            conn.commit()
                        r.xack(sname, GROUP, msg_id)
                    except psycopg2.InterfaceError:
                        # Lost DB connection; reconnect
                        logger.warning("DB connection lost, reconnecting...")
                        conn = get_db_conn()
                        cur = conn.cursor()
                    except Exception as e:
                        logger.exception("Error in archiver: %s", e)
                        r.xack(sname, GROUP, msg_id)
        except Exception as e:
            logger.exception("Archiver loop error: %s", e)
            time.sleep(1.0)


if __name__ == "__main__":
    main()
