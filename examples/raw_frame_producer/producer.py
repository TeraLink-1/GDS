"""Mock raw frame producer for testing the GDS pipeline.

This example service generates mock telemetry data and publishes it to the
Redis stream to test the decommutation and archiving services.
"""
import time
import random
import redis
from gds_shared.envelope import build_mock_raw_frame, envelope_to_redis, get_redis_url

r = redis.Redis.from_url(get_redis_url())

while True:
    env = build_mock_raw_frame("PWR_BUS_V", 30 + random.random()*2)
    r.xadd("gds:raw_frame", envelope_to_redis(env))
    time.sleep(1)
