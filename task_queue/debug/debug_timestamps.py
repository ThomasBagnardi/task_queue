#!/usr/bin/env python3
"""
Debug test to check timestamp handling in promote_ready_delayed_tasks.
"""

import time
import json
from datetime import datetime, timezone, timedelta
from broker import TaskBroker
from task import Task

broker = TaskBroker()

# Clear both queues
broker.redis_client.delete(broker.queue_key)
broker.redis_client.delete(broker.delayed_queue_key)

print("System current time check:")
now = datetime.now(timezone.utc)
now_ts = time.time()
print(f"  datetime.now(timezone.utc): {now}")
print(f"  time.time(): {now_ts}")

print("\nCreating a delayed task with 1-second delay...")
execute_at = now + timedelta(seconds=1)
execute_at_ts = execute_at.timestamp()

print(f"  execute_at datetime: {execute_at}")
print(f"  execute_at timestamp: {execute_at_ts}")
print(f"  Difference (should be ~1.0): {execute_at_ts - now_ts}")

# Create a task and add it to delayed queue
task = Task(
    task_type="test_delayed",
    data={"test": True},
    priority=1,
    execute_at=execute_at
)

task_dict = task.to_dict()
task_json = json.dumps(task_dict)

print(f"\nTask created:")
print(f"  ID: {task.id}")
print(f"  execute_at field: {task_dict.get('execute_at')}")

print(f"\nAdding task to delayed_queue_key with score={execute_at_ts}...")
broker.redis_client.zadd(
    broker.delayed_queue_key,
    {task_json: execute_at_ts}
)

delayed_size = broker.get_delayed_queue_size()
print(f"Delayed queue size: {delayed_size}")

print("\nAttempt 1: Check for ready tasks (should be 0, task not ready yet)...")
current_ts_1 = time.time()
print(f"  Current timestamp: {current_ts_1}")
print(f"  Task execute_at: {execute_at_ts}")
print(f"  Task ready? {execute_at_ts <= current_ts_1}")

# Query directly to debug
ready = broker.redis_client.zrangebyscore(
    broker.delayed_queue_key, 0, current_ts_1
)
print(f"  ZRANGEBYSCORE(0, {current_ts_1}): {len(ready)} results")

promoted = broker.promote_ready_delayed_tasks()
print(f"  Promoted: {promoted} tasks")

print("\nWaiting 1.5 seconds...")
time.sleep(1.5)

print("\nAttempt 2: Check for ready tasks (should be 1, task is now ready)...")
current_ts_2 = time.time()
print(f"  Current timestamp: {current_ts_2}")
print(f"  Task execute_at: {execute_at_ts}")
print(f"  Task ready? {execute_at_ts <= current_ts_2}")

# Query directly to debug
ready = broker.redis_client.zrangebyscore(
    broker.delayed_queue_key, 0, current_ts_2
)
print(f"  ZRANGEBYSCORE(0, {current_ts_2}): {len(ready)} results")

promoted = broker.promote_ready_delayed_tasks()
active_size = broker.get_queue_size()
delayed_size = broker.get_delayed_queue_size()
print(f"  Promoted: {promoted} tasks")
print(f"  Active queue size: {active_size}")
print(f"  Delayed queue size: {delayed_size}")

if promoted == 1 and delayed_size == 0:
    print("\n[SUCCESS] Task promotion works correctly!")
else:
    print("\n[FAILED] Task promotion did not work as expected")
