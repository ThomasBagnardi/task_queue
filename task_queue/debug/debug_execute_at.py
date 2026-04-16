#!/usr/bin/env python3
"""
Detailed debug of execute_at handling.
"""

import time
import json
from datetime import datetime, timezone, timedelta
from task import Task

print("Testing Task.execute_at handling...")

now = datetime.now(timezone.utc)
now_ts = time.time()

print(f"\nTime references:")
print(f"  datetime.now(timezone.utc): {now}")
print(f"  time.time():       {now_ts}")
print(f"  now.timestamp():   {now.timestamp()}")

execute_at = now + timedelta(seconds=1)
print(f"\nCreating Task with execute_at = now + 1 second:")
print(f"  execute_at datetime: {execute_at}")
print(f"  execute_at.timestamp(): {execute_at.timestamp()}")

# Create the task
task = Task(
    task_type="test",
    data={},
    priority=0,
    execute_at=execute_at
)

print(f"\nTask object:")
print(f"  task.execute_at: {task.execute_at}")
print(f"  task.execute_at type: {type(task.execute_at)}")
print(f"  task.execute_at.timestamp(): {task.execute_at.timestamp()}")

# Convert to dict
task_dict = task.to_dict()
print(f"\nTask.to_dict():")
print(f"  'execute_at' value: {task_dict.get('execute_at')}")
print(f"  'execute_at' type: {type(task_dict.get('execute_at'))}")

# Simulate what broker does
task_json = json.dumps(task_dict)
print(f"\njson.dumps(task_dict):")
print(f"  JSON string (truncated): {task_json[:200]}...")

# Simulate what broker unpacks
loaded_dict = json.loads(task_json)
print(f"\njson.loads():")
print(f"  'execute_at' value: {loaded_dict.get('execute_at')}")

# Simulate datetime parsing
if loaded_dict.get("execute_at"):
    parsed_dt = datetime.fromisoformat(loaded_dict["execute_at"])
    print(f"\nParsed datetime:")
    print(f"  parsed_dt: {parsed_dt}")
    print(f"  parsed_dt.timestamp(): {parsed_dt.timestamp()}")
    print(f"  Original execute_at.timestamp(): {execute_at.timestamp()}")
    print(f"  Difference: {parsed_dt.timestamp() - execute_at.timestamp()}")

# Now test in broker context
print("\n" + "="*70)
print("Testing in broker context...")

from broker import TaskBroker

broker = TaskBroker()
broker.redis_client.delete(broker.delayed_queue_key)

# This is what broker.enqueue does
task_dict_2 = task.to_dict()
execute_at_str = task_dict_2.get("execute_at")

print(f"\nBroker.enqueue logic:")
print(f"  execute_at_str: {execute_at_str}")

if execute_at_str:
    try:
        parsed_from_str = datetime.fromisoformat(execute_at_str)
        parsed_ts = parsed_from_str.timestamp()
        print(f"  Parsed datetime: {parsed_from_str}")
        print(f"  Parsed timestamp: {parsed_ts}")
        print(f"  Expected timestamp (original): {execute_at.timestamp()}")
        print(f"  Match? {parsed_ts == execute_at.timestamp()}")
        
        # Store in Redis
        current_time = time.time()
        print(f"\nRedis storage:")
        print(f"  current_time: {current_time}")
        print(f"  parsed_ts: {parsed_ts}")
        print(f"  parsed_ts > current_time? {parsed_ts > current_time}")
        
        # Add to delayed queue
        broker.redis_client.zadd(
            broker.delayed_queue_key,
            {json.dumps(task_dict_2): parsed_ts}
        )
        
        # Try to retrieve
        print(f"\nRedis retrieval:")
        all_tasks = broker.redis_client.zrange(broker.delayed_queue_key, 0, -1, withscores=True)
        for task_j, score in all_tasks:
            print(f"  Score in Redis: {score}")
            task_from_redis = json.loads(task_j)
            print(f"  execute_at from task: {task_from_redis.get('execute_at')}")

    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
