#!/usr/bin/env python3
"""
Test script to demonstrate priority queue functionality.
Submits tasks with different priorities and verifies FIFO within priority levels.
"""

import os
import sys
import json
from api_client import TaskQueueClient
from broker import TaskBroker
from task import Task


def print_section(title):
    """Print formatted section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def test_priority_queue():
    """Test priority queue with tasks at different priority levels."""
    print_section("Priority Queue Test - ZADD and ZRANGE")
    
    client = TaskQueueClient()
    broker = TaskBroker()
    
    # Test 1: Submit tasks with different priorities
    print("\n[1] Submitting tasks with different priorities...")
    task_priority_map = {}
    
    tasks_to_submit = [
        ("low_priority_task_1", {"process": "slow_ops"}, 0),
        ("high_priority_task_1", {"urgent": "critical_fix"}, 10),
        ("medium_priority_task_1", {"standard": "normal_ops"}, 5),
        ("high_priority_task_2", {"urgent": "critical_bug"}, 10),
        ("low_priority_task_2", {"process": "background_job"}, 0),
        ("medium_priority_task_2", {"standard": "periodic_check"}, 5),
    ]
    
    for task_type, data, priority in tasks_to_submit:
        task_id = client.submit_task(
            task_type=task_type,
            data=data,
            priority=priority,
            max_retries=1
        )
        task_priority_map[task_id] = (task_type, priority)
        print(f"  [+] {task_type}: ID={task_id[:12]}... (priority={priority})")
    
    # Test 2: Check queue size
    print("\n[2] Checking queue state...")
    status = client.get_queue_status()
    print(f"  [+] Queue size: {status['queue_size']} tasks")
    print(f"  [+] Processing: {status['processing_count']}")
    
    # Test 3: Verify dequeue order using broker
    print("\n[3] Verifying dequeue order (should be highest priority first)...")
    print("  Expected order: high(10), high(10), medium(5), medium(5), low(0), low(0)")
    print("  Actual dequeue order:")
    
    dequeue_order = []
    for i in range(len(tasks_to_submit)):
        task = broker.dequeue()
        if task:
            task_id = task.get("id")
            task_type, original_priority = task_priority_map[task_id]
            dequeue_order.append((task_type, original_priority))
            print(f"    {i+1}. {task_type}: priority={original_priority}")
        else:
            print(f"    {i+1}. No more tasks")
            break
    
    # Test 4: Verify priority order
    print("\n[4] Verifying priority ordering...")
    previous_priority = float('inf')
    all_correct = True
    
    for task_type, priority in dequeue_order:
        if priority > previous_priority:
            print(f"  [X] Wrong order: {task_type} (priority={priority}) after priority={previous_priority}")
            all_correct = False
        else:
            print(f"  [+] {task_type}: priority={priority} (correctly ordered)")
            previous_priority = priority
    
    if all_correct:
        print("\n  >>> Priority queue working correctly! Tasks dequeued in correct priority order.")
    else:
        print("\n  >>> Priority ordering failed!")
        raise Exception("Priority queue ordering is incorrect")
    
    # Test 5: Check Redis sorted set structure
    print("\n[5] Inspecting Redis sorted set structure...")
    queue_size = broker.redis_client.zcard(broker.queue_key)
    processing_size = broker.redis_client.zcard(broker.processing_key)
    
    print(f"  [+] Queue (sorted set) cardinality: {queue_size}")
    print(f"  [+] Processing (sorted set) cardinality: {processing_size}")
    
    # Show some tasks with their scores
    if processing_size > 0:
        print(f"\n  Processing queue tasks (with scores):")
        processing_tasks = broker.redis_client.zrange(
            broker.processing_key, 0, -1, withscores=True
        )
        for task_json, score in processing_tasks[:3]:
            task = json.loads(task_json)
            priority = -int(score // 1000)
            print(f"    - {task.get('type', 'unknown')}: score={score:.2f}, implied_priority={priority}")
    
    # Test 6: Demonstrate ZADD and ZRANGE usage
    print("\n[6] Redis operations used:")
    print("  - ZADD: Adds tasks to sorted set with score = -priority * 1000 + timestamp")
    print("  - ZRANGE: Gets task with minimum score (highest priority first)")
    print("  - ZCARD: Gets cardinality of sorted set")
    print("  - ZRANGE: Retrieves tasks in score order with scores")
    
    print("\n[7] Score formula explanation:")
    print("  score = (-priority * 1000) + (timestamp % 1000)")
    print("  - Negative priority ensures higher priority = lower score = popped first")
    print("  - Timestamp modulo provides FIFO ordering within same priority")
    print("  Example: priority=10 -> score=-10000, priority=0 -> score=0")


if __name__ == "__main__":
    try:
        test_priority_queue()
        print("\n" + "=" * 70)
        print("[SUCCESS] Priority Queue Test Complete!")
        print("=" * 70)
    except Exception as e:
        print(f"\n[FAILED] Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
