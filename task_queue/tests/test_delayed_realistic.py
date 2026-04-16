#!/usr/bin/env python3
"""
Test script for delayed task promotion with realistic timestamps.
Uses near-future delays to demonstrate actual scheduler operation.
"""

import os
import sys
import json
import time
from datetime import datetime, timezone, timedelta
from producer import TaskProducer
from task import Task
from broker import TaskBroker
from worker import TaskWorker, TaskExecutor, WorkerConfig


def print_section(title):
    """Print formatted section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def test_delayed_promotion_realistic():
    """Test delayed task promotion with near-future timestamps."""
    print_section("Delayed Tasks - Realistic Promotion Test")
    
    broker = TaskBroker()
    
    print("\n[SETUP] Creating delayed tasks with 1, 2, and 3 second delays...")
    current_time = datetime.now(timezone.utc)
    
    delayed_tasks = []
    
    for i, delay_secs in enumerate([1, 2, 3]):
        execute_at = current_time + timedelta(seconds=delay_secs)
        
        task = Task(
            task_type=f"delayed_task_{i}",
            data={"delay_seconds": delay_secs, "index": i},
            priority=0,
            execute_at=execute_at
        )
        
        # Store directly in delayed queue
        task_dict = task.to_dict()
        broker.redis_client.zadd(
            broker.delayed_queue_key,
            {json.dumps(task_dict): execute_at.timestamp()}
        )
        broker.set_task_status(task.id, "pending", task.created_at.isoformat())
        
        delayed_tasks.append((task.id, execute_at, delay_secs))
        print(f"  [+] Task {i}: {task.id[:12]}... (delay={delay_secs}s, execute_at={execute_at.isoformat()})")
    
    # Test promotion before any delays expire
    print("\n[TEST 1] Checking delayed queue - all tasks future-scheduled...")
    delayed_size_0 = broker.get_delayed_queue_size()
    active_size_0 = broker.get_queue_size()
    print(f"  [+] Delayed queue: {delayed_size_0} tasks")
    print(f"  [+] Active queue: {active_size_0} tasks")
    
    promoted_0 = broker.promote_ready_delayed_tasks()
    print(f"  [+] Promotion attempt 1: {promoted_0} tasks promoted (expected 0)")
    
    # Wait 1.2 seconds - task 0 should be ready
    print("\n[TEST 2] Waiting 1.2 seconds (task 0 should be ready)...")
    time.sleep(1.2)
    
    promoted_1 = broker.promote_ready_delayed_tasks()
    active_size_1 = broker.get_queue_size()
    delayed_size_1 = broker.get_delayed_queue_size()
    
    print(f"  [+] Promotion attempt 2: {promoted_1} task(s) promoted (expected 1)")
    print(f"  [+] Active queue: {active_size_1} tasks (was {active_size_0}, +{active_size_1 - active_size_0})")
    print(f"  [+] Delayed queue: {delayed_size_1} tasks (was {delayed_size_0}, -{delayed_size_0 - delayed_size_1})")
    
    # Wait another second - task 1 should be ready
    print("\n[TEST 3] Waiting 1 more second (task 1 should be ready)...")
    time.sleep(1.0)
    
    promoted_2 = broker.promote_ready_delayed_tasks()
    active_size_2 = broker.get_queue_size()
    delayed_size_2 = broker.get_delayed_queue_size()
    
    print(f"  [+] Promotion attempt 3: {promoted_2} task(s) promoted (expected 1)")
    print(f"  [+] Active queue: {active_size_2} tasks (was {active_size_1}, +{active_size_2 - active_size_1})")
    print(f"  [+] Delayed queue: {delayed_size_2} tasks (was {delayed_size_1}, -{delayed_size_1 - delayed_size_2})")
    
    # Wait another second - task 2 should be ready
    print("\n[TEST 4] Waiting 1 more second (task 2 should be ready)...")
    time.sleep(1.0)
    
    promoted_3 = broker.promote_ready_delayed_tasks()
    active_size_3 = broker.get_queue_size()
    delayed_size_3 = broker.get_delayed_queue_size()
    
    print(f"  [+] Promotion attempt 4: {promoted_3} task(s) promoted (expected 1)")
    print(f"  [+] Active queue: {active_size_3} tasks (was {active_size_2}, +{active_size_3 - active_size_2})")
    print(f"  [+] Delayed queue: {delayed_size_3} tasks (was {delayed_size_2}, -{delayed_size_2 - delayed_size_3})")
    
    # Verify all tasks promoted
    print("\n[RESULTS] Summary:")
    print(f"  [+] Total promotions: {promoted_0 + promoted_1 + promoted_2 + promoted_3}")
    print(f"  [+] Expected:         3 tasks")
    print(f"  [+] Final state:")
    print(f"      - Active queue: {active_size_3} tasks")
    print(f"      - Delayed queue: {delayed_size_3} tasks (should be 0)")
    
    # Validate
    total_promoted = promoted_0 + promoted_1 + promoted_2 + promoted_3
    if total_promoted == 3 and (active_size_3 - active_size_0) == 3:
        print("\n  [SUCCESS] All delayed tasks were promoted on schedule!")
        return True
    else:
        print(f"\n  [FAILED] Expected 3 promotions, got {total_promoted}")
        return False


def test_scheduler_thread_integration():
    """Test that scheduler thread properly polls and promotes tasks."""
    print_section("Scheduler Thread Integration Test")
    
    broker = TaskBroker()
    
    # Clear queues
    broker.redis_client.delete(broker.queue_key)
    broker.redis_client.delete(broker.delayed_queue_key)
    
    print("\n[SETUP] Creating delayed tasks with 2-second delay...")
    current_time = datetime.now(timezone.utc)
    execute_at = current_time + timedelta(seconds=2)
    
    task = Task(
        task_type="scheduled_payment",
        data={"amount": 100},
        priority=1,
        execute_at=execute_at
    )
    
    task_dict = task.to_dict()
    broker.redis_client.zadd(
        broker.delayed_queue_key,
        {json.dumps(task_dict): execute_at.timestamp()}
    )
    
    print(f"  [+] Task scheduled: execute_at={execute_at.isoformat()}")
    
    print("\n[TEST] Starting worker with scheduler thread...")
    config = WorkerConfig(scheduler_interval=0.5)  # Check every 0.5s
    executor = TaskExecutor()
    worker = TaskWorker(executor, config)
    
    initial_stats = worker.get_stats()
    print(f"  [+] Initial delayed_size: {initial_stats.get('delayed_size', 'N/A')}")
    
    print("\n[SCHEDULER] Starting background scheduler thread...")
    worker.start_scheduler()
    print(f"  [+] Scheduler thread started (interval={config.scheduler_interval}s)")
    
    try:
        # Monitor promotion
        print("\n[MONITORING] Checking for task promotion...")
        promoted_count = 0
        
        for attempt in range(6):  # Check up to 3 seconds
            time.sleep(0.5)
            stats = worker.get_stats()
            delayed_size = stats.get('delayed_size', 0)
            print(f"  [{attempt+1}] After {(attempt+1)*0.5:.1f}s: delayed_size={delayed_size}", end="")
            
            if delayed_size == 0:
                print(" -> PROMOTED!")
                promoted_count += 1
                break
            else:
                print()
        
        if promoted_count > 0:
            print("\n  [SUCCESS] Scheduler thread promoted the delayed task!")
            return True
        else:
            print("\n  [FAILED] Task was not promoted by scheduler thread")
            return False
    
    finally:
        print("\n[CLEANUP] Stopping scheduler thread...")
        worker.stop_scheduler()
        print("  [+] Scheduler thread stopped")


def main():
    """Run all tests."""
    print("\n")
    print("*" * 70)
    print("* DELAYED TASKS COMPREHENSIVE TEST SUITE")
    print("*" * 70)
    
    try:
        # Test 1: Realistic promotion timing
        result1 = test_delayed_promotion_realistic()
        
        # Test 2: Scheduler thread
        result2 = test_scheduler_thread_integration()
        
        # Summary
        print_section("TEST SUMMARY")
        print(f"  [1] Delayed promotion timing:    {'PASS' if result1 else 'FAIL'}")
        print(f"  [2] Scheduler thread:            {'PASS' if result2 else 'FAIL'}")
        print()
        
        if result1 and result2:
            print("  [OVERALL] All tests PASSED!")
            return 0
        else:
            print("  [OVERALL] Some tests FAILED")
            return 1
    
    except Exception as e:
        print(f"\n[ERROR] Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
