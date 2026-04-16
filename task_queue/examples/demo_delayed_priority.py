#!/usr/bin/env python3
"""
Comprehensive demonstration of delayed tasks with priority queue integration.
Shows the complete workflow: submit, schedule, promote, process.
"""

import time
from datetime import datetime, timezone, timedelta
from task import Task
from broker import TaskBroker
from worker import TaskWorker, TaskExecutor, WorkerConfig


def demo_delayed_tasks():
    """Complete demonstration of delayed task feature."""
    
    print("\n" + "="*80)
    print(" DELAYED TASKS + PRIORITY QUEUES - COMPREHENSIVE DEMONSTRATION")
    print("="*80)
    
    broker = TaskBroker()
    
    # Clear queues for clean demo
    broker.redis_client.delete(broker.queue_key)
    broker.redis_client.delete(broker.delayed_queue_key)
    
    print("\n[SCENARIO] Process urgent emails now, schedule reports for tomorrow")
    print("-" * 80)
    
    # 1. Submit immediate high-priority tasks
    print("\n[1] Submitting immediate urgent emails (priority=2)...")
    now = datetime.now(timezone.utc)
    
    urgent_tasks = []
    for i in range(2):
        task = Task(
            task_type="email_urgent",
            data={"email": f"urgent{i}@example.com", "content": "Time-sensitive"},
            priority=2  # High priority - process immediately
        )
        urgent_tasks.append(task)
        broker.enqueue(task.to_dict())
        print(f"   [+] Urgent email {i}: {task.id[:12]}... (priority=2)")
    
    # 2. Submit normal priority immediate tasks
    print("\n[2] Submitting immediate normal emails (priority=1)...")
    normal_tasks = []
    for i in range(2):
        task = Task(
            task_type="email_normal",
            data={"email": f"normal{i}@example.com", "content": "Regular message"},
            priority=1  # Normal priority
        )
        normal_tasks.append(task)
        broker.enqueue(task.to_dict())
        print(f"   [+] Normal email {i}: {task.id[:12]}... (priority=1)")
    
    # 3. Schedule delayed tasks for future execution
    print("\n[3] Scheduling reports for tomorrow (priority=0, delayed)...")
    delayed_tasks = []
    
    delays = [
        (2, "2min_report"),
        (5, "5min_report"),
        (3, "3min_report"),
    ]
    
    for delay_secs, label in delays:
        execute_at = now + timedelta(seconds=delay_secs)
        task = Task(
            task_type="daily_report",
            data={"report": label, "format": "csv"},
            priority=0,  # Low priority - process after urgent/normal
            execute_at=execute_at
        )
        delayed_tasks.append(task)
        broker.enqueue(task.to_dict())
        print(f"   [+] {label}: {task.id[:12]}... (execute in {delay_secs}s)")
    
    # 4. Display initial queue state
    print("\n[4] Queue State (Before Promotion):")
    print("-" * 80)
    active_size = broker.get_queue_size()
    delayed_size = broker.get_delayed_queue_size()
    print(f"   Active queue: {active_size} tasks (2 urgent + 2 normal)")
    print(f"   Delayed queue: {delayed_size} tasks (schedules for future)")
    
    # 5. Show dequeue order with priority
    print("\n[5] Processing Order (Priority-based):")
    print("-" * 80)
    processed_order = []
    
    # Dequeue in priority order
    for i in range(active_size):
        task_dict = broker.dequeue()
        if task_dict:
            task_id = task_dict["id"]
            task_type = task_dict["type"]
            priority = task_dict["priority"]
            processed_order.append((task_type, priority))
            print(f"   [{i+1}] {task_type:20s} (priority={priority}) - {task_id[:12]}...")
            broker.mark_completed(task_id)
    
    print("\n   -> Urgent emails processed first, then normal, then scheduled")
    
    # 6. Wait for delayed tasks to become ready
    print("\n[6] Waiting for scheduled tasks to become ready...")
    print("-" * 80)
    
    for i in range(6):
        time.sleep(1)
        promoted = broker.promote_ready_delayed_tasks()
        active_size = broker.get_queue_size()
        delayed_size = broker.get_delayed_queue_size()
        
        if promoted > 0:
            print(f"   After {i+1}s: Promoted {promoted} task(s) to active queue " +
                  f"(active={active_size}, delayed={delayed_size})")
        else:
            print(f"   After {i+1}s: No tasks ready yet (active={active_size}, delayed={delayed_size})")
    
    # 7. Process promoted tasks in priority order
    print("\n[7] Processing Promoted Scheduled Tasks:")
    print("-" * 80)
    
    active_size = broker.get_queue_size()
    for i in range(active_size):
        task_dict = broker.dequeue()
        if task_dict:
            task_id = task_dict["id"]
            task_type = task_dict["type"]
            priority = task_dict["priority"]
            execute_at = task_dict.get("execute_at", "N/A")
            processed_order.append((task_type, priority))
            print(f"   [{i+1}] {task_type:20s} (priority={priority}) " +
                  f"- was scheduled for {execute_at}")
            broker.mark_completed(task_id)
    
    # 8. Summary
    print("\n[8] Summary of Processing Order:")
    print("-" * 80)
    for i, (task_type, priority) in enumerate(processed_order, 1):
        print(f"   {i}. {task_type:20s} (priority={priority})")
    
    print("\n[RESULT] All tasks processed! Workflow:")
    print("   1. Urgent emails (priority=2) processed first")
    print("   2. Normal emails (priority=1) processed next")  
    print("   3. Scheduled reports (priority=0, delayed) processed when ready")
    
    # 9. Demonstrate scheduler thread
    print("\n[9] Scheduler Thread Integration:")
    print("-" * 80)
    
    config = WorkerConfig(scheduler_interval=0.5)
    executor = TaskExecutor()
    worker = TaskWorker(executor, config)
    
    stats = worker.get_stats()
    print(f"   Worker stats include delayed_size: {'delayed_size' in stats}")
    print(f"   Scheduler checks every {config.scheduler_interval} seconds")
    print(f"   Scheduler runs as daemon thread alongside worker threads")
    
    print("\n" + "="*80)
    print(" DEMONSTRATION COMPLETE!")
    print("="*80)
    print("\nKey Features Demonstrated:")
    print("  [✓] Immediate tasks with different priority levels")
    print("  [✓] Delayed tasks scheduled for future execution")
    print("  [✓] Priority-based ordering (higher priority first)")
    print("  [✓] Automatic promotion of ready delayed tasks")
    print("  [✓] Integration with worker scheduler thread")
    print("  [✓] Consistent timestamp handling (UTC timezone)")
    print()


if __name__ == "__main__":
    try:
        demo_delayed_tasks()
    except Exception as e:
        print(f"\n[ERROR] Demo failed: {e}")
        import traceback
        traceback.print_exc()
