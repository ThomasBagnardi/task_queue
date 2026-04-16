# Quick Start - Delayed Tasks

## Submit a Delayed Task

```python
from datetime import datetime, timezone, timedelta
from task import Task
from broker import TaskBroker

# Create a task scheduled for 1 hour from now
execute_at = datetime.now(timezone.utc) + timedelta(hours=1)

task = Task(
    task_type="send_reminder",
    data={"user_id": 123, "message": "Time to review your tasks"},
    priority=1,
    execute_at=execute_at
)

# Submit to broker
broker = TaskBroker()
broker.enqueue(task.to_dict())

print(f"Task {task.id[:12]}... scheduled for {execute_at}")
```

## Start Worker with Scheduler

```python
from worker import TaskWorker, TaskExecutor, WorkerConfig

# Create worker with 2-second scheduler interval
config = WorkerConfig(scheduler_interval=2.0)
executor = TaskExecutor()
worker = TaskWorker(executor, config)

# Run workers (scheduler thread starts automatically)
worker.run_parallel(num_workers=4)
```

## Monitor Scheduled Tasks

```python
from broker import TaskBroker

broker = TaskBroker()

# Check how many tasks are waiting
delayed = broker.get_delayed_queue_size()
active = broker.get_queue_size()

print(f"Delayed tasks: {delayed}")
print(f"Active tasks: {active}")

# Get worker stats
stats = worker.get_stats()
print(f"Delayed queue: {stats['delayed_size']}")
worker.print_stats()
```

## Manually Promote Ready Tasks

```python
from broker import TaskBroker

broker = TaskBroker()

# Promote all ready tasks to active queue
promoted = broker.promote_ready_delayed_tasks()
print(f"Promoted {promoted} tasks")

# This is called automatically by scheduler thread every N seconds
```

## Test Your Implementation

```bash
# Run comprehensive delayed task tests
python test_delayed_realistic.py

# See integrated priority + delayed demo
python demo_delayed_priority.py

# Check timestamp consistency
python debug_timestamps.py
```

## Key Concepts

### 1. **Scheduling a Task**
```python
# Task IS scheduled (in delayed queue) when execute_at > now
task = Task(..., execute_at=future_datetime)

# Task is IMMEDIATE (in active queue) when execute_at is None or <= now
task = Task(...)  # No execute_at
```

### 2. **Scheduler Thread**
- Runs automatically when you call `worker.run_parallel()` or `worker.run()`
- Wakes up every `scheduler_interval` seconds
- Checks for ready tasks and promotes them
- Runs as daemon thread alongside worker threads

### 3. **Priority Integration**
- Scheduled tasks with `priority=2` are processed before `priority=0` tasks
- When promoted, they join the active queue respecting priority order
- Immediate high-priority tasks still process first

### 4. **Timezone Details**
```python
# CORRECT - Timezone-aware UTC
from datetime import datetime, timezone, timedelta
execute_at = datetime.now(timezone.utc) + timedelta(hours=1)

# WRONG - Naive datetime (may cause timezone issues)
# execute_at = datetime.utcnow() + timedelta(hours=1)
```

## Configuration

### Worker Config Options
```python
from worker import WorkerConfig

config = WorkerConfig(
    scheduler_interval=5.0,    # Check every 5 seconds (default)
    worker_name="worker-1",    # Custom worker name
    # ... other options
)
```

## Troubleshooting

### Tasks not being promoted?
1. Check current time: `datetime.now(timezone.utc)`
2. Verify execute_at is in past: `task.execute_at < datetime.now(timezone.utc)`
3. Check scheduler is running: `worker.scheduler_thread.is_alive()`
4. Reduce scheduler_interval for faster checks

### Timestamp issues?
1. Always use `datetime.now(timezone.utc)` 
2. Never use `datetime.utcnow()`
3. Verify consistency: `python debug_timestamps.py`

### Need more details?
- See [DELAYED_TASKS_README.md](DELAYED_TASKS_README.md) for comprehensive guide
- Check [demo_delayed_priority.py](demo_delayed_priority.py) for full example
- Read [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) for technical details

## Common Patterns

### Schedule one-time task
```python
from datetime import datetime, timezone, timedelta

task = Task(
    task_type="send_report",
    data={"report_id": 1},
    execute_at=datetime.now(timezone.utc) + timedelta(hours=2)
)
```

### Schedule recurring task (submit multiple)
```python
for i in range(5):
    task = Task(
        task_type="check_status",
        data={"check_id": i},
        execute_at=datetime.now(timezone.utc) + timedelta(hours=1+i)
    )
    broker.enqueue(task.to_dict())
```

### Different priorities
```python
# Urgent task - process immediately
urgent = Task(..., priority=2)

# Normal task - process when urgent are done  
normal = Task(..., priority=1)

# Low priority scheduled task - process last
scheduled = Task(..., priority=0, execute_at=future)
```

## Performance Tips

1. **Adjust scheduler_interval**: Faster checks = quicker promotion but more CPU
2. **Use appropriate priorities**: Let scheduler focus on important tasks
3. **Monitor delayed_size**: Don't let too many tasks accumulate
4. **Consider batch promotions**: For many scheduled tasks, increase interval

---

For complete documentation, see [DELAYED_TASKS_README.md](DELAYED_TASKS_README.md)
