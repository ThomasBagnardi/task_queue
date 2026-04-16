# Delayed Tasks Implementation

## Overview

The task queue now supports **delayed task execution** using Redis sorted sets with timestamp-based scoring. Tasks can be scheduled to execute at a specific future time, and a background scheduler thread automatically promotes ready tasks to the active queue.

## Features

### 1. Delayed Task Storage
- Delayed tasks are stored in a separate Redis sorted set (`task_delayed`)
- Score = Unix timestamp of `execute_at` time
- Enables efficient range queries - ready tasks are identified via `ZRANGEBYSCORE`

### 2. Automatic Promotion
- Background scheduler thread periodically checks for ready tasks
- Ready tasks (where `execute_at <= current_time`) are promoted to the active queue
- Configurable check interval (default: 5 seconds)

### 3. Priority Integration
- When delayed tasks are promoted, they retain their priority level
- Promoted tasks are scored using the same priority formula as active queue tasks
- Higher priority tasks are still processed first

## API Usage

### Creating a Delayed Task

```python
from datetime import datetime, timedelta, timezone
from producer import TaskProducer

producer = TaskProducer()

# Schedule a task to execute 1 hour from now
execute_at = datetime.now(timezone.utc) + timedelta(hours=1)

# Submit delayed task
# Note: Currently, execute_at must be set directly on the Task object
# Future enhancement: add execute_at parameter to submit_task()

from task import Task

task = Task(
    task_type="scheduled_report",
    data={"report_type": "daily", "user_id": 123},
    priority=1,
    execute_at=execute_at
)

# The task will be stored in the delayed queue until execute_at time
```

### Checking Scheduled Tasks

```python
from broker import TaskBroker

broker = TaskBroker()

# Get number of delayed tasks waiting
delayed_count = broker.get_delayed_queue_size()

# Get worker stats including delayed tasks
# (See worker.get_stats() which includes delayed_size)
```

## Implementation Details

### Task Model (`task.py`)

- Added `execute_at: Optional[datetime]` field to `Task` class
- Datetime handling uses `datetime.now(timezone.utc)` for consistent timezone semantics
- Task serialization includes `execute_at` as ISO format string

```python
task = Task(
    task_type="email_send",
    data={"email": "user@example.com"},
    priority=0,
    execute_at=datetime.now(timezone.utc) + timedelta(hours=1)
)

# Serialized to JSON with ISO format timestamp
task_dict = task.to_dict()
print(task_dict["execute_at"])  # "2026-04-01T15:30:00+00:00"
```

### Broker Implementation (`broker.py`)

#### New Queue Keys
- `task_delayed`: Sorted set for scheduled tasks (score = Unix timestamp)

#### New Methods

**`enqueue(task)`**
- Routes tasks to appropriate queue:
  - If `execute_at` is in future → added to `task_delayed` sorted set
  - If `execute_at` is None or past → added to active `task_queue` sorted set
- Uses Redis ZADD for both queues

**`get_delayed_queue_size()`**
- Returns count of tasks in delayed queue
- Uses Redis ZCARD

**`promote_ready_delayed_tasks()`**
- Called by scheduler thread
- Checks for tasks where `score <= current_timestamp`
- Uses `ZRANGEBYSCORE(task_delayed, 0, current_time)`
- Promotes ready tasks to active queue with priority-based scoring
- Returns count of promoted tasks

```python
# Example from test
promoted = broker.promote_ready_delayed_tasks()
print(f"Promoted {promoted} tasks to active queue")
```

### Worker Implementation (`worker.py`)

#### Configuration

**`WorkerConfig`**
- Added `scheduler_interval: float` (default: 5.0 seconds)
- Controls how often scheduler checks for ready tasks

```python
config = WorkerConfig(scheduler_interval=1.0)  # Check every 1 second
```

#### Scheduler Thread Management

**New Methods**

`start_scheduler()`
- Creates and starts daemon thread running `_run_scheduler()`
- Called automatically by `run()` and `run_parallel()`

`stop_scheduler()`
- Sets `scheduler_running` flag to False
- Joins thread with 5-second timeout
- Called by `stop()` method

`_run_scheduler()`
- Infinite loop that:
  1. Calls `broker.promote_ready_delayed_tasks()`
  2. Sleeps for `scheduler_interval` seconds
  3. Continues until `scheduler_running` is False

#### Task Lifecycle Integration

**`run()` and `run_parallel()` Methods**
- Automatically call `start_scheduler()` at startup
- Scheduler runs alongside worker threads

**`stop()` Method**
- Calls `stop_scheduler()` for clean shutdown

#### Statistics

**`get_stats()`**
- Now includes `delayed_size` - count of tasks in delayed queue
- Example: `stats['delayed_size']` returns 5 (5 tasks waiting to execute)

**`print_stats()`**
- Displays delayed queue size in summary output

```
Worker Stats:
  Queue size: 3 active tasks
  Delayed tasks: 5 tasks waiting
  Processing: 2 tasks
  ...
```

## Performance Characteristics

### Time Complexity
- **Enqueue**: O(1) - Redis ZADD
- **Promotion Check**: O(m log n) where m = ready tasks, n = total delayed tasks
  - ZRANGEBYSCORE efficiently finds ready tasks
  - ZADD moves tasks to active queue one by one
- **Dequeue**: O(1) - Redis ZRANGE with limit

### Scalability
- No query of entire queue needed - only tasks ready for promotion
- Scheduler interval controls check frequency vs. CPU usage tradeoff
- Tested with thousands of scheduled tasks

## Timezone Handling

**Critical:** All time operations use `datetime.now(timezone.utc)` and `time.time()` for consistency:

```python
# CORRECT - use timezone-aware UTC
from datetime import datetime, timezone, timedelta
execute_at = datetime.now(timezone.utc) + timedelta(hours=1)

# INCORRECT - avoid naive datetime.utcnow() on Windows
# execute_at = datetime.utcnow() + timedelta(hours=1)
```

Windows has known timezone handling issues with `datetime.utcnow()`. Using `datetime.now(timezone.utc)` ensures timestamp values from `datetime.timestamp()` and `time.time()` are consistent.

## Example Workflow

```python
from datetime import datetime, timezone, timedelta
from task import Task
from broker import TaskBroker
from worker import TaskWorker, TaskExecutor, WorkerConfig

# 1. Create a delayed task
future_time = datetime.now(timezone.utc) + timedelta(seconds=30)
task = Task(
    task_type="backup",
    data={"backup_type": "incremental"},
    priority=1,
    execute_at=future_time
)

# 2. Enqueue (goes to delayed queue)
broker = TaskBroker()
broker.enqueue(task.to_dict())
print(f"Task scheduled for {future_time.isoformat()}")

# 3. Start worker with scheduler
config = WorkerConfig(scheduler_interval=2.0)
executor = TaskExecutor()
worker = TaskWorker(executor, config)

# Scheduler thread starts automatically
worker.run_parallel(num_workers=2)

# After 30+ seconds, scheduler promotes task to active queue
# Worker picks up and processes it

# 4. Shutdown
worker.stop()  # Scheduler stops automatically
```

## Testing

### Test Files

**`test_delayed_realistic.py`**
- Validates delayed task promotion with realistic near-future timestamps
- Tests scheduler thread integration
- Verifies promotion timing accuracy

**`debug_timestamps.py`**  
- Diagnoses timestamp handling
- Verifies timezone consistency between datetime and time.time()

### Running Tests

```bash
# Test delayed task promotion timing
python test_delayed_realistic.py

# Check timezone consistency
python debug_timestamps.py

# Verify syntax
python -m py_compile task.py broker.py worker.py
```

## Future Enhancements

1. **API Endpoint** - Add `execute_at` parameter to HTTP API
   ```
   POST /tasks/submit
   {
     "task_type": "report",
     "data": {...},
     "execute_at": "2026-04-01T15:30:00+00:00"
   }
   ```

2. **Task Persistence** - Save scheduled tasks to backup storage for recovery

3. **Execution Guarantee** - Ensure all ready tasks are processed even if scheduler misses interval

4. **Metrics** - Track promotion delays and scheduling statistics

5. **Async Scheduler** - Replace poll-based approach with event-based promotion

## Integration Checklist

- [x] Task model supports `execute_at` datetime
- [x] Serialization preserves timezones (ISO format with +00:00)
- [x] Broker routes tasks by execute_at value
- [x] ZRANGEBYSCORE queries ready tasks efficiently
- [x] Scheduler thread polls and promotes at configured intervals
- [x] Worker starts/stops scheduler automatically
- [x] Statistics include delayed queue size
- [x] Timezone consistency fixed (datetime.now(timezone.utc))
- [x] Tests validate end-to-end functionality
- [x] All files syntax-validated

## Troubleshooting

### Tasks Not Being Promoted
1. Check scheduler thread is running: `worker.scheduler_thread.is_alive()`
2. Verify `execute_at` time is in past: `execute_at <= datetime.now(timezone.utc)`
3. Check scheduler interval isn't too long
4. Review Redis connection is active

### Timezone Issues
- Always use `datetime.now(timezone.utc)` instead of `datetime.utcnow()`
- Verify `execute_at.timestamp()` matches `time.time()` scale
- Use `test_delayed_realistic.py::test_delayed_promotion_realistic` to diagnose

### Performance Issues
- Reduce `scheduler_interval` if tasks need faster promotion
- Monitor Redis ZRANGEBYSCORE performance with large queues
- Consider promoting in batches vs. one at a time
