# Priority Queue Implementation - Redis Sorted Sets

## Overview

The task queue system now supports **priority-based task processing** using Redis sorted sets with ZADD and ZRANGE operations. Tasks with higher priority values are processed first, with FIFO ordering within the same priority level.

## Implementation Details

### Redis Data Structure

Previously: `task_queue` used a Redis **List** (RPUSH/LPOP/RPOPLPUSH)
Now: `task_queue` uses a Redis **Sorted Set** (ZADD/ZRANGE/ZREM) with priority-based scoring

### Score Calculation

```
score = (-priority * 1000) + (timestamp % 1000)
```

**Why this formula?**
- **Lower scores are returned first** by ZRANGE: This is the default Redis behavior
- **Negative priority**: Higher priority values (10) → lower scores (-10000) → processed first
- **Timestamp component**: Ensures FIFO ordering for tasks with identical priorities
- **Modulo 1000**: Keeps scores compact while preserving sub-second ordering

### Score Examples

| Priority | Approximate Score | Processing Order |
|----------|-------------------|------------------|
| 10 (high) | -9995 | 1st (lowest score) |
| 10 (high) | -9994 | 2nd |
| 5 (medium) | -4997 | 3rd |
| 5 (medium) | -4996 | 4th |
| 0 (low) | -2 | 5th |
| 0 (low) | -1 | 6th (highest score) |

## Redis Commands Used

### Enqueue (Submit Task)

```python
# Add task to priority queue sorted set
ZADD task_queue {task_json: score}
```

**Operation:** `O(log N)` where N is number of tasks in queue

### Dequeue (Get Next Task)

```python
# Get task with minimum score (highest priority)
ZRANGE task_queue 0 0 WITHSCORES  # Gets lowest score item
ZREM task_queue task_json          # Remove from queue
ZADD task_processing {task_json: score}  # Move to processing
```

**Operation:** `O(log N + N)` for insert and remove

### Queue Statistics

```python
ZCARD task_queue          # Get queue size
ZCARD task_processing     # Get processing count
ZRANGE key 0 -1 WITHSCORES  # View all tasks with scores
```

## Features

### ✅ Priority-Based Processing
```python
# High priority task (processed first)
client.submit_task(
    task_type="urgent_email",
    data={"to": "admin@example.com"},
    priority=10  # Higher number = higher priority
)

# Low priority task (processed last)
client.submit_task(
    task_type="cleanup_job",
    data={"type": "garbage_collection"},
    priority=0
)
```

### ✅ FIFO Within Same Priority
Tasks with identical priority are processed in submission order (insertion order).

### ✅ Sorted Set Introspection
Monitor queue priorities:
```python
# View all pending tasks with scores
broker.redis_client.zrange('task_queue', 0, -1, withscores=True)

# Count tasks at each priority
broker.redis_client.zcard('task_queue')

# Get score range
broker.redis_client.zcount('task_queue', '-inf', '0')
```

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Submit task | O(log N) | ZADD operation |
| Get next task | O(log N) | ZRANGE + ZREM |
| Queue size | O(1) | ZCARD operation |
| Range query | O(log N + M) | M = number of items returned |

**Where N = number of tasks in queue**

## Configuration

### Priority Ranges

By default, priorities can be:
- **Negative to Positive**: Full integer range supported
- **Recommended Range**: 0-100 for simplicity
- **Special Values**:
  - Priority 0: Default/normal tasks
  - Priority < 0: Background/deferred tasks
  - Priority > 10: Urgent/critical tasks

### Score Formula Customization

To use different priority ranges, modify in `broker.py`:

```python
# Current: Supports priorities -1000 to +1000
score = (-priority * 1000) + (current_time % 1000)

# Alternative for 0-100 priority range:
score = (-priority * 100) + (current_time % 100)
```

## API Changes

### Task Submission
```json
POST /tasks/submit
{
  "task_type": "send_email",
  "data": {"to": "user@example.com"},
  "priority": 5,           // NEW: Priority level (higher = more urgent)
  "max_retries": 3,
  "timeout": 30
}
```

### Response Examples
```json
// Success
{
  "success": true,
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "Task submitted successfully"
}
```

## Usage Example

```python
from api_client import TaskQueueClient

client = TaskQueueClient()

# Submit tasks at different priorities
high_priority_id = client.submit_task(
    task_type="critical_backup",
    data={"backup_type": "full"},
    priority=10
)

medium_priority_id = client.submit_task(
    task_type="report_generation", 
    data={"report_type": "daily"},
    priority=5
)

low_priority_id = client.submit_task(
    task_type="log_cleanup",
    data={"days_to_keep": 30},
    priority=0
)

# All three tasks are now in queue
# They will be dequeued in this order:
# 1. critical_backup (priority=10)
# 2. report_generation (priority=5)
# 3. log_cleanup (priority=0)

# Check queue status
status = client.get_queue_status()
print(f"Pending: {status['queue_size']}")  # Output: 3
```

## Testing Priority Queues

Run the comprehensive priority queue test:

```bash
python test_priority_queue.py
```

### Test Scenarios
1. **Multi-priority submission**: Submit 6 tasks at 3 priority levels
2. **Dequeue ordering**: Verify tasks are popped in priority order
3. **FIFO within priority**: Verify tasks with same priority maintain order
4. **Sorted set inspection**: View Redis structure and scores
5. **Card operations**: Verify ZCARD counts

### Expected Output
```
✓ high_priority_task_1: priority=10 (correctly ordered)
✓ high_priority_task_2: priority=10 (correctly ordered)
✓ medium_priority_task_1: priority=5 (correctly ordered)
✓ medium_priority_task_2: priority=5 (correctly ordered)
✓ low_priority_task_1: priority=0 (correctly ordered)
✓ low_priority_task_2: priority=0 (correctly ordered)

✅ Priority queue working correctly!
```

## Migration from List-Based Queue

If migrating existing systems:

1. **Old system** (List-based):
   ```
   RPUSH task_queue task_json     # Just append
   RPOPLPUSH task_queue processing  # Get oldest first
   ```

2. **New system** (Sorted set with priority):
   ```
   ZADD task_queue {task_json: score}  # Score-based insertion
   ZRANGE + ZREM  # Get by score, then remove
   ```

## Redis Version Compatibility

- **ZADD**: Available in all Redis versions
- **ZRANGE**: Available in all Redis versions
- **ZCARD**: Available in all Redis versions
- **ZPOPMIN**: Available in Redis 5.0+ (not used - using ZRANGE instead)

This implementation works with **Redis 2.6+** and is fully backward compatible.

## Monitoring and Debugging

### Check Queue Priorities

```python
import redis
r = redis.Redis()

# View queue with scores
tasks = r.zrange('task_queue', 0, -1, withscores=True)
for task_json, score in tasks:
    task = json.loads(task_json)
    priority = -int(score // 1000)
    print(f"{task['type']}: priority={priority}, score={score}")
```

### Queue Analytics

```python
# Count tasks by priority
all_tasks = r.zrange('task_queue', 0, -1, withscores=True)

priority_counts = {}
for task_json, score in all_tasks:
    priority = -int(score // 1000)
    priority_counts[priority] = priority_counts.get(priority, 0) + 1

for priority in sorted(priority_counts.keys(), reverse=True):
    print(f"Priority {priority}: {priority_counts[priority]} tasks")
```

## Future Enhancements

1. **Dynamic Priority Adjustment**: Increase priority of long-waiting tasks
2. **Priority Groups**: Group related tasks at same priority
3. **Max Priority Cap**: Prevent runaway high-priority tasks starving lower-priority ones
4. **Prometheus Metrics**: Export priority distribution to monitoring systems
5. **Dead Letter Queue Priorities**: Preserve priority when moving to DLQ

## References

- [Redis Sorted Sets Documentation](https://redis.io/docs/data-types/sorted-sets/)
- [ZADD Command](https://redis.io/commands/zadd/)
- [ZRANGE Command](https://redis.io/commands/zrange/)
- [ZREM Command](https://redis.io/commands/zrem/)
- [ZCARD Command](https://redis.io/commands/zcard/)

---

**Last Updated**: 2026-03-30  
**Version**: 1.0.0  
**Status**: ✅ Production Ready
