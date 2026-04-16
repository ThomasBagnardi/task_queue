import redis
import json
import logging
from typing import Any, Optional, Dict
from datetime import datetime, timezone
from contextlib import contextmanager
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskBroker:
    """
    Manages task queue operations using Redis as the backend.
    Handles enqueue/dequeue operations with reliable task processing.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        max_connections: int = 50,
    ):
        """
        Initialize the task broker with Redis connection.

        Args:
            host: Redis server host
            port: Redis server port
            db: Redis database number
            password: Redis password (optional)
            max_connections: Maximum number of connections in pool
        """
        self.pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            max_connections=max_connections,
            decode_responses=True,
        )
        self.redis_client = redis.Redis(connection_pool=self.pool)
        self.queue_key = "task_queue"
        self.processing_key = "task_processing"
        self.completed_key = "task_completed"
        self.dead_letter_queue_key = "task_dlq"  # Dead Letter Queue for permanently failed tasks
        self.delayed_queue_key = "task_delayed"  # Delayed tasks sorted by execute_at timestamp

        # Verify connection
        self._verify_connection()

    def _verify_connection(self) -> None:
        """Verify Redis connection is active."""
        try:
            self.redis_client.ping()
            logger.info("Successfully connected to Redis")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    @contextmanager
    def _get_connection(self):
        """Context manager for getting a Redis connection."""
        conn = self.pool.get_connection("default")
        try:
            yield conn
        finally:
            self.pool.release(conn)

    def enqueue(self, task: Dict[str, Any]) -> str:
        """
        Add a task to the priority queue or delayed queue using Redis sorted sets.
        Higher priority tasks are processed first (from active queue).
        Delayed tasks are stored separately, scored by execute_at timestamp.

        Args:
            task: Task dictionary containing task data

        Returns:
            Task ID
        """
        try:
            task_id = task.get("id")
            task_json = json.dumps(task)
            priority = task.get("priority", 0)
            execute_at_str = task.get("execute_at")
            
            current_time = time.time()
            
            # Check if this is a delayed task
            if execute_at_str:
                try:
                    execute_at = datetime.fromisoformat(execute_at_str)
                    execute_at_timestamp = execute_at.timestamp()
                    
                    # If execute_at is in the future, add to delayed queue
                    if execute_at_timestamp > current_time:
                        # Score is the unix timestamp - tasks are checked when score <= current time
                        self.redis_client.zadd(self.delayed_queue_key, {task_json: execute_at_timestamp})
                        
                        # Store task status in Redis hash
                        self.set_task_status(task_id, "pending", task.get("created_at"))
                        
                        logger.info(f"Delayed task enqueued: {task_id} (execute_at: {execute_at_str})")
                        return task_id
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid execute_at value, treating as immediate: {e}")
            
            # Standard immediate enqueue (to active queue)
            # Create a score for the sorted set:
            # - Lower scores are popped first by ZPOPMIN
            # - We use -priority so higher priority values = lower scores = popped first
            # - We add timestamp/10000 for FIFO ordering within same priority
            # Score range: [-1000, 0] allows priorities from -1000 to 1000
            score = (-priority * 1000) + (current_time % 1000)
            
            # Add to priority queue using sorted set (ZADD)
            self.redis_client.zadd(self.queue_key, {task_json: score})
            
            # Store task status in Redis hash
            self.set_task_status(task_id, "pending", task.get("created_at"))
            
            logger.info(f"Task enqueued: {task_id} (priority: {priority}, score: {score:.2f})")

            return task_id
        except Exception as e:
            logger.error(f"Error enqueuing task: {e}")
            raise

    def dequeue(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve and remove the highest priority task from the queue.
        Moves task to processing sorted set for reliability.
        Gets the task with lowest score (highest priority).

        Returns:
            Task dictionary or None if queue is empty
        """
        try:
            # Get the task with minimum score (highest priority) from sorted set
            # ZRANGE returns tasks in order of score
            result = self.redis_client.zrange(self.queue_key, 0, 0, withscores=True)
            
            if not result or len(result) == 0:
                return None
            
            task_json, score = result[0]
            
            # Remove from queue sorted set
            self.redis_client.zrem(self.queue_key, task_json)
            
            # Add to processing sorted set with same score for tracking
            self.redis_client.zadd(self.processing_key, {task_json: score})
            
            task = json.loads(task_json)
            task_id = task.get("id")
            
            # Update task status to processing
            self.set_task_status(task_id, "processing", datetime.now(timezone.utc).isoformat())
            
            logger.info(f"Task dequeued: {task_id} (priority: {-score // 1000})")

            return task
        except Exception as e:
            logger.error(f"Error dequeuing task: {e}")
            raise

    def mark_completed(self, task_id: str) -> bool:
        """
        Mark a task as completed and remove from processing queue.

        Args:
            task_id: ID of the completed task

        Returns:
            True if successful, False otherwise
        """
        try:
            # Store in completed set with timestamp
            completion_data = {
                "task_id": task_id,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
            self.redis_client.sadd(self.completed_key, json.dumps(completion_data))
            
            # Remove from processing sorted set by finding and removing by task_id
            # Since we store task_json as member, we need to search and remove
            processing_tasks = self.redis_client.zrange(self.processing_key, 0, -1)
            for task_json in processing_tasks:
                task_data = json.loads(task_json)
                if task_data.get("id") == task_id:
                    self.redis_client.zrem(self.processing_key, task_json)
                    break
            
            # Update task status to completed
            self.set_task_status(task_id, "completed", datetime.now(timezone.utc).isoformat())

            logger.info(f"Task marked completed: {task_id}")
            return True
        except Exception as e:
            logger.error(f"Error marking task completed: {e}")
            return False

    def mark_failed(self, task_id: str, error: str) -> bool:
        """
        Mark a task as failed and move back to queue.

        Args:
            task_id: ID of the failed task
            error: Error message

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.warning(f"Task failed: {task_id} - {error}")
            
            # Update task status to failed with error message
            self.redis_client.hset(f"task:{task_id}", "status", "failed")
            self.redis_client.hset(f"task:{task_id}", "error", error)
            self.redis_client.hset(f"task:{task_id}", "updated_at", datetime.now(timezone.utc).isoformat())
            
            # Task remains in processing queue for retry
            return True
        except Exception as e:
            logger.error(f"Error marking task failed: {e}")
            return False

    def get_queue_size(self) -> int:
        """Get the current size of the priority task queue (sorted set)."""
        try:
            return self.redis_client.zcard(self.queue_key)
        except Exception as e:
            logger.error(f"Error getting queue size: {e}")
            return 0

    def get_processing_size(self) -> int:
        """Get the number of tasks currently being processed (from sorted set)."""
        try:
            return self.redis_client.zcard(self.processing_key)
        except Exception as e:
            logger.error(f"Error getting processing size: {e}")
            return 0

    def get_completed_count(self) -> int:
        """Get the count of completed tasks."""
        try:
            return self.redis_client.scard(self.completed_key)
        except Exception as e:
            logger.error(f"Error getting completed count: {e}")
            return 0

    def set_task_status(
        self, task_id: str, status: str, timestamp: Optional[str] = None
    ) -> bool:
        """
        Store task status in Redis hash.

        Args:
            task_id: Task ID
            status: Status value (pending, processing, completed, failed, retrying)
            timestamp: Optional timestamp of status update

        Returns:
            True if successful, False otherwise
        """
        try:
            task_key = f"task:{task_id}"
            if timestamp is None:
                timestamp = datetime.now(timezone.utc).isoformat()

            # Store status in Redis hash - use hset with field/value pairs
            self.redis_client.hset(
                task_key,
                "status",
                status
            )
            self.redis_client.hset(
                task_key,
                "updated_at",
                timestamp
            )

            logger.debug(f"Task {task_id} status updated: {status}")
            return True
        except Exception as e:
            logger.error(f"Error setting task status: {e}")
            return False

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve task status and metadata from Redis.

        Args:
            task_id: Task ID

        Returns:
            Dictionary with status info or None if task not found
        """
        try:
            task_key = f"task:{task_id}"
            task_data = self.redis_client.hgetall(task_key)

            if not task_data:
                logger.warning(f"Task {task_id} status not found")
                return None

            logger.debug(f"Task {task_id} status retrieved: {task_data.get('status')}")
            return task_data
        except Exception as e:
            logger.error(f"Error getting task status: {e}")
            return None

    def get_task_status_value(self, task_id: str) -> Optional[str]:
        """
        Get just the status value for a task.

        Args:
            task_id: Task ID

        Returns:
            Status string or None if not found
        """
        try:
            status = self.redis_client.hget(f"task:{task_id}", "status")
            return status
        except Exception as e:
            logger.error(f"Error getting task status value: {e}")
            return None

    def move_to_dlq(self, task_dict: Dict[str, Any], reason: str) -> bool:
        """
        Move a task to the dead-letter queue (permanently failed tasks).

        Args:
            task_dict: Task dictionary
            reason: Reason for moving to DLQ

        Returns:
            True if successful, False otherwise
        """
        try:
            task_id = task_dict.get("id")
            
            # Store DLQ entry with metadata
            dlq_entry = {
                "task_id": task_id,
                "task_type": task_dict.get("type"),
                "reason": reason,
                "moved_at": datetime.now(timezone.utc).isoformat(),
                "retry_count": task_dict.get("retry_count"),
                "max_retries": task_dict.get("max_retries"),
                "error": task_dict.get("error"),
            }
            
            # Add to DLQ
            self.redis_client.lpush(self.dead_letter_queue_key, json.dumps(dlq_entry))
            
            # Update task status to dead-lettered
            self.redis_client.hset(f"task:{task_id}", "status", "dead-lettered")
            self.redis_client.hset(f"task:{task_id}", "reason", reason)
            self.redis_client.hset(f"task:{task_id}", "updated_at", datetime.now(timezone.utc).isoformat())
            
            logger.warning(
                f"Task {task_id} moved to DLQ: {reason} "
                f"(retries: {task_dict.get('retry_count')}/{task_dict.get('max_retries')})"
            )
            return True
        except Exception as e:
            logger.error(f"Error moving task to DLQ: {e}")
            return False

    def get_dlq_size(self) -> int:
        """Get the number of tasks in the dead-letter queue."""
        try:
            return self.redis_client.llen(self.dead_letter_queue_key)
        except Exception as e:
            logger.error(f"Error getting DLQ size: {e}")
            return 0

    def get_dlq_tasks(self, limit: int = 100) -> list:
        """
        Retrieve tasks from the dead-letter queue.

        Args:
            limit: Maximum number of tasks to retrieve

        Returns:
            List of DLQ task dictionaries
        """
        try:
            dlq_entries = self.redis_client.lrange(self.dead_letter_queue_key, 0, limit - 1)
            tasks = [json.loads(entry) for entry in dlq_entries]
            logger.info(f"Retrieved {len(tasks)} tasks from DLQ")
            return tasks
        except Exception as e:
            logger.error(f"Error retrieving DLQ tasks: {e}")
            return []

    def get_dlq_tasks_by_type(self, task_type: str) -> list:
        """
        Get DLQ tasks filtered by type.

        Args:
            task_type: Task type to filter by

        Returns:
            List of matching DLQ tasks
        """
        try:
            all_tasks = self.get_dlq_tasks(limit=1000)
            filtered = [t for t in all_tasks if t.get("task_type") == task_type]
            logger.info(f"Found {len(filtered)} {task_type} tasks in DLQ")
            return filtered
        except Exception as e:
            logger.error(f"Error filtering DLQ tasks: {e}")
            return []

    def clear_dlq(self) -> None:
        """Clear the dead-letter queue (use with caution)."""
        try:
            self.redis_client.delete(self.dead_letter_queue_key)
            logger.info("Dead-letter queue cleared")
        except Exception as e:
            logger.error(f"Error clearing DLQ: {e}")

    def get_delayed_queue_size(self) -> int:
        """Get the number of delayed tasks waiting to be scheduled."""
        try:
            return self.redis_client.zcard(self.delayed_queue_key)
        except Exception as e:
            logger.error(f"Error getting delayed queue size: {e}")
            return 0

    def promote_ready_delayed_tasks(self) -> int:
        """
        Check delayed queue and move tasks with execute_at <= now to active queue.
        Called by scheduler thread to promote ready delayed tasks.

        Returns:
            Number of tasks promoted to active queue
        """
        try:
            current_timestamp = time.time()
            
            # Get all tasks from delayed queue with score <= current time
            # ZRANGEBYSCORE returns all members with score in range
            ready_tasks = self.redis_client.zrangebyscore(
                self.delayed_queue_key, 0, current_timestamp
            )
            
            if not ready_tasks:
                return 0
            
            promoted_count = 0
            for task_json in ready_tasks:
                try:
                    task = json.loads(task_json)
                    task_id = task.get("id")
                    priority = task.get("priority", 0)
                    
                    # Remove from delayed queue
                    self.redis_client.zrem(self.delayed_queue_key, task_json)
                    
                    # Add to active queue with priority-based score
                    current_time = time.time()
                    score = (-priority * 1000) + (current_time % 1000)
                    self.redis_client.zadd(self.queue_key, {task_json: score})
                    
                    promoted_count += 1
                    logger.info(f"Delayed task promoted to active queue: {task_id} (priority: {priority})")
                except Exception as e:
                    logger.error(f"Error promoting delayed task: {e}")
                    continue
            
            if promoted_count > 0:
                logger.info(f"Promoted {promoted_count} delayed tasks to active queue")
            
            return promoted_count
        except Exception as e:
            logger.error(f"Error promoting delayed tasks: {e}")
            return 0
            logger.warning("Dead-letter queue cleared")
        except Exception as e:
            logger.error(f"Error clearing DLQ: {e}")

    def clear_queue(self) -> None:
        """Clear all queues (use with caution)."""
        try:
            self.redis_client.delete(self.queue_key, self.processing_key, self.completed_key)
            logger.warning("All queues cleared")
        except Exception as e:
            logger.error(f"Error clearing queues: {e}")
            raise

    def health_check(self) -> Dict[str, Any]:
        """Get broker health status."""
        try:
            return {
                "status": "healthy",
                "queue_size": self.get_queue_size(),
                "processing_size": self.get_processing_size(),
                "completed_count": self.get_completed_count(),
                "dlq_size": self.get_dlq_size(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"status": "unhealthy", "error": str(e)}

    def close(self) -> None:
        """Close all Redis connections."""
        try:
            self.pool.disconnect()
            logger.info("Redis connection pool closed")
        except Exception as e:
            logger.error(f"Error closing connection pool: {e}")


if __name__ == "__main__":
    # Example usage
    broker = TaskBroker()

    try:
        # Example: enqueue a task
        sample_task = {"id": "task-001", "type": "compute", "data": {"x": 10, "y": 20}}
        task_id = broker.enqueue(sample_task)
        print(f"Enqueued task: {task_id}")

        # Check status
        status = broker.health_check()
        print(f"Broker status: {status}")

        # Example: dequeue a task
        task = broker.dequeue()
        if task:
            print(f"Dequeued task: {task}")

    finally:
        broker.close()
