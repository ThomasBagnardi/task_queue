import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from broker import TaskBroker
from task import Task, TaskStatus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ProducerConfig:
    """Configuration for task producer."""
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    max_connections: int = 50


class TaskProducer:
    """
    Produces tasks and submits them to the task queue.
    Provides a high-level API for task creation and submission.
    """

    def __init__(self, config: Optional[ProducerConfig] = None):
        """
        Initialize the task producer.

        Args:
            config: ProducerConfig instance with broker settings
        """
        if config is None:
            config = ProducerConfig()

        self.config = config
        self.broker = TaskBroker(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            password=config.redis_password,
            max_connections=config.max_connections,
        )
        self.tasks_submitted = 0
        logger.info("TaskProducer initialized")

    def submit_task(
        self,
        task_type: str,
        data: Dict[str, Any],
        priority: int = 0,
        max_retries: int = 3,
        timeout: Optional[int] = None,
        task_id: Optional[str] = None,
    ) -> str:
        """
        Create and submit a single task to the queue.

        Args:
            task_type: Type/name of the task
            data: Task payload/parameters
            priority: Task priority (default: 0)
            max_retries: Max retry attempts (default: 3)
            timeout: Execution timeout in seconds (default: None)
            task_id: Optional custom task ID

        Returns:
            Task ID of the submitted task

        Raises:
            ValueError: If task validation fails
            Exception: If broker enqueue fails
        """
        try:
            # Create task
            task = Task(
                task_type=task_type,
                data=data,
                task_id=task_id,
                priority=priority,
                max_retries=max_retries,
                timeout=timeout,
            )

            # Validate task
            if not task.validate():
                raise ValueError(f"Task validation failed: {task}")

            # Enqueue task
            task_dict = task.to_dict()
            task_id = self.broker.enqueue(task_dict)
            self.tasks_submitted += 1

            logger.info(
                f"Task submitted: {task_id} (type: {task_type}, priority: {priority})"
            )
            return task_id

        except Exception as e:
            logger.error(f"Error submitting task: {e}")
            raise

    def submit_tasks_batch(
        self,
        tasks: List[Dict[str, Any]],
    ) -> List[str]:
        """
        Submit multiple tasks to the queue in batch.

        Args:
            tasks: List of task dictionaries with keys:
                - task_type (required): Task type
                - data (required): Task data
                - priority (optional): Priority level
                - max_retries (optional): Max retries
                - timeout (optional): Timeout seconds
                - task_id (optional): Custom task ID

        Returns:
            List of submitted task IDs

        Raises:
            Exception: If any task submission fails
        """
        try:
            submitted_ids = []

            for task_data in tasks:
                task_id = self.submit_task(
                    task_type=task_data["task_type"],
                    data=task_data["data"],
                    priority=task_data.get("priority", 0),
                    max_retries=task_data.get("max_retries", 3),
                    timeout=task_data.get("timeout"),
                    task_id=task_data.get("task_id"),
                )
                submitted_ids.append(task_id)

            logger.info(f"Batch submitted: {len(submitted_ids)} tasks")
            return submitted_ids

        except Exception as e:
            logger.error(f"Error submitting batch: {e}")
            raise

    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get current queue status and statistics.

        Returns:
            Dictionary with queue metrics
        """
        try:
            status = self.broker.health_check()
            status["total_submitted"] = self.tasks_submitted

            logger.debug(f"Queue status: {status}")
            return status

        except Exception as e:
            logger.error(f"Error getting queue status: {e}")
            raise

    def get_queue_size(self) -> int:
        """Get number of pending tasks in queue."""
        try:
            size = self.broker.get_queue_size()
            logger.debug(f"Queue size: {size}")
            return size

        except Exception as e:
            logger.error(f"Error getting queue size: {e}")
            raise

    def get_processing_count(self) -> int:
        """Get number of tasks currently being processed."""
        try:
            count = self.broker.get_processing_size()
            logger.debug(f"Processing count: {count}")
            return count

        except Exception as e:
            logger.error(f"Error getting processing count: {e}")
            raise

    def get_completed_count(self) -> int:
        """Get number of completed tasks."""
        try:
            count = self.broker.get_completed_count()
            logger.debug(f"Completed count: {count}")
            return count

        except Exception as e:
            logger.error(f"Error getting completed count: {e}")
            raise

    def wait_for_queue(self, max_wait_seconds: int = 300) -> bool:
        """
        Wait for queue to be processed (blocking call).

        Args:
            max_wait_seconds: Maximum seconds to wait (default: 300)

        Returns:
            True if queue is empty, False if timeout reached
        """
        import time

        try:
            start_time = time.time()
            poll_interval = 1  # seconds

            while True:
                elapsed = time.time() - start_time
                if elapsed > max_wait_seconds:
                    logger.warning(
                        f"Timeout waiting for queue after {max_wait_seconds}s"
                    )
                    return False

                queue_size = self.get_queue_size()
                processing_count = self.get_processing_count()

                if queue_size == 0 and processing_count == 0:
                    logger.info("Queue is now empty")
                    return True

                logger.info(
                    f"Waiting for queue... (pending: {queue_size}, "
                    f"processing: {processing_count})"
                )
                time.sleep(poll_interval)

        except Exception as e:
            logger.error(f"Error waiting for queue: {e}")
            raise

    def clear_queue(self) -> None:
        """Clear all queues (use with caution)."""
        try:
            self.broker.clear_queue()
            self.tasks_submitted = 0
            logger.warning("Queue cleared")

        except Exception as e:
            logger.error(f"Error clearing queue: {e}")
            raise

    def close(self) -> None:
        """Close broker connection."""
        try:
            self.broker.close()
            logger.info("Producer closed")

        except Exception as e:
            logger.error(f"Error closing producer: {e}")


class TaskProducerContext:
    """Context manager for TaskProducer for automatic resource cleanup."""

    def __init__(self, config: Optional[ProducerConfig] = None):
        """Initialize context manager."""
        self.config = config
        self.producer = None

    def __enter__(self) -> TaskProducer:
        """Enter context and create producer."""
        self.producer = TaskProducer(self.config)
        return self.producer

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context and close producer."""
        if self.producer:
            self.producer.close()


if __name__ == "__main__":
    import time

    # Example usage with context manager
    config = ProducerConfig(redis_host="localhost", redis_port=6379)

    with TaskProducerContext(config) as producer:
        # Submit a single task
        task_id = producer.submit_task(
            task_type="send_email",
            data={
                "recipient": "user@example.com",
                "subject": "Hello",
                "body": "This is a test email",
            },
            priority=1,
            max_retries=3,
            timeout=30,
        )
        print(f"Submitted task: {task_id}")

        # Submit a batch of tasks
        batch_tasks = [
            {
                "task_type": "process_data",
                "data": {"file": f"data_{i}.csv", "operation": "aggregate"},
                "priority": 0,
                "max_retries": 2,
                "timeout": 60,
            }
            for i in range(5)
        ]
        batch_ids = producer.submit_tasks_batch(batch_tasks)
        print(f"Submitted batch: {batch_ids}")

        # Get queue status
        status = producer.get_queue_status()
        print(f"Queue status: {status}")

        # Display statistics
        print(f"\nProducer Statistics:")
        print(f"  Total submitted: {producer.tasks_submitted}")
        print(f"  Pending tasks: {producer.get_queue_size()}")
        print(f"  Processing: {producer.get_processing_count()}")
        print(f"  Completed: {producer.get_completed_count()}")

        # Optional: wait for some processing (with timeout)
        # producer.wait_for_queue(max_wait_seconds=10)
