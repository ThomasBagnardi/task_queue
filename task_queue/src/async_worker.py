"""
Async task worker using asyncio and aioredis for concurrent task processing.
Allows a single worker to handle many tasks simultaneously without threads.
"""

import asyncio
import json
import logging
from typing import Any, Callable, Dict, Optional, Awaitable
from dataclasses import dataclass
from datetime import datetime
from task import Task, TaskStatus

try:
    import redis.asyncio as aioredis
except ImportError:
    raise ImportError(
        "redis[asyncio] is required for async worker. Install with: pip install redis"
    )

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class AsyncWorkerConfig:
    """Configuration for async task worker."""

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    poll_interval: float = 0.5  # seconds between queue checks
    max_concurrent_tasks: int = 10  # max tasks to process concurrently
    exponential_backoff_base: float = 1.0  # base delay for exponential backoff
    exponential_backoff_max: float = 300.0  # max delay for exponential backoff


class AsyncTaskExecutor:
    """
    Registry and executor for async task handlers.
    Maps task types to their async handler functions.
    """

    def __init__(self):
        """Initialize the async task executor."""
        self.handlers: Dict[str, Callable] = {}
        logger.info("AsyncTaskExecutor initialized")

    def register(self, task_type: str, handler: Callable) -> None:
        """
        Register an async handler for a specific task type.

        Args:
            task_type: Type of task this handler processes
            handler: Async callable that processes the task

        Raises:
            ValueError: If task_type is already registered
        """
        if task_type in self.handlers:
            raise ValueError(f"Handler already registered for task type: {task_type}")

        self.handlers[task_type] = handler
        logger.info(f"Async handler registered for task type: {task_type}")

    def unregister(self, task_type: str) -> None:
        """Unregister a handler for a task type."""
        if task_type in self.handlers:
            del self.handlers[task_type]
            logger.info(f"Async handler unregistered for task type: {task_type}")

    def has_handler(self, task_type: str) -> bool:
        """Check if handler exists for task type."""
        return task_type in self.handlers

    async def execute(self, task: Task) -> Any:
        """
        Execute a task using its registered async handler.

        Args:
            task: Task instance to execute

        Returns:
            Task execution result

        Raises:
            ValueError: If no handler registered for task type
            Exception: If handler execution fails
        """
        if not self.has_handler(task.type):
            raise ValueError(f"No handler registered for task type: {task.type}")

        handler = self.handlers[task.type]
        logger.info(f"Executing async handler for task {task.id} (type: {task.type})")

        try:
            result = await handler(task.data)
            logger.info(f"Task {task.id} execution succeeded")
            return result
        except Exception as e:
            logger.error(f"Task {task.id} handler execution failed: {e}")
            raise


class AsyncTaskBroker:
    """
    Async Redis broker for task queue operations.
    """

    def __init__(self, config: AsyncWorkerConfig):
        """Initialize async broker with Redis connection."""
        self.config = config
        self.redis = None
        self.queue_key = "task_queue"
        self.processing_key = "task_processing"
        self.completed_key = "task_completed"
        self.dead_letter_queue_key = "task_dlq"

    async def connect(self) -> None:
        """Establish Redis connection."""
        try:
            self.redis = await aioredis.from_url(
                f"redis://{self.config.redis_host}:{self.config.redis_port}/{self.config.redis_db}",
                password=self.config.redis_password,
                encoding="utf-8",
                decode_responses=True,
            )
            logger.info("Connected to Redis via async redis library")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            logger.info("Redis connection closed")

    async def dequeue(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve and remove next task from queue.
        Moves task to processing queue atomically.
        """
        try:
            # Use blpop for blocking operation with timeout
            result = await asyncio.wait_for(
                self.redis.blpop(self.queue_key, timeout=1),
                timeout=self.config.poll_interval + 1
            )

            if result is None:
                return None

            # result is (key, value) tuple
            key, task_json = result
            task = json.loads(task_json)
            task_id = task.get("id")

            # Move to processing queue
            await self.redis.lpush(self.processing_key, task_json)

            # Update task status
            await self.set_task_status(task_id, "processing")

            logger.info(f"Task dequeued: {task_id}")
            return task

        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"Error dequeuing task: {e}")
            raise

    async def mark_completed(self, task_id: str) -> bool:
        """Mark task as completed."""
        try:
            completion_data = {
                "task_id": task_id,
                "completed_at": datetime.utcnow().isoformat(),
            }
            await self.redis.sadd(self.completed_key, json.dumps(completion_data))
            await self.set_task_status(task_id, "completed")

            logger.info(f"Task marked completed: {task_id}")
            return True
        except Exception as e:
            logger.error(f"Error marking task completed: {e}")
            return False

    async def set_task_status(self, task_id: str, status: str) -> bool:
        """Store task status in Redis hash."""
        try:
            task_key = f"task:{task_id}"
            await self.redis.hset(
                task_key,
                "status",
                status,
                "updated_at",
                datetime.utcnow().isoformat(),
            )
            logger.debug(f"Task {task_id} status updated: {status}")
            return True
        except Exception as e:
            logger.error(f"Error setting task status: {e}")
            return False

    async def move_to_dlq(self, task_dict: Dict[str, Any], reason: str) -> bool:
        """Move task to dead-letter queue."""
        try:
            task_id = task_dict.get("id")
            dlq_entry = {
                "task_id": task_id,
                "task_type": task_dict.get("type"),
                "reason": reason,
                "moved_at": datetime.utcnow().isoformat(),
                "retry_count": task_dict.get("retry_count"),
                "max_retries": task_dict.get("max_retries"),
                "error": task_dict.get("error"),
            }

            await self.redis.lpush(self.dead_letter_queue_key, json.dumps(dlq_entry))

            # Update status
            task_key = f"task:{task_id}"
            await self.redis.hset(
                task_key,
                "status",
                "dead-lettered",
                "reason",
                reason,
                "updated_at",
                datetime.utcnow().isoformat(),
            )

            logger.warning(f"Task {task_id} moved to DLQ: {reason}")
            return True
        except Exception as e:
            logger.error(f"Error moving task to DLQ: {e}")
            return False

    async def get_queue_size(self) -> int:
        """Get queue size."""
        try:
            return await self.redis.llen(self.queue_key)
        except Exception as e:
            logger.error(f"Error getting queue size: {e}")
            return 0

    async def health_check(self) -> Dict[str, Any]:
        """Get broker health status."""
        try:
            return {
                "status": "healthy",
                "queue_size": await self.get_queue_size(),
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"status": "unhealthy", "error": str(e)}


class AsyncTaskWorker:
    """
    Async task worker for concurrent task processing.
    Handles multiple tasks simultaneously without threads.
    """

    def __init__(
        self,
        executor: AsyncTaskExecutor,
        config: Optional[AsyncWorkerConfig] = None,
        worker_id: Optional[str] = None,
    ):
        """Initialize async worker."""
        if config is None:
            config = AsyncWorkerConfig()

        self.config = config
        self.executor = executor
        self.worker_id = worker_id or f"async-worker-{id(self)}"
        self.broker = AsyncTaskBroker(config)
        self.running = False
        self.tasks_processed = 0
        self.tasks_succeeded = 0
        self.tasks_failed = 0
        self.active_tasks = set()
        self.start_time = datetime.utcnow()

        logger.info(f"AsyncTaskWorker initialized: {self.worker_id}")

    async def process_task(self, task_dict: Dict[str, Any]) -> bool:
        """
        Process a single task asynchronously.

        Args:
            task_dict: Task dictionary from broker

        Returns:
            True if task succeeded, False if failed
        """
        try:
            task = Task.from_dict(task_dict)
            logger.info(f"Processing task {task.id} (type: {task.type})")

            # Check timeout
            if task.is_expired():
                logger.warning(f"Task {task.id} already expired")
                task.fail("Task expired before execution")
                self.tasks_failed += 1
                return False

            # Mark task as started
            task.start()

            # Execute task
            try:
                result = await self.executor.execute(task)
                task.complete(result=result)
                await self.broker.mark_completed(task.id)
                self.tasks_succeeded += 1
                logger.info(f"Task {task.id} completed successfully")
                return True

            except Exception as e:
                # Handle failure
                error_msg = str(e)
                should_retry = task.fail(error_msg)

                if should_retry:
                    # Exponential backoff
                    await self.broker.set_task_status(task.id, "retrying")

                    backoff_delay = min(
                        self.config.exponential_backoff_base * (2 ** task.retry_count),
                        self.config.exponential_backoff_max,
                    )

                    logger.info(
                        f"Task {task.id} will be retried after {backoff_delay:.2f}s "
                        f"(attempt {task.retry_count}/{task.max_retries})"
                    )

                    # Async sleep instead of blocking
                    await asyncio.sleep(backoff_delay)

                    # Re-enqueue (would need async enqueue in real implementation)
                    task_json = json.dumps(task.to_dict())
                    await self.broker.redis.lpush("task_queue", task_json)

                else:
                    # Task exhausted retries
                    logger.error(f"Task {task.id} failed permanently after retries")
                    reason = f"Exhausted {task.max_retries} retries. Last error: {error_msg}"
                    await self.broker.move_to_dlq(task.to_dict(), reason)
                    self.tasks_failed += 1

                return should_retry

        except Exception as e:
            logger.error(f"Error processing task: {e}")
            self.tasks_failed += 1
            return False

    async def process_queue(self, max_tasks: Optional[int] = None) -> int:
        """
        Process tasks from queue with concurrency control.

        Args:
            max_tasks: Maximum tasks to process

        Returns:
            Number of tasks processed
        """
        tasks_processed = 0

        while self.running:
            # Check limits
            if max_tasks and tasks_processed >= max_tasks:
                break

            # Don't exceed concurrent task limit
            if len(self.active_tasks) >= self.config.max_concurrent_tasks:
                await asyncio.sleep(0.1)
                continue

            # Try to dequeue task
            task_dict = await self.broker.dequeue()
            if task_dict is None:
                # No tasks available, wait a bit
                await asyncio.sleep(self.config.poll_interval)
                continue

            # Create task processing coroutine
            coro = self.process_task(task_dict)
            task_future = asyncio.create_task(coro)

            # Track active task
            self.active_tasks.add(task_future)
            task_future.add_done_callback(self.active_tasks.discard)

            self.tasks_processed += 1
            tasks_processed += 1

        # Wait for remaining tasks
        if self.active_tasks:
            await asyncio.gather(*self.active_tasks, return_exceptions=True)

        return tasks_processed

    async def run(self, max_duration: Optional[float] = None) -> None:
        """
        Start the async worker loop.

        Args:
            max_duration: Maximum seconds to run
        """
        self.running = True
        loop_start = datetime.utcnow()

        logger.info(
            f"Async worker {self.worker_id} starting... "
            f"(max_concurrent: {self.config.max_concurrent_tasks}, "
            f"poll_interval: {self.config.poll_interval}s)"
        )

        try:
            await self.broker.connect()

            while self.running:
                # Check duration
                if max_duration:
                    elapsed = (datetime.utcnow() - loop_start).total_seconds()
                    if elapsed > max_duration:
                        logger.info(f"Worker reached max duration: {max_duration}s")
                        break

                # Try to process task
                queue_size = await self.broker.get_queue_size()
                if queue_size > 0:
                    task_dict = await self.broker.dequeue()
                    if task_dict:
                        # Create non-blocking task
                        coro = self.process_task(task_dict)
                        task_future = asyncio.create_task(coro)
                        self.active_tasks.add(task_future)
                        task_future.add_done_callback(self.active_tasks.discard)
                        self.tasks_processed += 1
                else:
                    # No tasks, sleep briefly
                    await asyncio.sleep(self.config.poll_interval)

                # Yield control to other tasks
                await asyncio.sleep(0)

        except asyncio.CancelledError:
            logger.info("Async worker cancelled")
        except Exception as e:
            logger.error(f"Async worker error: {e}")
            raise
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the worker."""
        self.running = False
        logger.info(f"Async worker {self.worker_id} stopped")

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        uptime = (datetime.utcnow() - self.start_time).total_seconds()

        return {
            "worker_id": self.worker_id,
            "uptime_seconds": uptime,
            "tasks_processed": self.tasks_processed,
            "tasks_succeeded": self.tasks_succeeded,
            "tasks_failed": self.tasks_failed,
            "active_tasks": len(self.active_tasks),
            "success_rate": (
                self.tasks_succeeded / self.tasks_processed
                if self.tasks_processed > 0
                else 0
            ),
        }

    def print_stats(self) -> None:
        """Print worker statistics."""
        stats = self.get_stats()
        print("\n" + "="*50)
        print(f"Async Worker Statistics: {stats['worker_id']}")
        print("="*50)
        print(f"Uptime: {stats['uptime_seconds']:.2f}s")
        print(f"Tasks Processed: {stats['tasks_processed']}")
        print(f"  - Succeeded: {stats['tasks_succeeded']}")
        print(f"  - Failed: {stats['tasks_failed']}")
        print(f"  - Active: {stats['active_tasks']}")
        print(f"Success Rate: {stats['success_rate']*100:.1f}%")
        print("="*50 + "\n")

    async def close(self) -> None:
        """Close worker and disconnect from Redis."""
        try:
            self.stop()
            
            # Wait for active tasks
            if self.active_tasks:
                await asyncio.gather(*self.active_tasks, return_exceptions=True)
            
            await self.broker.disconnect()
            logger.info(f"Async worker {self.worker_id} closed")
        except Exception as e:
            logger.error(f"Error closing async worker: {e}")


async def main_example():
    """Example usage of async worker."""
    
    # Define async handlers
    async def handle_send_email(data: Dict[str, Any]) -> Dict[str, Any]:
        """Async email handler."""
        await asyncio.sleep(0.1)  # Simulate work
        return {"status": "sent", "recipient": data.get("recipient")}

    async def handle_process_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Async data processing handler."""
        await asyncio.sleep(0.2)  # Simulate work
        return {"status": "processed", "file": data.get("file")}

    # Create executor and register handlers
    executor = AsyncTaskExecutor()
    executor.register("send_email", handle_send_email)
    executor.register("process_data", handle_process_data)

    # Create and run worker
    config = AsyncWorkerConfig(
        redis_host="localhost",
        redis_port=6379,
        max_concurrent_tasks=5,
        poll_interval=0.5,
    )

    worker = AsyncTaskWorker(executor, config, worker_id="async-worker-1")

    try:
        # Run worker for 30 seconds
        await worker.run(max_duration=30)
        worker.print_stats()
    finally:
        await worker.close()


if __name__ == "__main__":
    # Run example
    print("\nNote: This example requires Redis running on localhost:6379")
    print("and redis[asyncio] installed. Install with: pip install redis")
    print()

    try:
        asyncio.run(main_example())
    except KeyboardInterrupt:
        print("\nAsync worker interrupted")
