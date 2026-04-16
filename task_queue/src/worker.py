import logging
import time
import signal
import threading
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timezone
from broker import TaskBroker
from task import Task, TaskStatus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    """Configuration for task worker."""

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    max_connections: int = 50
    poll_interval: float = 1.0  # seconds between queue checks
    num_threads: int = 1  # number of worker threads
    exponential_backoff_base: float = 1.0  # base delay for exponential backoff (seconds)
    exponential_backoff_max: float = 300.0  # max delay for exponential backoff (seconds)
    scheduler_interval: float = 5.0  # seconds between delayed task checks


class TaskExecutor:
    """
    Registry and executor for task handlers.
    Maps task types to their handler functions.
    """

    def __init__(self):
        """Initialize the task executor."""
        self.handlers: Dict[str, Callable] = {}
        logger.info("TaskExecutor initialized")

    def register(self, task_type: str, handler: Callable) -> None:
        """
        Register a handler for a specific task type.

        Args:
            task_type: Type of task this handler processes
            handler: Callable that processes the task

        Raises:
            ValueError: If task_type is already registered
        """
        if task_type in self.handlers:
            raise ValueError(f"Handler already registered for task type: {task_type}")

        self.handlers[task_type] = handler
        logger.info(f"Handler registered for task type: {task_type}")

    def unregister(self, task_type: str) -> None:
        """Unregister a handler for a task type."""
        if task_type in self.handlers:
            del self.handlers[task_type]
            logger.info(f"Handler unregistered for task type: {task_type}")

    def has_handler(self, task_type: str) -> bool:
        """Check if handler exists for task type."""
        return task_type in self.handlers

    def execute(self, task: Task) -> Any:
        """
        Execute a task using its registered handler.

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
        logger.info(f"Executing handler for task {task.id} (type: {task.type})")

        try:
            result = handler(task.data)
            logger.info(f"Task {task.id} execution succeeded")
            return result
        except Exception as e:
            logger.error(f"Task {task.id} handler execution failed: {e}")
            raise


class TaskWorker:
    """
    Consumes tasks from queue and executes them.
    Handles task lifecycle including retries and error handling.
    """

    def __init__(
        self,
        executor: TaskExecutor,
        config: Optional[WorkerConfig] = None,
        worker_id: Optional[str] = None,
    ):
        """
        Initialize the task worker.

        Args:
            executor: TaskExecutor instance for task type handlers
            config: WorkerConfig with broker settings
            worker_id: Optional unique worker identifier
        """
        if config is None:
            config = WorkerConfig()

        self.config = config
        self.executor = executor
        self.worker_id = worker_id or f"worker-{int(time.time())}"
        self.broker = TaskBroker(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            password=config.redis_password,
            max_connections=config.max_connections,
        )
        self.running = False
        self.tasks_processed = 0
        self.tasks_succeeded = 0
        self.tasks_failed = 0
        self.start_time = datetime.now(timezone.utc)
        self.scheduler_thread = None
        self.scheduler_running = False

        logger.info(f"TaskWorker initialized: {self.worker_id}")

    def process_task(self, task_dict: Dict[str, Any]) -> bool:
        """
        Process a single task.

        Args:
            task_dict: Task dictionary from broker

        Returns:
            True if task succeeded, False if failed
        """
        try:
            # Deserialize task
            task = Task.from_dict(task_dict)
            logger.info(f"Processing task {task.id} (type: {task.type})")

            # Check timeout before starting
            if task.is_expired():
                logger.warning(f"Task {task.id} already expired, marking as failed")
                task.fail("Task expired before execution")
                self.tasks_failed += 1
                return False

            # Mark task as started
            task.start()

            # Execute task
            try:
                result = self.executor.execute(task)
                task.complete(result=result)
                self.broker.mark_completed(task.id)
                self.tasks_succeeded += 1
                logger.info(f"Task {task.id} completed successfully")
                return True

            except Exception as e:
                # Handle execution failure
                error_msg = str(e)
                should_retry = task.fail(error_msg)

                if should_retry:
                    # Update task status to retrying
                    self.broker.set_task_status(task.id, "retrying")
                    
                    # Calculate exponential backoff delay
                    backoff_delay = min(
                        self.config.exponential_backoff_base * (2 ** task.retry_count),
                        self.config.exponential_backoff_max,
                    )
                    logger.info(
                        f"Task {task.id} will be retried after {backoff_delay:.2f}s "
                        f"(attempt {task.retry_count}/{task.max_retries})"
                    )
                    
                    # Apply exponential backoff delay
                    time.sleep(backoff_delay)
                    
                    # Re-enqueue for retry
                    self.broker.enqueue(task.to_dict())
                else:
                    # Task exhausted retries - move to dead-letter queue
                    logger.error(f"Task {task.id} failed permanently after retries")
                    reason = f"Exhausted {task.max_retries} retries. Last error: {error_msg}"
                    self.broker.move_to_dlq(task.to_dict(), reason)
                    self.tasks_failed += 1

                return should_retry

        except Exception as e:
            logger.error(f"Error processing task: {e}")
            self.tasks_failed += 1
            return False

    def process_queue(self, max_tasks: Optional[int] = None) -> int:
        """
        Process tasks from queue until empty or max_tasks reached.

        Args:
            max_tasks: Maximum tasks to process (None = process all)

        Returns:
            Number of tasks processed
        """
        tasks_processed = 0

        while True:
            # Check limit
            if max_tasks is not None and tasks_processed >= max_tasks:
                logger.info(f"Reached max task limit: {max_tasks}")
                break

            # Dequeue task
            task_dict = self.broker.dequeue()
            if task_dict is None:
                logger.info("Queue is empty")
                break

            # Process task
            self.process_task(task_dict)
            self.tasks_processed += 1
            tasks_processed += 1

        return tasks_processed

    def run(self, max_duration: Optional[float] = None) -> None:
        """
        Start worker loop - continuously consume and process tasks.
        Also starts the scheduler thread for delayed task promotion.

        Args:
            max_duration: Maximum seconds to run (None = infinite)

        Raises:
            Exception: If broker connection fails
        """
        self.running = True
        
        # Start scheduler thread for delayed task management
        self.start_scheduler()
        
        loop_start = time.time()

        logger.info(
            f"Worker {self.worker_id} starting... (poll_interval: {self.config.poll_interval}s)"
        )

        try:
            while self.running:
                # Check duration limit
                if max_duration is not None:
                    elapsed = time.time() - loop_start
                    if elapsed > max_duration:
                        logger.info(f"Worker reached max duration: {max_duration}s")
                        break

                # Try to process available tasks
                queue_size = self.broker.get_queue_size()
                if queue_size > 0:
                    self.process_queue(max_tasks=1)
                else:
                    logger.debug("Queue is empty, waiting...")
                    time.sleep(self.config.poll_interval)

        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        except Exception as e:
            logger.error(f"Worker error: {e}")
            raise
        finally:
            self.stop()

    def run_parallel(self, num_threads: int, max_duration: Optional[float] = None) -> None:
        """
        Start multiple worker threads for parallel task processing.
        Also starts the scheduler thread for delayed task promotion.

        Args:
            num_threads: Number of worker threads to spawn
            max_duration: Maximum seconds to run (None = infinite)
        """
        self.running = True
        
        # Start scheduler thread for delayed task management
        self.start_scheduler()
        
        threads = []

        logger.info(f"Starting {num_threads} worker threads...")

        try:
            for i in range(num_threads):
                thread_id = f"{self.worker_id}-thread-{i}"
                thread = threading.Thread(
                    target=self._run_thread,
                    args=(thread_id, max_duration),
                    daemon=False,
                )
                thread.start()
                threads.append(thread)
                logger.info(f"Started worker thread: {thread_id}")

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            logger.info("All worker threads completed")

        except KeyboardInterrupt:
            logger.info("Parallel workers interrupted")
            self.running = False
            for thread in threads:
                thread.join(timeout=5)

    def _run_thread(self, thread_id: str, max_duration: Optional[float]) -> None:
        """Worker thread execution function."""
        logger.info(f"Thread {thread_id} started")
        loop_start = time.time()

        try:
            while self.running:
                # Check duration limit
                if max_duration is not None:
                    elapsed = time.time() - loop_start
                    if elapsed > max_duration:
                        logger.info(f"Thread {thread_id} reached max duration")
                        break

                # Try to process one task
                task_dict = self.broker.dequeue()
                if task_dict:
                    self.process_task(task_dict)
                    self.tasks_processed += 1
                else:
                    time.sleep(self.config.poll_interval)

        except Exception as e:
            logger.error(f"Thread {thread_id} error: {e}")
        finally:
            logger.info(f"Thread {thread_id} stopped")

    def _run_scheduler(self) -> None:
        """
        Scheduler thread that periodically checks for delayed tasks ready to be executed.
        Promotes ready delayed tasks to the active queue for processing.
        """
        logger.info("Scheduler thread started")
        
        try:
            while self.scheduler_running:
                try:
                    # Promote any delayed tasks that are ready
                    promoted = self.broker.promote_ready_delayed_tasks()
                    
                    if promoted > 0:
                        logger.info(f"Scheduler promoted {promoted} delayed tasks")
                    
                    # Check delayed queue size periodically
                    delayed_size = self.broker.get_delayed_queue_size()
                    if delayed_size > 0:
                        logger.debug(f"Delayed queue size: {delayed_size}")
                    
                    # Sleep before next check
                    time.sleep(self.config.scheduler_interval)
                    
                except Exception as e:
                    logger.error(f"Error in scheduler: {e}")
                    time.sleep(self.config.scheduler_interval)
                    
        except Exception as e:
            logger.error(f"Scheduler thread error: {e}")
        finally:
            logger.info("Scheduler thread stopped")

    def start_scheduler(self) -> None:
        """Start the scheduler thread for delayed task management."""
        if self.scheduler_thread is None or not self.scheduler_thread.is_alive():
            self.scheduler_running = True
            self.scheduler_thread = threading.Thread(
                target=self._run_scheduler,
                daemon=True,
                name=f"{self.worker_id}-scheduler"
            )
            self.scheduler_thread.start()
            logger.info(f"Scheduler thread started for {self.worker_id} (interval: {self.config.scheduler_interval}s)")
        else:
            logger.warning("Scheduler thread is already running")

    def stop_scheduler(self) -> None:
        """Stop the scheduler thread."""
        self.scheduler_running = False
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)
            logger.info("Scheduler thread stopped")

    def stop(self) -> None:
        """Stop the worker and scheduler."""
        self.running = False
        self.stop_scheduler()
        logger.info(f"Worker {self.worker_id} stopped")

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()

        return {
            "worker_id": self.worker_id,
            "uptime_seconds": uptime,
            "tasks_processed": self.tasks_processed,
            "tasks_succeeded": self.tasks_succeeded,
            "tasks_failed": self.tasks_failed,
            "success_rate": (
                self.tasks_succeeded / self.tasks_processed
                if self.tasks_processed > 0
                else 0
            ),
            "queue_size": self.broker.get_queue_size(),
            "processing_size": self.broker.get_processing_size(),
            "completed_count": self.broker.get_completed_count(),
            "delayed_size": self.broker.get_delayed_queue_size(),
        }

    def print_stats(self) -> None:
        """Print worker statistics in human-readable format."""
        stats = self.get_stats()
        print("\n" + "=" * 50)
        print(f"Worker Statistics: {stats['worker_id']}")
        print("=" * 50)
        print(f"Uptime: {stats['uptime_seconds']:.2f}s")
        print(f"Tasks Processed: {stats['tasks_processed']}")
        print(f"  - Succeeded: {stats['tasks_succeeded']}")
        print(f"  - Failed: {stats['tasks_failed']}")
        print(f"Success Rate: {stats['success_rate']*100:.1f}%")
        print(f"Queue Status:")
        print(f"  - Pending: {stats['queue_size']}")
        print(f"  - Processing: {stats['processing_size']}")
        print(f"  - Completed: {stats['completed_count']}")
        print(f"  - Delayed: {stats['delayed_size']}")
        print("=" * 50 + "\n")

    def close(self) -> None:
        """Close worker and broker connection."""
        try:
            self.stop()
            self.broker.close()
            logger.info(f"Worker {self.worker_id} closed")
        except Exception as e:
            logger.error(f"Error closing worker: {e}")


class WorkerContext:
    """Context manager for TaskWorker for automatic resource cleanup."""

    def __init__(
        self,
        executor: TaskExecutor,
        config: Optional[WorkerConfig] = None,
        worker_id: Optional[str] = None,
    ):
        """Initialize context manager."""
        self.executor = executor
        self.config = config
        self.worker_id = worker_id
        self.worker = None

    def __enter__(self) -> TaskWorker:
        """Enter context and create worker."""
        self.worker = TaskWorker(self.executor, self.config, self.worker_id)
        return self.worker

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context and close worker."""
        if self.worker:
            self.worker.close()


if __name__ == "__main__":
    # Example usage with task handlers

    # Define some example task handlers
    def handle_send_email(data: Dict[str, Any]) -> Dict[str, Any]:
        """Example email task handler."""
        recipient = data.get("recipient")
        subject = data.get("subject")
        time.sleep(1)  # Simulate work
        return {"status": "sent", "recipient": recipient, "subject": subject}

    def handle_process_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Example data processing task handler."""
        file_path = data.get("file")
        operation = data.get("operation")
        time.sleep(2)  # Simulate work
        return {"status": "processed", "file": file_path, "operation": operation}

    # Create executor and register handlers
    executor = TaskExecutor()
    executor.register("send_email", handle_send_email)
    executor.register("process_data", handle_process_data)

    # Create and run worker with context manager
    config = WorkerConfig(
        redis_host="localhost",
        redis_port=6379,
        poll_interval=1.0,
    )

    with WorkerContext(executor, config, "worker-1") as worker:
        print(f"Starting worker: {worker.worker_id}")

        # Process tasks for a limited duration (for demo)
        # Uncomment one of these:

        # Single-threaded processing
        # worker.run(max_duration=30)

        # Multi-threaded processing
        # worker.run_parallel(num_threads=4, max_duration=30)

        # Process fixed number of tasks
        # worker.process_queue(max_tasks=10)

        # Print statistics
        worker.print_stats()
