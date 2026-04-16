import uuid
import json
import logging
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from enum import Enum
import time


logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Enumeration of possible task states."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class Task:
    """
    Represents a unit of work in the task queue system.
    Encapsulates task data, metadata, and lifecycle management.
    """

    def __init__(
        self,
        task_type: str,
        data: Dict[str, Any],
        task_id: Optional[str] = None,
        priority: int = 0,
        max_retries: int = 3,
        timeout: Optional[int] = None,
        execute_at: Optional[datetime] = None,
    ):
        """
        Initialize a new task.

        Args:
            task_type: Type/name of the task (e.g., 'email_send', 'data_process')
            data: Dictionary containing task payload/parameters
            task_id: Unique task identifier (auto-generated if not provided)
            priority: Task priority (higher values = higher priority)
            max_retries: Maximum number of retry attempts on failure
            timeout: Task execution timeout in seconds (None = no timeout)
            execute_at: Optional datetime for delayed task execution
        """
        self.id = task_id or str(uuid.uuid4())
        self.type = task_type
        self.data = data
        self.priority = priority
        self.max_retries = max_retries
        self.timeout = timeout
        self.execute_at = execute_at
        self.status = TaskStatus.PENDING
        self.retry_count = 0
        self.result = None
        self.error = None
        self.created_at = datetime.now(timezone.utc)
        self.started_at = None
        self.completed_at = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert task to dictionary for serialization.

        Returns:
            Dictionary representation of the task
        """
        return {
            "id": self.id,
            "type": self.type,
            "data": self.data,
            "priority": self.priority,
            "max_retries": self.max_retries,
            "timeout": self.timeout,
            "execute_at": self.execute_at.isoformat() if self.execute_at else None,
            "status": self.status.value,
            "retry_count": self.retry_count,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        """
        Create task instance from dictionary (deserialization).

        Args:
            data: Dictionary containing task data

        Returns:
            Task instance
        """
        try:
            execute_at = None
            if data.get("execute_at"):
                execute_at = datetime.fromisoformat(data["execute_at"])
            
            task = cls(
                task_type=data["type"],
                data=data["data"],
                task_id=data.get("id"),
                priority=data.get("priority", 0),
                max_retries=data.get("max_retries", 3),
                timeout=data.get("timeout"),
                execute_at=execute_at,
            )

            # Restore state
            task.status = TaskStatus(data.get("status", "pending"))
            task.retry_count = data.get("retry_count", 0)
            task.result = data.get("result")
            task.error = data.get("error")

            if data.get("created_at"):
                task.created_at = datetime.fromisoformat(data["created_at"])
            if data.get("started_at"):
                task.started_at = datetime.fromisoformat(data["started_at"])
            if data.get("completed_at"):
                task.completed_at = datetime.fromisoformat(data["completed_at"])

            return task
        except (KeyError, ValueError) as e:
            logger.error(f"Error deserializing task: {e}")
            raise

    def to_json(self) -> str:
        """
        Serialize task to JSON string.

        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> 'Task':
        """
        Create task instance from JSON string.

        Args:
            json_str: JSON string representation of task

        Returns:
            Task instance
        """
        try:
            data = json.loads(json_str)
            return cls.from_dict(data)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding task JSON: {e}")
            raise

    def start(self) -> None:
        """Mark task as started (processing)."""
        self.status = TaskStatus.PROCESSING
        self.started_at = datetime.now(timezone.utc)
        logger.info(f"Task {self.id} started")

    def complete(self, result: Optional[Any] = None) -> None:
        """
        Mark task as completed successfully.

        Args:
            result: Optional result data from task execution
        """
        self.status = TaskStatus.COMPLETED
        self.result = result
        self.completed_at = datetime.now(timezone.utc)
        logger.info(f"Task {self.id} completed with result: {result}")

    def fail(self, error: str) -> bool:
        """
        Mark task as failed and determine if retry should occur.

        Args:
            error: Error message/description

        Returns:
            True if task should be retried, False otherwise
        """
        self.error = error
        logger.warning(f"Task {self.id} failed: {error}")

        # Check if we should retry
        if self.retry_count < self.max_retries:
            self.retry_count += 1
            self.status = TaskStatus.RETRYING
            logger.info(f"Task {self.id} will retry (attempt {self.retry_count}/{self.max_retries})")
            return True
        else:
            self.status = TaskStatus.FAILED
            self.completed_at = datetime.now(timezone.utc)
            logger.error(
                f"Task {self.id} exhausted all retries ({self.max_retries}). Marking as failed."
            )
            return False

    def should_retry(self) -> bool:
        """Check if task can be retried."""
        return (
            self.status == TaskStatus.RETRYING
            and self.retry_count <= self.max_retries
        )

    def is_expired(self) -> bool:
        """
        Check if task execution has exceeded timeout.

        Returns:
            True if task has timed out, False otherwise
        """
        if not self.timeout or not self.started_at:
            return False

        elapsed = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        return elapsed > self.timeout

    def get_age(self) -> float:
        """Get task age in seconds since creation."""
        return (datetime.now(timezone.utc) - self.created_at).total_seconds()

    def get_execution_time(self) -> Optional[float]:
        """Get task execution duration in seconds (or None if not started/completed)."""
        if not self.started_at or not self.completed_at:
            return None
        return (self.completed_at - self.started_at).total_seconds()

    def validate(self) -> bool:
        """
        Validate task data integrity.

        Returns:
            True if task is valid, False otherwise
        """
        try:
            # Check required fields
            if not self.id or not isinstance(self.id, str):
                logger.error("Task ID is missing or invalid")
                return False

            if not self.type or not isinstance(self.type, str):
                logger.error("Task type is missing or invalid")
                return False

            if not isinstance(self.data, dict):
                logger.error("Task data must be a dictionary")
                return False

            if not isinstance(self.priority, int):
                logger.error("Task priority must be an integer")
                return False

            if not isinstance(self.max_retries, int) or self.max_retries < 0:
                logger.error("Max retries must be a non-negative integer")
                return False

            logger.info(f"Task {self.id} validated successfully")
            return True
        except Exception as e:
            logger.error(f"Task validation error: {e}")
            return False

    def __repr__(self) -> str:
        """String representation of task."""
        return (
            f"Task(id={self.id}, type={self.type}, status={self.status.value}, "
            f"priority={self.priority}, retries={self.retry_count}/{self.max_retries})"
        )

    def __eq__(self, other: object) -> bool:
        """Compare tasks by ID."""
        if not isinstance(other, Task):
            return False
        return self.id == other.id

    def __hash__(self) -> int:
        """Allow tasks to be used in sets and as dict keys."""
        return hash(self.id)


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    # Create a task
    task = Task(
        task_type="send_email",
        data={"recipient": "user@example.com", "subject": "Hello"},
        priority=1,
        max_retries=3,
        timeout=30,
    )

    print(f"Created: {task}")
    print(f"Valid: {task.validate()}")

    # Simulate task lifecycle
    task.start()
    print(f"Started: {task}")

    task.complete(result={"message_id": "msg-123"})
    print(f"Completed: {task}")
    print(f"Execution time: {task.get_execution_time():.2f}s")

    # Serialize and deserialize
    task_json = task.to_json()
    print(f"JSON: {task_json}")

    restored_task = Task.from_json(task_json)
    print(f"Restored: {restored_task}")

    # Test retry logic
    failing_task = Task(
        task_type="process_data",
        data={"file": "data.csv"},
        max_retries=2,
    )
    print(f"\nFailing task: {failing_task}")
    should_retry = failing_task.fail("Connection timeout")
    print(f"Should retry: {should_retry}")
    print(f"Status: {failing_task.status.value}")