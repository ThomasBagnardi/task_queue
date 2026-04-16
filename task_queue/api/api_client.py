"""
FastAPI Control Plane - API Client and Usage Examples
Demonstrates how to interact with the Task Queue Control Plane API
"""

import os
import requests
import json
import time
from typing import Dict, Any, List, Optional


class TaskQueueClient:
    """
    Client for interacting with the Task Queue Control Plane API.
    Provides high-level methods for common operations.
    """
    
    def __init__(self, base_url: str = None):
        """
        Initialize the API client.
        
        Args:
            base_url: Base URL of the API (default: http://localhost:8000 or based on API_PORT env var)
        """
        if base_url is None:
            # Check for API_PORT environment variable
            api_port = os.environ.get("API_PORT", "8000")
            api_host = os.environ.get("API_HOST", "localhost")
            base_url = f"http://{api_host}:{api_port}"
        
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
    
    def health_check(self) -> Dict[str, Any]:
        """Check API health status."""
        response = self.session.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()
    
    def submit_task(
        self,
        task_type: str,
        data: Dict[str, Any],
        priority: int = 0,
        max_retries: int = 3,
        timeout: Optional[int] = None
    ) -> str:
        """
        Submit a single task to the queue.
        
        Args:
            task_type: Type of task
            data: Task payload
            priority: Task priority
            max_retries: Maximum retry attempts
            timeout: Execution timeout in seconds
            
        Returns:
            Task ID of the submitted task
        """
        payload = {
            "task_type": task_type,
            "data": data,
            "priority": priority,
            "max_retries": max_retries,
            "timeout": timeout
        }
        
        response = self.session.post(
            f"{self.base_url}/tasks/submit",
            json=payload
        )
        response.raise_for_status()
        return response.json()["task_id"]
    
    def submit_batch(
        self,
        tasks: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Submit multiple tasks in a batch.
        
        Args:
            tasks: List of task dictionaries
            
        Returns:
            List of submitted task IDs
        """
        response = self.session.post(
            f"{self.base_url}/tasks/batch-submit",
            json=tasks
        )
        response.raise_for_status()
        return response.json()["task_ids"]
    
    def get_task(self, task_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Task information dictionary
        """
        response = self.session.get(f"{self.base_url}/tasks/{task_id}")
        response.raise_for_status()
        return response.json()
    
    def get_task_status(self, task_id: str) -> str:
        """
        Get only the status of a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Task status (e.g., "pending", "processing", "completed")
        """
        response = self.session.get(f"{self.base_url}/tasks/{task_id}/status")
        response.raise_for_status()
        return response.json()["status"]
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status and statistics."""
        response = self.session.get(f"{self.base_url}/queue/status")
        response.raise_for_status()
        return response.json()
    
    def get_pending_tasks(self, limit: int = 100) -> Dict[str, Any]:
        """Get pending tasks in the queue."""
        response = self.session.get(
            f"{self.base_url}/queue/pending",
            params={"limit": limit}
        )
        response.raise_for_status()
        return response.json()
    
    def get_dlq_tasks(
        self,
        task_type: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get dead-lettered (failed) tasks.
        
        Args:
            task_type: Filter by task type
            limit: Maximum number of tasks to return
            
        Returns:
            Dictionary with DLQ task information
        """
        params = {"limit": limit}
        if task_type:
            params["task_type"] = task_type
        
        response = self.session.get(
            f"{self.base_url}/queue/dlq",
            params=params
        )
        response.raise_for_status()
        return response.json()
    
    def retry_task(self, task_id: str) -> str:
        """
        Retry a failed task by requeuing it.
        
        Args:
            task_id: ID of the task to retry
            
        Returns:
            New task ID for the retry
        """
        response = self.session.post(f"{self.base_url}/tasks/{task_id}/retry")
        response.raise_for_status()
        return response.json()["task_id"]
    
    def clear_dlq_task(self, task_id: str) -> Dict[str, Any]:
        """
        Remove a task from the dead-letter queue.
        
        Args:
            task_id: ID of the task to clear
            
        Returns:
            Operation result
        """
        response = self.session.post(f"{self.base_url}/tasks/{task_id}/clear-dlq")
        response.raise_for_status()
        return response.json()


def example_submit_single_task():
    """Example: Submit a single task."""
    print("\n" + "="*60)
    print("Example 1: Submit a Single Task")
    print("="*60)
    
    client = TaskQueueClient()
    
    # Submit an email task
    task_id = client.submit_task(
        task_type="send_email",
        data={
            "recipient": "user@example.com",
            "subject": "Welcome!",
            "body": "Hello, welcome to our service!"
        },
        priority=1,
        max_retries=3,
        timeout=30
    )
    
    print(f"✓ Task submitted successfully!")
    print(f"  Task ID: {task_id}")
    
    # Check status
    status = client.get_task_status(task_id)
    print(f"  Status: {status}")


def example_submit_batch_tasks():
    """Example: Submit multiple tasks in a batch."""
    print("\n" + "="*60)
    print("Example 2: Submit Batch of Tasks")
    print("="*60)
    
    client = TaskQueueClient()
    
    # Create multiple tasks
    tasks = [
        {
            "task_type": "send_email",
            "data": {"recipient": f"user{i}@example.com"},
            "priority": 1
        }
        for i in range(5)
    ]
    
    task_ids = client.submit_batch(tasks)
    
    print(f"✓ Batch submitted successfully!")
    print(f"  Submitted {len(task_ids)} tasks")
    for i, task_id in enumerate(task_ids, 1):
        print(f"  {i}. {task_id}")


def example_monitor_tasks():
    """Example: Monitor task status."""
    print("\n" + "="*60)
    print("Example 3: Monitor Task Status")
    print("="*60)
    
    client = TaskQueueClient()
    
    # Submit a task
    task_id = client.submit_task(
        task_type="process_data",
        data={"file": "data.csv"},
        timeout=60
    )
    
    print(f"✓ Task submitted: {task_id}")
    
    # Monitor status
    for i in range(5):
        try:
            status = client.get_task_status(task_id)
            print(f"  Check {i+1}: Status = {status}")
            
            if status in ["completed", "failed"]:
                task_info = client.get_task(task_id)
                if status == "completed":
                    print(f"  Result: {task_info.get('result')}")
                else:
                    print(f"  Error: {task_info.get('error')}")
                break
        except requests.exceptions.RequestException as e:
            print(f"  Check {i+1}: {e}")
        
        time.sleep(1)


def example_queue_status():
    """Example: Check queue status."""
    print("\n" + "="*60)
    print("Example 4: Queue Status")
    print("="*60)
    
    client = TaskQueueClient()
    
    status = client.get_queue_status()
    
    print(f"✓ Queue Status:")
    print(f"  Pending tasks: {status['queue_size']}")
    print(f"  Processing: {status['processing_count']}")
    print(f"  Completed: {status['completed_count']}")
    print(f"  Dead-lettered: {status['dlq_size']}")


def example_health_check():
    """Example: Health check."""
    print("\n" + "="*60)
    print("Example 5: Health Check")
    print("="*60)
    
    client = TaskQueueClient()
    
    health = client.health_check()
    
    print(f"✓ API Status: {health['status']}")
    print(f"  Redis connected: {health['redis_connected']}")
    print(f"  Queue size: {health['queue_size']}")


def example_retry_failed_task():
    """Example: Retry a failed task."""
    print("\n" + "="*60)
    print("Example 6: Retry Failed Task")
    print("="*60)
    
    client = TaskQueueClient()
    
    try:
        # Get DLQ tasks
        dlq_response = client.get_dlq_tasks(limit=1)
        tasks = dlq_response.get("tasks", [])
        
        if not tasks:
            print("ℹ No dead-lettered tasks found")
            return
        
        failed_task = tasks[0]
        task_id = failed_task["task_id"]
        
        print(f"✓ Found failed task: {task_id}")
        print(f"  Type: {failed_task['task_type']}")
        print(f"  Reason: {failed_task['reason']}")
        
        # Retry the task
        new_task_id = client.retry_task(task_id)
        
        print(f"✓ Task retried!")
        print(f"  Original task: {task_id}")
        print(f"  New task ID: {new_task_id}")
    except Exception as e:
        print(f"Error: {e}")


def main():
    """Run all examples."""
    print("\n" + "="*60)
    print("Task Queue Control Plane - API Examples")
    print("="*60)
    
    try:
        # Test health first
        client = TaskQueueClient()
        health = client.health_check()
        
        if health['status'] != 'healthy':
            print("\n⚠ Warning: API not fully healthy")
            print("Make sure Redis is running and API is started with:")
            print("  python api.py")
            return
        
        print("✓ API is healthy and ready!\n")
        
        # Run examples
        example_health_check()
        example_queue_status()
        example_submit_single_task()
        example_submit_batch_tasks()
        example_monitor_tasks()
        example_queue_status()
        
        # Only run retry example if there are failed tasks
        print("\n" + "="*60)
        print("To test retry functionality:")
        print("1. Create and fail some tasks")
        print("2. Wait for them to be moved to DLQ")
        print("3. Call example_retry_failed_task()")
        print("="*60 + "\n")
        
    except requests.exceptions.ConnectionError:
        print("\n⚠ Could not connect to API at http://localhost:8000")
        print("Make sure to start the API with:")
        print("  python api.py")
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()