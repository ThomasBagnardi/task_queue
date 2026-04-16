#!/usr/bin/env python3
"""
Task Queue Control Plane - Quick Start Guide
Run this script to test the API with various scenarios
"""

import time
import subprocess
import sys
from api_client import TaskQueueClient


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


def print_success(message):
    """Print a success message."""
    print(f"✓ {message}")


def print_info(message):
    """Print an info message."""
    print(f"ℹ {message}")


def print_warning(message):
    """Print a warning message."""
    print(f"⚠ {message}")


def print_error(message):
    """Print an error message."""
    print(f"✗ {message}")


def wait_for_api(max_retries=10, timeout=1):
    """Wait for API to be available."""
    print_section("Checking API Availability")
    print("Waiting for API to be ready...")
    
    client = TaskQueueClient()
    
    for attempt in range(max_retries):
        try:
            health = client.health_check()
            print_success(f"API is ready! (attempt {attempt + 1})")
            return True
        except Exception as e:
            print(f"  Attempt {attempt + 1}/{max_retries}: Not ready - {type(e).__name__}")
            if attempt < max_retries - 1:
                time.sleep(timeout)
    
    print_error("API is not responding. Make sure it's started with: python api.py")
    return False


def test_health_check():
    """Test: Health check endpoint."""
    print_section("Test 1: Health Check")
    
    client = TaskQueueClient()
    health = client.health_check()
    
    print_success("Health check succeeded")
    print(f"  Status: {health['status']}")
    print(f"  Redis connected: {health['redis_connected']}")
    print(f"  Queue size: {health['queue_size']}")


def test_submit_single_task():
    """Test: Submit a single task."""
    print_section("Test 2: Submit Single Task")
    
    client = TaskQueueClient()
    
    # Submit email task
    task_id = client.submit_task(
        task_type="send_email",
        data={
            "recipient": "user@example.com",
            "subject": "Welcome",
            "body": "Hello!"
        },
        priority=1,
        max_retries=2
    )
    
    print_success(f"Task submitted: {task_id}")
    
    # Check status
    status = client.get_task_status(task_id)
    print_success(f"Task status: {status}")
    
    return task_id


def test_batch_submission():
    """Test: Batch task submission."""
    print_section("Test 3: Batch Task Submission")
    
    client = TaskQueueClient()
    
    # Create batch
    tasks = [
        {
            "task_type": "send_email",
            "data": {"recipient": f"user{i}@example.com"},
            "priority": i % 3
        }
        for i in range(5)
    ]
    
    task_ids = client.submit_batch(tasks)
    
    print_success(f"Batch submitted: {len(task_ids)} tasks")
    for i, task_id in enumerate(task_ids, 1):
        print(f"  {i}. {task_id[:8]}...")
    
    return task_ids


def test_queue_monitoring():
    """Test: Queue monitoring."""
    print_section("Test 4: Queue Monitoring")
    
    client = TaskQueueClient()
    
    # Get queue status
    status = client.get_queue_status()
    
    print_success("Queue status retrieved")
    print(f"  Pending tasks: {status['queue_size']}")
    print(f"  Processing: {status['processing_count']}")
    print(f"  Completed: {status['completed_count']}")
    print(f"  Dead-lettered: {status['dlq_size']}")
    
    # Get pending
    pending = client.get_pending_tasks(limit=10)
    print_info(f"{pending['message']}")


def test_dlq_operations():
    """Test: Dead-letter queue operations."""
    print_section("Test 5: Dead-Letter Queue")
    
    client = TaskQueueClient()
    
    # Get DLQ tasks
    dlq_response = client.get_dlq_tasks(limit=10)
    
    count = dlq_response.get("count", 0)
    print_success(f"DLQ tasks retrieved: {count} tasks")
    
    if count > 0:
        tasks = dlq_response.get("tasks", [])
        for task in tasks[:3]:
            print(f"  - {task['task_id'][:8]}... ({task['task_type']})")
            print(f"    Reason: {task['reason'][:50]}...")
    else:
        print_info("No dead-lettered tasks - this is good!")


def test_task_details():
    """Test: Get detailed task information."""
    print_section("Test 6: Task Details")
    
    client = TaskQueueClient()
    
    # Submit a task first
    task_id = client.submit_task(
        task_type="process_data",
        data={"file": "data.csv", "format": "csv"},
        timeout=60
    )
    
    print_success(f"Task submitted: {task_id}")
    
    # Get details
    task = client.get_task(task_id)
    
    print_success("Task details retrieved")
    print(f"  ID: {task['id'][:8]}...")
    print(f"  Type: {task['type']}")
    print(f"  Status: {task['status']}")
    print(f"  Priority: {task['priority']}")
    print(f"  Created: {task['created_at']}")


def run_comprehensive_test():
    """Run comprehensive API test suite."""
    print("\n" + "="*70)
    print("  Task Queue Control Plane - API Quick Start Test")
    print("="*70)
    
    try:
        # Check API availability
        if not wait_for_api():
            return False
        
        # Run tests
        test_health_check()
        test_submit_single_task()
        test_batch_submission()
        test_queue_monitoring()
        test_dlq_operations()
        test_task_details()
        
        # Summary
        print_section("Test Summary")
        print_success("All tests completed!")
        print("\n📚 Next Steps:")
        print("  1. View API docs: http://localhost:8000/docs")
        print("  2. Try API client: python api_client.py")
        print("  3. Read docs: API_DOCUMENTATION.md")
        print("  4. Check responses in Swagger UI\n")
        
        return True
        
    except KeyboardInterrupt:
        print_warning("Tests interrupted by user")
        return False
    except Exception as e:
        print_error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def show_api_endpoints():
    """Show available API endpoints."""
    print_section("Available API Endpoints")
    
    endpoints = [
        ("GET", "/", "API info and available endpoints"),
        ("GET", "/health", "Health check"),
        ("", "", ""),
        ("GET", "/queue/status", "Queue status and statistics"),
        ("GET", "/queue/pending", "Pending tasks"),
        ("GET", "/queue/dlq", "Dead-lettered tasks"),
        ("", "", ""),
        ("POST", "/tasks/submit", "Submit a single task"),
        ("POST", "/tasks/batch-submit", "Submit multiple tasks"),
        ("GET", "/tasks/{id}", "Get task details"),
        ("GET", "/tasks/{id}/status", "Get task status only"),
        ("POST", "/tasks/{id}/retry", "Retry a failed task"),
        ("POST", "/tasks/{id}/clear-dlq", "Remove from DLQ"),
    ]
    
    for method, path, description in endpoints:
        if method:
            print(f"  {method:6} {path:30} {description}")
        else:
            print()
    
    print("\n📖 Full documentation:")
    print("  - Interactive Docs: http://localhost:8000/docs")
    print("  - ReDoc: http://localhost:8000/redoc")
    print("  - OpenAPI: http://localhost:8000/openapi.json")


def show_usage_examples():
    """Show usage examples."""
    print_section("Usage Examples")
    
    print("Python Client:")
    print("""
from api_client import TaskQueueClient

client = TaskQueueClient()

# Submit task
task_id = client.submit_task(
    task_type="send_email",
    data={"recipient": "user@example.com"}
)

# Check status
status = client.get_task_status(task_id)
print(f"Status: {status}")

# Batch submission
tasks = [
    {"task_type": "email", "data": {"to": f"user{i}@example.com"}}
    for i in range(10)
]
task_ids = client.submit_batch(tasks)

# Get queue stats
stats = client.get_queue_status()
print(f"Queue: {stats['queue_size']} pending, {stats['completed_count']} done")

# Retry failed task
dlq = client.get_dlq_tasks(limit=1)
if dlq['tasks']:
    new_id = client.retry_task(dlq['tasks'][0]['task_id'])
    print(f"Retried as: {new_id}")
""")
    
    print("\ncURL Examples:")
    print("""
# Health check
curl http://localhost:8000/health

# Submit task
curl -X POST http://localhost:8000/tasks/submit \\
  -H "Content-Type: application/json" \\
  -d '{"task_type":"email","data":{"to":"user@example.com"}}'

# Get status
curl http://localhost:8000/tasks/{task_id}/status

# Queue status
curl http://localhost:8000/queue/status

# Get DLQ
curl http://localhost:8000/queue/dlq?limit=10

# Retry
curl -X POST http://localhost:8000/tasks/{task_id}/retry
""")


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "help":
            show_api_endpoints()
            show_usage_examples()
            return True
        elif command == "endpoints":
            show_api_endpoints()
            return True
        elif command == "examples":
            show_usage_examples()
            return True
        elif command == "test":
            return run_comprehensive_test()
        else:
            print(f"Unknown command: {command}")
            print("Usage: python api_quickstart.py [test|endpoints|examples|help]")
            return False
    else:
        # Run comprehensive test by default
        return run_comprehensive_test()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
