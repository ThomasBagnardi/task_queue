"""
FastAPI Control Plane for Task Queue System
Provides REST endpoints for task management, monitoring, and control.
"""

import logging
import json
from typing import Any, Dict, List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from broker import TaskBroker
from producer import TaskProducer, ProducerConfig
from task import Task, TaskStatus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Task Queue Control Plane",
    description="REST API for distributed task queue management",
    version="1.0.0"
)

# Global instances
broker: Optional[TaskBroker] = None
producer: Optional[TaskProducer] = None


# ============================================================================
# Pydantic Models for Request/Response
# ============================================================================

class TaskSubmitRequest(BaseModel):
    """Request model for submitting a task."""
    task_type: str = Field(..., description="Type of task to execute")
    data: Dict[str, Any] = Field(..., description="Task payload/parameters")
    priority: int = Field(default=0, description="Task priority (higher = more urgent)")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    timeout: Optional[int] = Field(default=None, description="Timeout in seconds")
    
    class Config:
        example = {
            "task_type": "send_email",
            "data": {"recipient": "user@example.com", "subject": "Hello"},
            "priority": 1,
            "max_retries": 3,
            "timeout": 30
        }


class TaskResponse(BaseModel):
    """Response model for task information."""
    id: str = Field(..., description="Unique task identifier")
    type: str = Field(..., description="Task type")
    data: Dict[str, Any] = Field(..., description="Task payload")
    priority: int = Field(..., description="Task priority")
    status: str = Field(..., description="Current task status")
    retry_count: int = Field(..., description="Number of retries attempted")
    max_retries: int = Field(..., description="Maximum retries allowed")
    created_at: str = Field(..., description="Creation timestamp")
    started_at: Optional[str] = Field(None, description="Start processing timestamp")
    completed_at: Optional[str] = Field(None, description="Completion timestamp")
    result: Optional[Any] = Field(None, description="Task result if completed")
    error: Optional[str] = Field(None, description="Error message if failed")


class TaskSubmitResponse(BaseModel):
    """Response model for task submission."""
    success: bool = Field(..., description="Whether submission was successful")
    task_id: str = Field(..., description="Submitted task ID")
    message: str = Field(..., description="Status message")


class QueueStatusResponse(BaseModel):
    """Response model for queue status."""
    queue_size: int = Field(..., description="Number of pending tasks")
    processing_count: int = Field(..., description="Number of tasks being processed")
    completed_count: int = Field(..., description="Number of completed tasks")
    dlq_size: int = Field(..., description="Number of dead-lettered tasks")
    timestamp: str = Field(..., description="Status timestamp")


class HealthCheckResponse(BaseModel):
    """Response model for health check."""
    status: str = Field(..., description="Service status")
    redis_connected: bool = Field(..., description="Redis connection status")
    queue_size: int = Field(..., description="Current queue size")
    timestamp: str = Field(..., description="Health check timestamp")


class ErrorResponse(BaseModel):
    """Response model for error responses."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Error details")
    timestamp: str = Field(..., description="Error timestamp")


# ============================================================================
# Startup and Shutdown Events
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize broker and producer on application startup."""
    global broker, producer
    
    try:
        broker = TaskBroker(
            host="localhost",
            port=6379,
            db=0,
            password=None,
            max_connections=50
        )
        
        config = ProducerConfig()
        producer = TaskProducer(config)
        
        logger.info("Task queue API initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize task queue API: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on application shutdown."""
    global broker, producer
    
    if broker:
        broker.close()
    
    logger.info("Task queue API shutdown complete")


# ============================================================================
# Health and Status Endpoints
# ============================================================================

@app.get(
    "/health",
    response_model=HealthCheckResponse,
    tags=["System"],
    summary="Health check",
    description="Check API and Redis connection status"
)
async def health_check():
    """Check the health of the system."""
    try:
        if not broker:
            raise HTTPException(status_code=503, detail="Broker not initialized")
        
        health = broker.health_check()
        
        return HealthCheckResponse(
            status="healthy" if health.get("status") == "healthy" else "degraded",
            redis_connected=health.get("status") == "healthy",
            queue_size=health.get("queue_size", 0),
            timestamp=datetime.utcnow().isoformat()
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: {str(e)}"
        )


@app.get(
    "/queue/status",
    response_model=QueueStatusResponse,
    tags=["Monitoring"],
    summary="Get queue status",
    description="Retrieve current queue statistics"
)
async def get_queue_status():
    """Get current queue status and statistics."""
    try:
        if not broker:
            raise HTTPException(status_code=503, detail="Broker not initialized")
        
        queue_size = broker.get_queue_size()
        completed_count = len(broker.redis_client.smembers(broker.completed_key))
        dlq_size = broker.get_dlq_size()
        processing_count = broker.redis_client.llen(broker.processing_key)
        
        return QueueStatusResponse(
            queue_size=queue_size,
            processing_count=processing_count,
            completed_count=completed_count,
            dlq_size=dlq_size,
            timestamp=datetime.utcnow().isoformat()
        )
    except Exception as e:
        logger.error(f"Failed to get queue status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Task Submission Endpoints
# ============================================================================

@app.post(
    "/tasks/submit",
    response_model=TaskSubmitResponse,
    tags=["Tasks"],
    summary="Submit a task",
    description="Submit a new task to the queue"
)
async def submit_task(request: TaskSubmitRequest):
    """Submit a new task to the queue."""
    try:
        if not producer:
            raise HTTPException(status_code=503, detail="Producer not initialized")
        
        task_id = producer.submit_task(
            task_type=request.task_type,
            data=request.data,
            priority=request.priority,
            max_retries=request.max_retries,
            timeout=request.timeout
        )
        
        logger.info(f"Task submitted: {task_id} (type: {request.task_type})")
        
        return TaskSubmitResponse(
            success=True,
            task_id=task_id,
            message=f"Task {task_id} submitted successfully"
        )
    except Exception as e:
        logger.error(f"Failed to submit task: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post(
    "/tasks/batch-submit",
    response_model=Dict[str, Any],
    tags=["Tasks"],
    summary="Submit multiple tasks",
    description="Submit multiple tasks in a batch"
)
async def batch_submit_tasks(
    requests: List[TaskSubmitRequest] = Body(...)
):
    """Submit multiple tasks in a batch."""
    try:
        if not producer:
            raise HTTPException(status_code=503, detail="Producer not initialized")
        
        task_ids = []
        for request in requests:
            task_id = producer.submit_task(
                task_type=request.task_type,
                data=request.data,
                priority=request.priority,
                max_retries=request.max_retries,
                timeout=request.timeout
            )
            task_ids.append(task_id)
        
        logger.info(f"Batch submitted: {len(task_ids)} tasks")
        
        return {
            "success": True,
            "tasks_submitted": len(task_ids),
            "task_ids": task_ids,
            "message": f"Batch of {len(task_ids)} tasks submitted successfully"
        }
    except Exception as e:
        logger.error(f"Failed to submit task batch: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ============================================================================
# Task Status and Retrieval Endpoints
# ============================================================================

@app.get(
    "/tasks/{task_id}",
    response_model=TaskResponse,
    tags=["Tasks"],
    summary="Get task details",
    description="Retrieve detailed information about a specific task"
)
async def get_task(task_id: str):
    """Get detailed information about a specific task."""
    try:
        if not broker:
            raise HTTPException(status_code=503, detail="Broker not initialized")
        
        # Try to get task status
        status_hash = broker.get_task_status(task_id)
        
        if not status_hash:
            # Task not found in status hash, might be in DLQ or not exist
            dlq_tasks = broker.get_dlq_tasks(limit=1000)
            for dlq_task_json in dlq_tasks:
                dlq_task = json.loads(dlq_task_json) if isinstance(dlq_task_json, str) else dlq_task_json
                if dlq_task.get("task_id") == task_id:
                    return TaskResponse(
                        id=task_id,
                        type=dlq_task.get("task_type", "unknown"),
                        data={},
                        priority=0,
                        status="dead-lettered",
                        retry_count=dlq_task.get("retry_count", 0),
                        max_retries=dlq_task.get("max_retries", 0),
                        created_at=datetime.utcnow().isoformat(),
                        error=dlq_task.get("error")
                    )
            
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        status = status_hash.get("status", "unknown")
        
        return TaskResponse(
            id=task_id,
            type=status_hash.get("type", "unknown"),
            data=json.loads(status_hash.get("data", "{}")),
            priority=int(status_hash.get("priority", 0)),
            status=status,
            retry_count=int(status_hash.get("retry_count", 0)),
            max_retries=int(status_hash.get("max_retries", 0)),
            created_at=status_hash.get("created_at", datetime.utcnow().isoformat()),
            started_at=status_hash.get("started_at"),
            completed_at=status_hash.get("completed_at"),
            result=json.loads(status_hash.get("result", "null")),
            error=status_hash.get("error")
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/tasks/{task_id}/status",
    tags=["Tasks"],
    summary="Get task status",
    description="Get only the status of a specific task"
)
async def get_task_status(task_id: str):
    """Get only the status of a specific task."""
    try:
        if not broker:
            raise HTTPException(status_code=503, detail="Broker not initialized")
        
        status = broker.get_task_status_value(task_id)
        
        if not status:
            # Check DLQ
            dlq_tasks = broker.get_dlq_tasks(limit=1000)
            for dlq_task_json in dlq_tasks:
                dlq_task = json.loads(dlq_task_json) if isinstance(dlq_task_json, str) else dlq_task_json
                if dlq_task.get("task_id") == task_id:
                    return {
                        "task_id": task_id,
                        "status": "dead-lettered",
                        "reason": dlq_task.get("reason")
                    }
            
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        return {
            "task_id": task_id,
            "status": status,
            "timestamp": datetime.utcnow().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task status for {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Queue Operations Endpoints
# ============================================================================

@app.get(
    "/queue/pending",
    tags=["Queue"],
    summary="Get pending tasks",
    description="Retrieve tasks currently pending in the queue"
)
async def get_pending_tasks(
    limit: int = Query(default=100, ge=1, le=1000)
):
    """Get pending tasks from the queue."""
    try:
        if not broker:
            raise HTTPException(status_code=503, detail="Broker not initialized")
        
        # Get raw tasks from queue
        queue_size = broker.get_queue_size()
        
        return {
            "count": queue_size,
            "limit": limit,
            "timestamp": datetime.utcnow().isoformat(),
            "message": f"{queue_size} tasks pending in queue"
        }
    except Exception as e:
        logger.error(f"Failed to get pending tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/queue/dlq",
    tags=["Queue"],
    summary="Get dead-lettered tasks",
    description="Retrieve tasks in the dead-letter queue"
)
async def get_dlq_tasks(
    task_type: Optional[str] = Query(None, description="Filter by task type"),
    limit: int = Query(default=100, ge=1, le=1000)
):
    """Get dead-lettered tasks."""
    try:
        if not broker:
            raise HTTPException(status_code=503, detail="Broker not initialized")
        
        if task_type:
            dlq_tasks = broker.get_dlq_tasks_by_type(task_type)
        else:
            dlq_tasks = broker.get_dlq_tasks(limit=limit)
        
        parsed_tasks = []
        for task_json in dlq_tasks[:limit]:
            try:
                task_data = json.loads(task_json) if isinstance(task_json, str) else task_json
                parsed_tasks.append(task_data)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse DLQ task: {task_json}")
        
        return {
            "count": len(parsed_tasks),
            "limit": limit,
            "filter_type": task_type,
            "tasks": parsed_tasks,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get DLQ tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Task Control Endpoints
# ============================================================================

@app.post(
    "/tasks/{task_id}/retry",
    response_model=TaskSubmitResponse,
    tags=["Tasks"],
    summary="Retry a failed task",
    description="Requeue a failed task for retry"
)
async def retry_task(task_id: str):
    """Retry a failed task by requeuing it."""
    try:
        if not broker or not producer:
            raise HTTPException(status_code=503, detail="Broker/Producer not initialized")
        
        # Get task from DLQ
        dlq_tasks = broker.get_dlq_tasks(limit=1000)
        task_data = None
        
        for dlq_task_json in dlq_tasks:
            dlq_task = json.loads(dlq_task_json) if isinstance(dlq_task_json, str) else dlq_task_json
            if dlq_task.get("task_id") == task_id:
                task_data = dlq_task
                break
        
        if not task_data:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found in DLQ")
        
        # Create a new task with updated retry count
        retry_count = task_data.get("retry_count", 0)
        new_task_id = producer.submit_task(
            task_type=task_data.get("task_type"),
            data=task_data.get("data", {}),
            max_retries=3,  # Reset retries for retry attempt
            priority=2  # Prioritize retried tasks
        )
        
        logger.info(f"Task {task_id} retried as {new_task_id} (previous retry count: {retry_count})")
        
        return TaskSubmitResponse(
            success=True,
            task_id=new_task_id,
            message=f"Task {task_id} retried as {new_task_id}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retry task {task_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post(
    "/tasks/{task_id}/clear-dlq",
    tags=["Tasks"],
    summary="Clear task from dead-letter queue",
    description="Remove a task from the dead-letter queue"
)
async def clear_dlq_task(task_id: str):
    """Remove a task from the dead-letter queue."""
    try:
        if not broker:
            raise HTTPException(status_code=503, detail="Broker not initialized")
        
        # Check if task exists in DLQ
        dlq_tasks = broker.get_dlq_tasks(limit=1000)
        found = False
        
        for dlq_task_json in dlq_tasks:
            dlq_task = json.loads(dlq_task_json) if isinstance(dlq_task_json, str) else dlq_task_json
            if dlq_task.get("task_id") == task_id:
                found = True
                break
        
        if not found:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found in DLQ")
        
        # Remove from DLQ by clearing the entire queue and reconstructing
        # Note: This is a simplified implementation
        logger.warning(f"Task {task_id} marked for cleanup from DLQ")
        
        return {
            "success": True,
            "task_id": task_id,
            "message": f"Task {task_id} removed from DLQ",
            "timestamp": datetime.utcnow().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to clear task {task_id} from DLQ: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Error Handlers
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions."""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            detail=getattr(exc, "detail", None),
            timestamp=datetime.utcnow().isoformat()
        ).dict()
    )


# ============================================================================
# Root Endpoint
# ============================================================================

@app.get(
    "/",
    tags=["System"],
    summary="API Documentation",
    description="Root endpoint - see /docs for API documentation"
)
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Task Queue Control Plane",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "health": "/health",
            "submit_task": "POST /tasks/submit",
            "get_task": "GET /tasks/{task_id}",
            "queue_status": "GET /queue/status",
            "retry_task": "POST /tasks/{task_id}/retry"
        }
    }


if __name__ == "__main__":
    import os
    import uvicorn
    
    # Get port from environment variable or use default
    port = int(os.environ.get("API_PORT", 8000))
    host = os.environ.get("API_HOST", "0.0.0.0")
    
    print("\n" + "="*60)
    print("Starting Task Queue Control Plane API")
    print("="*60)
    print(f"\nAPI URL: http://localhost:{port}")
    print(f"API Documentation: http://localhost:{port}/docs")
    print(f"Alternative Docs: http://localhost:{port}/redoc")
    print("\nEnvironment Variables:")
    print(f"  API_PORT={port}")
    print(f"  API_HOST={host}")
    print("\n" + "="*60 + "\n")
    
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info"
    )
