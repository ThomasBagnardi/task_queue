"""REST API module for Task Queue System

This module provides HTTP/REST endpoints for task management.
"""

from .api import app, TaskQueueAPI
from .api_client import TaskQueueClient

__all__ = ['app', 'TaskQueueAPI', 'TaskQueueClient']
