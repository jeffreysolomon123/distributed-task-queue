import uuid;
import time;
from dataclasses import dataclass, field;

@dataclass(order=True)
class Job:
    priority: int
    created_at: float = field(default_factory=time.time, compare=False)
    job_id: str = field(default_factory=lambda:str(uuid.uuid4())[:8], compare=False)
    task_name: str = field(default="", compare=False)
    payload: dict = field(default_factory=dict, compare=False)
    retries: int = field(default=0, compare=False)
    max_retries: int = field(default=3, compare=False)
    