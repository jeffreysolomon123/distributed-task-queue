import queue
import threading
import time
from core.job import Job
from core.worker import Worker

class QueueManager:
    def __init__(self, num_workers: int = 3):
        self.job_queue = queue.PriorityQueue()
        self.workers = []
        self.stats = {"done":0,"dead":0,"total_latency":0.0}
        self.lock = threading.Lock()
        self._start_workers(num_workers)

    def _start_workers(self, n):
        for i in range(n):
            w= Worker(i+1, self.job_queue, self._on_result)
            w.start()
            self.workers.append(w)


    def submit(self, task_name: str, priority: int = 2, payload: dict = {}):
        job = Job(priority=priority, task_name=task_name, payload=payload)
        self.job_queue.put(job)
        return job.job_id
    
    
    def _on_result(self, status: str, job:Job, latency:float):
        with self.lock:
            self.stats[status]+=1
            self.stats["total_latency"] +=latency

    def wait(self):
        self.job_queue.join()  # blocks until all jobs are done

    def summary(self):
        done = self.stats["done"]
        avg = self.stats["total_latency"] / done if done else 0
        print(f"\n=== Results ===")
        print(f"Completed: {done}")
        print(f"Dead (failed all retries): {self.stats['dead']}")
        print(f"Avg latency: {avg:.1f}ms")