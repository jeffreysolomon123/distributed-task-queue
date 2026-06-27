import threading 
import time
import random
import queue
from core.job import Job


class Worker(threading.Thread):
    def __init__(self, worker_id: int, job_queue, result_callback):
        super().__init__(daemon=True)
        self.worker_id  = worker_id
        self.job_queue = job_queue
        self.result_callback = result_callback
        self.jobs_processed = 0
        self.is_running = True

    def run(self):
        print(f"[Worker {self.worker_id}] Started")
        while self.is_running:
            try:
                job = self.job_queue.get(timeout=1)
                self._process(job)
                self.job_queue.task_done()  # always called after get()
            except queue.Empty:
                continue  # only catch empty queue, not everything

    def _process(self, job: Job) : 
        print(f"[Worker {self.worker_id}] Processing '{job.task_name}' (priority {job.priority})")
        start = time.time()

        time.sleep(random.uniform(0.01,0.05)) ## actual work being done

        # simulating handling failure rate of 20%

        if random.random() < 0.3 : 
            self._handle_failure(job)
        else:
            latency = (time.time() - start) * 1000
            self.jobs_processed+=1
            self.result_callback("done", job, latency)

    def _handle_failure(self, job: Job):
        job.retries += 1
        if job.retries <= job.max_retries:
            print(f"[Worker {self.worker_id}] FAILED '{job.task_name}' — retry {job.retries}/{job.max_retries}")
            time.sleep(0.05)  # brief backoff before retry
            self.job_queue.put(job)
        else:
            print(f"[Worker {self.worker_id}] DEAD '{job.task_name}' — max retries exceeded")
            self.result_callback("dead", job, 0)
    def stop(self):
        self.is_running = False



        
                