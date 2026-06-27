# Distributed Priority Task Queue

A concurrent, fault-tolerant priority task queue built in Python using thread pools and synchronization primitives — inspired by systems like Google Cloud Tasks, Celery, and Redis Queue.

Built to demonstrate real-world distributed systems concepts: concurrency, thread-safe shared state, priority scheduling, and fault tolerance with automatic retry.

---

## How It Works

Jobs are submitted with a priority level (1 = high, 3 = low) into a thread-safe `PriorityQueue`. A configurable pool of worker threads pulls jobs concurrently — higher priority jobs are always picked up first. Failed jobs are automatically retried up to 3 times before being marked dead.

```
Producer
   │
   ▼
PriorityQueue (thread-safe)
   ├── 🔴 Priority 1 jobs  ← picked up first
   ├── 🟡 Priority 2 jobs
   └── 🟢 Priority 3 jobs
         │
         ▼
   ┌─────────────────────────┐
   │  Worker Pool (N threads) │
   │  Worker 1  Worker 2  .. │
   └─────────────────────────┘
         │
    ┌────┴────┐
    ▼         ▼
  Done ✓    Failed → retry → Dead ✗ (after 3 retries)
```

---

## Key Technical Decisions

**Why `queue.PriorityQueue` instead of a raw list?**
`PriorityQueue` uses an internal `threading.Lock` — only one worker can dequeue at a time, eliminating race conditions when multiple threads access the queue simultaneously. A raw list with manual sorting would require explicit locking and is error-prone.

**Why `get(timeout=1)` instead of a busy loop?**
Workers block on `get()` and are woken by the OS only when a job is available. A `while True` busy loop would consume 100% CPU on idle workers. The 1-second timeout allows clean shutdown.

**Why call `task_done()` even on failed jobs?**
`queue.join()` tracks the delta between `put()` and `task_done()` calls. If a failed job skips `task_done()`, the counter never reaches zero and the program hangs forever. Re-queued retry jobs get their own new `put()` + `task_done()` cycle.

**Why a `threading.Lock()` on the stats counter?**
Two workers completing jobs simultaneously and both doing `stats["done"] += 1` is a read-modify-write operation — not atomic in Python. Without a lock, increments get lost. The lock ensures every completion is counted exactly once.

---

## Benchmark Results

Tested with 500 jobs, 30% simulated failure rate, automatic retry up to 3x.

### Scaling with worker count

| Workers | Throughput (jobs/sec) | Avg Latency (ms) |
|--------:|----------------------:|-----------------:|
|       1 |                    18 |            ~30ms |
|       2 |                    35 |            ~30ms |
|       5 |                    89 |            ~30ms |
|      10 |                   153 |            ~30ms |
|      20 |                   187 |            ~30ms |

Throughput scales linearly up to ~10 workers, then flattens — at that point the bottleneck shifts from worker count to queue locking overhead, not task duration. This is the expected behavior for I/O-bound workloads.

### Single run (10 workers, 500 jobs)
```
Completed:  499
Dead:         1  (hit max retries)
Avg latency: 30.4ms
Throughput:  153 jobs/sec
Total time:  3.28s
```

---

## Project Structure

```
distributed-task-queue/
├── core/
│   ├── __init__.py
│   ├── job.py          # Job dataclass with priority ordering
│   └── worker.py       # Worker thread: process, fail, retry
├── queue_manager.py    # Orchestrator: thread pool, stats, locks
└── main.py             # Benchmark runner
```

---

## Getting Started

**Requirements:** Python 3.9+, no external dependencies.

```bash
git clone https://github.com/jeffreysolomon123/distributed-task-queue
cd distributed-task-queue
python main.py
```

**Run the scaling benchmark:**
```python
# In main.py
for num_workers in [1, 2, 5, 10, 20]:
    qm = QueueManager(num_workers=num_workers)
    # submit 500 jobs...
```

---

## Concepts Demonstrated

| Concept | Where |
|---|---|
| Thread pool | `QueueManager._start_workers()` |
| Thread-safe shared queue | `queue.PriorityQueue` in `queue_manager.py` |
| Mutex / locking | `threading.Lock()` on stats counter |
| Priority scheduling | `Job.priority` field + `PriorityQueue` ordering |
| Fault tolerance | `Worker._handle_failure()` with retry counter |
| Blocking vs busy-wait | `job_queue.get(timeout=1)` in `Worker.run()` |
| Graceful shutdown | `daemon=True` workers + `queue.join()` |

---

## What I'd Add With More Time

- **Exponential backoff** on retries instead of fixed delay
- **Persistent queue** using SQLite so jobs survive process restarts
- **Dead letter queue** — separate storage for permanently failed jobs
- **Job result storage** — retrieve output after completion
- **Distributed mode** — multiple processes across machines using Redis as the queue backend