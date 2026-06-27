import time
from queue_manager import QueueManager

if __name__ == "__main__":
    qm = QueueManager(num_workers=10)

    start = time.time()

    # Submit 500 jobs with mixed priorities
    for i in range(500):
        priority = (i % 3) + 1  # cycles through 1, 2, 3
        qm.submit(f"task_{i}", priority=priority, payload={"index": i})

    qm.wait()
    elapsed = time.time() - start

    qm.summary()
    print(f"Total time: {elapsed:.2f}s")
    print(f"Throughput: {500 / elapsed:.0f} jobs/sec")