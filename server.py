import asyncio
import json
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from queue_manager import QueueManager


qm = QueueManager(num_workers=10)
connected_clients: set[WebSocket] = set()
# Mutable dict so the closure in broadcast_loop can update it without globals
_throughput_state = {"prev_done": 0, "prev_time": time.time()}


class SubmitRequest(BaseModel):
    task_name: str
    priority: int = 2
    payload: dict = Field(default_factory=dict)


def collect_metrics() -> dict:
    with qm.lock:
        done = qm.stats["done"]
        dead = qm.stats["dead"]
        total_latency = qm.stats["total_latency"]

    now = time.time()
    elapsed = now - _throughput_state["prev_time"]
    throughput = (done - _throughput_state["prev_done"]) / elapsed if elapsed > 0 else 0.0
    _throughput_state["prev_done"] = done
    _throughput_state["prev_time"] = now

    avg_latency = total_latency / done if done > 0 else 0.0

    return {
        "jobs_completed": done,
        "jobs_dead": dead,
        "avg_latency_ms": round(avg_latency, 2),
        "throughput_per_sec": round(throughput, 2),
        "queue_size": qm.job_queue.qsize(),
        "active_workers": sum(1 for w in qm.workers if w.is_alive()),
    }


async def broadcast_loop():
    tick = 0
    while True:
        await asyncio.sleep(1)
        tick += 1
        print(f"[broadcast] tick={tick} clients={len(connected_clients)}", flush=True)
        if not connected_clients:
            continue
        try:
            payload = json.dumps(collect_metrics())
        except Exception as e:
            print(f"[broadcast] collect_metrics failed: {e}", flush=True)
            continue
        dead: set[WebSocket] = set()
        for ws in list(connected_clients):
            try:
                await ws.send_text(payload)
                print(f"[broadcast] sent tick={tick} to ws={id(ws)}", flush=True)
            except Exception as e:
                print(f"[broadcast] send failed ws={id(ws)}: {type(e).__name__}: {e}", flush=True)
                dead.add(ws)
        connected_clients -= dead


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(broadcast_loop())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/submit")
async def submit_job(req: SubmitRequest):
    job_id = qm.submit(req.task_name, priority=req.priority, payload=req.payload)
    return {"job_id": job_id, "status": "queued"}


@app.websocket("/ws/metrics")
async def metrics_ws(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    print(f"[ws] ADDED ws={id(websocket)} total={len(connected_clients)}", flush=True)
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive(), timeout=60)
                if data.get("type") == "websocket.disconnect":
                    break
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"[ws] receive error: {type(e).__name__}: {e}", flush=True)
                break
    finally:
        connected_clients.discard(websocket)
        print(f"[ws] REMOVED ws={id(websocket)} total={len(connected_clients)}", flush=True)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
