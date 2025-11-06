import json
import threading
from datetime import datetime

_METRICS_LOCK = threading.Lock()
METRICS_FILE = "adaptive_stats.json"

def log_adaptive_stats(peers: int, msg_rate: float, avg_latency: float, fanout: int, interval: float):
    """
    Thread-safe logging of adaptive gossip statistics to a JSON lines file.
    Each record is one JSON object per line.
    """
    record = {
        "time": datetime.now().isoformat(timespec="seconds"),
        "peers": peers,
        "msg_rate": round(msg_rate, 3),
        "avg_latency": round(avg_latency, 4),
        "fanout": fanout,
        "interval": round(interval, 3),
    }
    try:
        with _METRICS_LOCK:
            with open(METRICS_FILE, "a") as f:
                json.dump(record, f)
                f.write("\n")
    except Exception as e:
        print(f"[WARN] Metrics logging failed: {e}")
