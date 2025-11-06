# utils/plot_metrics.py
import json, matplotlib.pyplot as plt

times, fanout, interval, rate = [], [], [], []
with open(f"C:\\Users\\naing\\DAPSS\\adaptive_stats.json") as f:
    for line in f:
        r = json.loads(line)
        times.append(r["time"])
        fanout.append(r["fanout"])
        interval.append(r["interval"])
        rate.append(r["msg_rate"])

plt.figure(figsize=(8,4))
plt.subplot(3,1,1); plt.plot(fanout); plt.ylabel("Fanout")
plt.subplot(3,1,2); plt.plot(interval); plt.ylabel("Interval (s)")
plt.subplot(3,1,3); plt.plot(rate); plt.ylabel("Msg rate"); plt.xlabel("Adapt rounds")
plt.tight_layout(); plt.show()
