"""
Analyze Latency Logs
"""

import json
import glob
import statistics
from typing import List, Dict


def read_latency_logs() -> Dict[str, List[Dict]]:
    """Read all latency log files"""
    log_files = glob.glob("latency_log_*.json")

    if not log_files:
        print("[ERROR] No latency log files found!")
        print("\nMake sure you:")
        print("  1. Started nodes with DAPSS_LATENCY_LOG=1")
        print("  2. Ran latency_test.py")
        print("  3. Run this script from the same directory")
        return {}

    logs_by_node = {}

    for log_file in log_files:
        # Extract port from filename (e.g., latency_log_5002.json -> 5002)
        port = log_file.replace("latency_log_", "").replace(".json", "")

        entries = []
        with open(log_file, "r") as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    entries.append(entry)
                except Exception:
                    pass

        if entries:
            logs_by_node[f"Node {port}"] = entries

    return logs_by_node


def calculate_statistics(latencies: List[float]) -> Dict:
    """Calculate latency statistics"""
    if not latencies:
        return {"error": "No data"}

    sorted_latencies = sorted(latencies)

    return {
        "count": len(latencies),
        "mean_ms": statistics.mean(latencies),
        "median_ms": statistics.median(latencies),
        "min_ms": min(latencies),
        "max_ms": max(latencies),
        "stdev_ms": statistics.stdev(latencies) if len(latencies) > 1 else 0,
        "p50_ms": sorted_latencies[int(len(sorted_latencies) * 0.50)],
        "p95_ms": sorted_latencies[int(len(sorted_latencies) * 0.95)],
        "p99_ms": sorted_latencies[int(len(sorted_latencies) * 0.99)]
    }


def main():
    print("="*60)
    print("DAPSS Latency Analysis")
    print("="*60)

    logs = read_latency_logs()

    if not logs:
        return

    print(f"\nFound {len(logs)} node(s) with latency logs\n")

    all_latencies = []

    # Per-node statistics
    for node_name, entries in logs.items():
        latencies = [e["latency_ms"] for e in entries]
        all_latencies.extend(latencies)

        stats = calculate_statistics(latencies)

        print(f"{node_name}:")
        print(f"  Messages Received: {stats['count']}")
        print(f"  Mean Latency: {stats['mean_ms']:.2f} ms")
        print(f"  Median Latency: {stats['median_ms']:.2f} ms")
        print(f"  Min Latency: {stats['min_ms']:.2f} ms")
        print(f"  Max Latency: {stats['max_ms']:.2f} ms")
        print(f"  Std Dev: {stats['stdev_ms']:.2f} ms")
        print(f"  P50 (median): {stats['p50_ms']:.2f} ms")
        print(f"  P95: {stats['p95_ms']:.2f} ms")
        print(f"  P99: {stats['p99_ms']:.2f} ms")
        print()

    # Overall statistics
    if len(logs) > 1:
        overall_stats = calculate_statistics(all_latencies)

        print("="*60)
        print("OVERALL CLUSTER STATISTICS")
        print("="*60)
        print(f"Total Messages: {overall_stats['count']}")
        print(f"Mean Latency: {overall_stats['mean_ms']:.2f} ms")
        print(f"Median Latency: {overall_stats['median_ms']:.2f} ms")
        print(f"Min Latency: {overall_stats['min_ms']:.2f} ms")
        print(f"Max Latency: {overall_stats['max_ms']:.2f} ms")
        print(f"P95: {overall_stats['p95_ms']:.2f} ms")
        print(f"P99: {overall_stats['p99_ms']:.2f} ms")
        print()

    # Cleanup option
    print("="*60)
    print("\nTo clean up log files, run:")
    print("  del latency_log_*.json  (Windows)")
    print("  rm latency_log_*.json   (Linux)")
    print()


if __name__ == "__main__":
    main()
