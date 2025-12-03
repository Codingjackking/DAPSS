"""
DAPSS Comprehensive Metrics Visualization
Generates detailed performance charts and analysis from metrics data
"""

import json
import glob
import os
import sys
import argparse

from typing import List, Dict, Tuple
from datetime import datetime

from matplotlib.lines import Line2D

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.gridspec import GridSpec
except ImportError:
    print("[ERROR] matplotlib is required for visualization")
    print("Install with: pip install matplotlib")
    sys.exit(1)


class MetricsVisualizer:
    """Comprehensive visualization for DAPSS metrics"""
    
    def __init__(self, metrics_dir: str = "metrics"):
        self.metrics_dir = metrics_dir
        self.data = {}
        
    def load_data(self):
        """Load all metrics files"""
        print(f"[INFO] Loading metrics from {self.metrics_dir}/...")
        
        # Load adaptive stats
        self.data['adaptive'] = self._load_json_files("adaptive_stats_*.json")
        
        # Load gossip metrics
        self.data['gossip'] = self._load_json_files("gossip_metrics_*.json")
        
        # Load consensus metrics
        self.data['consensus'] = self._load_json_files("consensus_metrics_*.json")
        
        # Load network health
        self.data['network'] = self._load_json_files("network_health_*.json")
        
        # Load summaries
        self.data['summaries'] = self._load_json_summary("summary_*.json")
        
        total_records = sum(
            sum(len(records) for records in category.values()) 
            for category in self.data.values() if isinstance(category, dict)
        )
        
        print(f"[INFO] Loaded {total_records} total records")
        
        if total_records == 0:
            print("[WARN] No metrics data found!")
            print(f"[WARN] Make sure nodes have run and generated metrics in {self.metrics_dir}/")
            return False
        
        return True
    
    def _load_json_files(self, pattern: str) -> Dict[str, List[Dict]]:
        results = {}
        files = glob.glob(os.path.join(self.metrics_dir, pattern))

        # print(f"[DEBUG] Pattern {pattern} -> {files}")  

        if not files:
            files = glob.glob(pattern)
            # print(f"[DEBUG] Fallback search -> {files}")

        for filepath in files:
            node_id = self._extract_node_id(filepath)
            results[node_id] = self._load_json(filepath)

        return results

    
    def _load_json_summary(self, pattern: str) -> Dict[str, Dict]:
        """Load JSON summary files"""
        results = {}
        files = glob.glob(os.path.join(self.metrics_dir, pattern))
        
        if not files:
            files = glob.glob(pattern)
        
        for filepath in files:
            node_id = self._extract_node_id(filepath)
            try:
                with open(filepath, 'r') as f:
                    results[node_id] = json.load(f)
            except Exception as e:
                print(f"[WARN] Failed to load {filepath}: {e}")
        
        return results
    
    def _load_json(self, filepath: str) -> List[Dict]:
        """Load JSON lines file"""
        records = []
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    try:
                        records.append(json.loads(line.strip()))
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            print(f"[WARN] Failed to load {filepath}: {e}")
        
        return records
    
    def _extract_node_id(self, filepath: str) -> str:
        """Extract node ID from filename, supporting both colon and underscore formats."""
        basename = os.path.basename(filepath)

        prefixes = [
            'adaptive_stats_',
            'gossip_metrics_',
            'consensus_metrics_',
            'network_health_',
            'summary_'
        ]

        for prefix in prefixes:
            if basename.startswith(prefix):
                node_id = basename[len(prefix):].replace(".json", "")
                # Convert underscores to colons if needed
                if "_" in node_id and node_id.count("_") == 2:
                    # Example: 127.0.0.1_5001  →  127.0.0.1:5001
                    parts = node_id.rsplit("_", 1)
                    node_id = f"{parts[0]}:{parts[1]}"
                return node_id

        return basename

    
    # ========================================================================
    # ADAPTIVE GOSSIP VISUALIZATION
    # ========================================================================
    
    def plot_adaptive_metrics(self, save_path: str = None):
        """Plot adaptive gossip tuning over time"""
        adaptive_data = self.data.get('adaptive', {})
        
        if not adaptive_data:
            print("[WARN] No adaptive metrics data found")
            return
        
        fig, axes = plt.subplots(4, 1, figsize=(12, 10))
        fig.suptitle('Adaptive Gossip Protocol Metrics', fontsize=16, fontweight='bold')
        
        for node_id, records in adaptive_data.items():
            if not records:
                continue
            
            # Extract time series data
            times = list(range(len(records)))
            fanouts = [r.get('fanout', 0) for r in records]
            intervals = [r.get('interval', 0) for r in records]
            msg_rates = [r.get('msg_rate', 0) for r in records]
            latencies = [r.get('avg_latency', 0) for r in records]
            
            label = f"Node {node_id}"
            
            # Fanout over time
            axes[0].plot(times, fanouts, marker='o', label=label, linewidth=2)
            axes[0].set_ylabel('Fanout', fontweight='bold')
            axes[0].set_title('Gossip Fanout Adaptation')
            axes[0].grid(True, alpha=0.3)
            axes[0].legend()
            
            # Interval over time
            axes[1].plot(times, intervals, marker='s', label=label, linewidth=2)
            axes[1].set_ylabel('Interval (s)', fontweight='bold')
            axes[1].set_title('Gossip Interval Adaptation')
            axes[1].grid(True, alpha=0.3)
            axes[1].legend()
            
            # Message rate over time
            axes[2].plot(times, msg_rates, marker='^', label=label, linewidth=2)
            axes[2].set_ylabel('Msg Rate', fontweight='bold')
            axes[2].set_title('Message Rate')
            axes[2].grid(True, alpha=0.3)
            axes[2].legend()
            
            # Average latency over time
            axes[3].plot(times, latencies, marker='d', label=label, linewidth=2)
            axes[3].set_ylabel('Latency (s)', fontweight='bold')
            axes[3].set_xlabel('Adaptation Round', fontweight='bold')
            axes[3].set_title('Average Message Latency')
            axes[3].grid(True, alpha=0.3)
            axes[3].legend()
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"[SAVED] {save_path}")
        else:
            plt.show()
    
    # ========================================================================
    # SUMMARY DASHBOARD
    # ========================================================================
    
    def plot_summary_dashboard(self, save_path: str = None):
        """Create comprehensive summary dashboard"""
        summaries = self.data.get('summaries', {})
        
        if not summaries:
            print("[WARN] No summary data found")
            return
        
        fig = plt.figure(figsize=(16, 12))
        gs = GridSpec(3, 3, figure=fig, hspace=0.3, wspace=0.3)
        fig.suptitle('DAPSS Cluster Performance Dashboard', fontsize=18, fontweight='bold')
        
        # Extract data for all nodes
        nodes = list(summaries.keys())
        
        # Gossip metrics
        gossip_sent = [summaries[n]['gossip']['sent'] for n in nodes]
        gossip_recv = [summaries[n]['gossip']['received'] for n in nodes]
        gossip_dup = [summaries[n]['gossip']['duplicates'] for n in nodes]
        gossip_rate = [summaries[n]['gossip']['rate_per_sec'] for n in nodes]
        
        # Consensus metrics
        votes_req = [summaries[n]['consensus']['votes_requested'] for n in nodes]
        votes_grant = [summaries[n]['consensus']['votes_granted'] for n in nodes]
        leader_elec = [summaries[n]['consensus']['leader_elections'] for n in nodes]
        
        # Network metrics
        conn_attempts = [summaries[n]['network']['connection_attempts'] for n in nodes]
        conn_success = [summaries[n]['network']['connection_success_rate'] for n in nodes]
        healthy_peers = [summaries[n]['network']['healthy_peers'] for n in nodes]
        
        # 1. Gossip Messages
        ax1 = fig.add_subplot(gs[0, 0])
        x = range(len(nodes))
        width = 0.35
        ax1.bar([i - width/2 for i in x], gossip_sent, width, label='Sent', color='#2ecc71')
        ax1.bar([i + width/2 for i in x], gossip_recv, width, label='Received', color='#3498db')
        ax1.set_xlabel('Node', fontweight='bold')
        ax1.set_ylabel('Messages', fontweight='bold')
        ax1.set_title('Gossip Messages')
        ax1.set_xticks(x)
        ax1.set_xticklabels(nodes)
        ax1.legend()
        ax1.grid(True, alpha=0.3, axis='y')
        
        # 2. Message Rate
        ax2 = fig.add_subplot(gs[0, 1])
        ax2.bar(nodes, gossip_rate, color='#e74c3c')
        ax2.set_xlabel('Node', fontweight='bold')
        ax2.set_ylabel('Rate (msg/sec)', fontweight='bold')
        ax2.set_title('Message Throughput')
        ax2.grid(True, alpha=0.3, axis='y')
        
        # 3. Duplicate Rate
        ax3 = fig.add_subplot(gs[0, 2])
        total_recv = [max(1, r) for r in gossip_recv]  # Avoid division by zero
        dup_rate = [100 * gossip_dup[i] / total_recv[i] for i in range(len(nodes))]
        ax3.bar(nodes, dup_rate, color='#f39c12')
        ax3.set_xlabel('Node', fontweight='bold')
        ax3.set_ylabel('Duplicate Rate (%)', fontweight='bold')
        ax3.set_title('Message Deduplication')
        ax3.grid(True, alpha=0.3, axis='y')
        
        # 4. Consensus Activity
        ax4 = fig.add_subplot(gs[1, 0])
        x = range(len(nodes))
        ax4.bar([i - width/2 for i in x], votes_req, width, label='Requested', color='#9b59b6')
        ax4.bar([i + width/2 for i in x], votes_grant, width, label='Granted', color='#8e44ad')
        ax4.set_xlabel('Node', fontweight='bold')
        ax4.set_ylabel('Votes', fontweight='bold')
        ax4.set_title('Consensus Voting')
        ax4.set_xticks(x)
        ax4.set_xticklabels(nodes)
        ax4.legend()
        ax4.grid(True, alpha=0.3, axis='y')
        
        # 5. Leader Elections
        ax5 = fig.add_subplot(gs[1, 1])
        ax5.bar(nodes, leader_elec, color='#e67e22')
        ax5.set_xlabel('Node', fontweight='bold')
        ax5.set_ylabel('Elections', fontweight='bold')
        ax5.set_title('Leader Elections')
        ax5.grid(True, alpha=0.3, axis='y')
        
        # 6. Connection Success Rate
        ax6 = fig.add_subplot(gs[1, 2])
        ax6.bar(nodes, conn_success, color='#16a085')
        ax6.set_xlabel('Node', fontweight='bold')
        ax6.set_ylabel('Success Rate (%)', fontweight='bold')
        ax6.set_title('Connection Reliability')
        ax6.set_ylim(0, 105)
        ax6.grid(True, alpha=0.3, axis='y')
        
        # 7. Latency Distribution (if available)
        ax7 = fig.add_subplot(gs[2, :2])
        for node in nodes:
            lat = summaries[node]['gossip'].get('latency', {})
            if lat:
                metrics = ['Mean', 'Median', 'P95', 'P99']
                values = [lat.get(f'{m.lower()}_ms', 0) for m in metrics]
                ax7.plot(metrics, values, marker='o', label=f'Node {node}', linewidth=2)
        ax7.set_ylabel('Latency (ms)', fontweight='bold')
        ax7.set_title('Message Latency Distribution')
        ax7.legend()
        ax7.grid(True, alpha=0.3)
        
        # 8. Network Health
        ax8 = fig.add_subplot(gs[2, 2])
        total_peers = [summaries[n]['network']['total_peers'] for n in nodes]
        x = range(len(nodes))
        ax8.bar(x, total_peers, width, label='Total', color='#95a5a6', alpha=0.5)
        ax8.bar(x, healthy_peers, width, label='Healthy', color='#27ae60')
        ax8.set_xlabel('Node', fontweight='bold')
        ax8.set_ylabel('Peers', fontweight='bold')
        ax8.set_title('Peer Health')
        ax8.set_xticks(x)
        ax8.set_xticklabels(nodes)
        ax8.legend()
        ax8.grid(True, alpha=0.3, axis='y')
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"[SAVED] {save_path}")
        else:
            plt.show()
    
    # ========================================================================
    # CONSENSUS TIMELINE
    # ========================================================================
    
    def plot_consensus_timeline(self, save_path: str = None):
        """Plot consensus events over time"""
        consensus_data = self.data.get('consensus', {})
        
        if not consensus_data:
            print("[WARN] No consensus data found")
            return
        
        fig, ax = plt.subplots(figsize=(14, 6))
        fig.suptitle('Consensus Protocol Timeline', fontsize=16, fontweight='bold')
        
        y_pos = 0
        yticks = []
        yticklabels = []
        
        for node_id, records in consensus_data.items():
            if not records:
                continue
            
            # Extract events
            for record in records:
                event = record.get('event', '')
                timestamp = record.get('timestamp', '')
                
                try:
                    dt = datetime.fromisoformat(timestamp)
                    time_num = mdates.date2num(dt)
                    
                    # Color code by event type
                    if 'vote' in event:
                        color = 'blue'
                        marker = 'o'
                    elif 'state_change' in event:
                        color = 'red'
                        marker = 's'
                    elif 'heartbeat' in event:
                        color = 'green'
                        marker = '^'
                    else:
                        color = 'gray'
                        marker = 'x'
                    
                    ax.scatter(time_num, y_pos, c=color, marker=marker, s=100, alpha=0.7)
                except Exception:
                    pass
            
            yticks.append(y_pos)
            yticklabels.append(f"Node {node_id}")
            y_pos += 1
        
        ax.set_yticks(yticks)
        ax.set_yticklabels(yticklabels)
        ax.set_xlabel('Time', fontweight='bold')
        ax.set_title('Consensus Events by Node')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax.grid(True, alpha=0.3, axis='x')
        
        # Legend
        legend_elements = [
            Line2D([0], [0], marker='o', color='w', markerfacecolor='blue', markersize=10, label='Vote'),
            Line2D([0], [0], marker='s', color='w', markerfacecolor='red', markersize=10, label='State Change'),
            Line2D([0], [0], marker='^', color='w', markerfacecolor='green', markersize=10, label='Heartbeat')
        ]
        ax.legend(handles=legend_elements)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"[SAVED] {save_path}")
        else:
            plt.show()
    
    # ========================================================================
    # NETWORK HEALTH OVER TIME
    # ========================================================================
    
    def plot_network_health(self, save_path: str = None):
        """Plot network health events"""
        network_data = self.data.get('network', {})
        
        if not network_data:
            print("[WARN] No network health data found")
            return
        
        fig, ax = plt.subplots(figsize=(14, 6))
        fig.suptitle('Network Health Events', fontsize=16, fontweight='bold')
        
        y_pos = 0
        yticks = []
        yticklabels = []
        
        for node_id, records in network_data.items():
            if not records:
                continue
            
            for record in records:
                event = record.get('event', '')
                timestamp = record.get('timestamp', '')
                peer = record.get('peer', '')
                
                try:
                    dt = datetime.fromisoformat(timestamp)
                    time_num = mdates.date2num(dt)
                    
                    if 'partition_detected' in event:
                        color = 'red'
                        marker = 'v'
                        label = f"Partition: {peer}"
                    elif 'partition_healed' in event:
                        color = 'green'
                        marker = '^'
                        label = f"Healed: {peer}"
                    else:
                        color = 'gray'
                        marker = 'o'
                        label = event
                    
                    ax.scatter(time_num, y_pos, c=color, marker=marker, s=100, alpha=0.7)
                except Exception:
                    pass
            
            yticks.append(y_pos)
            yticklabels.append(f"Node {node_id}")
            y_pos += 1
        
        ax.set_yticks(yticks)
        ax.set_yticklabels(yticklabels)
        ax.set_xlabel('Time', fontweight='bold')
        ax.set_title('Partition Detection and Recovery')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax.grid(True, alpha=0.3, axis='x')
        
        # Legend
        legend_elements = [
            Line2D([0], [0], marker='v', color='w', markerfacecolor='red', markersize=10, label='Partition'),
            Line2D([0], [0], marker='^', color='w', markerfacecolor='green', markersize=10, label='Healed')
        ]
        ax.legend(handles=legend_elements)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"[SAVED] {save_path}")
        else:
            plt.show()
    
    # ========================================================================
    # GENERATE ALL PLOTS
    # ========================================================================
    
    def generate_all_plots(self, output_dir: str = "plots"):
        """Generate all available visualizations"""
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\n[INFO] Generating visualizations in {output_dir}/...")
        
        plots_generated = []
        
        if self.data.get('adaptive'):
            path = os.path.join(output_dir, "adaptive_gossip.png")
            self.plot_adaptive_metrics(save_path=path)
            plots_generated.append(path)
        
        if self.data.get('summaries'):
            path = os.path.join(output_dir, "summary_dashboard.png")
            self.plot_summary_dashboard(save_path=path)
            plots_generated.append(path)
        
        if self.data.get('consensus'):
            path = os.path.join(output_dir, "consensus_timeline.png")
            self.plot_consensus_timeline(save_path=path)
            plots_generated.append(path)
        
        if self.data.get('network'):
            path = os.path.join(output_dir, "network_health.png")
            self.plot_network_health(save_path=path)
            plots_generated.append(path)
        
        print(f"\n[SUCCESS] Generated {len(plots_generated)} visualizations:")
        for plot in plots_generated:
            print(f"  - {plot}")
        
        return plots_generated


def main():
    parser = argparse.ArgumentParser(
        description='DAPSS Comprehensive Metrics Visualization',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python plot_metrics.py                    # Generate all plots interactively
  python plot_metrics.py --save             # Save all plots to plots/ directory
  python plot_metrics.py --adaptive         # Show only adaptive gossip plot
  python plot_metrics.py --dashboard        # Show only summary dashboard
  python plot_metrics.py --dir custom_metrics  # Use custom metrics directory
        """
    )
    
    parser.add_argument('--dir', default='metrics', help='Metrics directory (default: metrics)')
    parser.add_argument('--save', action='store_true', help='Save plots instead of displaying')
    parser.add_argument('--output', default='plots', help='Output directory for saved plots')
    parser.add_argument('--adaptive', action='store_true', help='Show adaptive gossip metrics only')
    parser.add_argument('--dashboard', action='store_true', help='Show summary dashboard only')
    parser.add_argument('--consensus', action='store_true', help='Show consensus timeline only')
    parser.add_argument('--network', action='store_true', help='Show network health only')
    parser.add_argument('--all', action='store_true', help='Generate all visualizations')
    
    args = parser.parse_args()
    
    print("="*70)
    print("DAPSS Metrics Visualization")
    print("="*70)
    
    viz = MetricsVisualizer(metrics_dir=args.dir)
    
    if not viz.load_data():
        print("\n[ERROR] No data to visualize!")
        print("[INFO] Make sure:")
        print("  1. Nodes have been running")
        print("  2. Metrics are being collected")
        print(f"  3. Metrics files exist in {args.dir}/")
        sys.exit(1)
    
    save_path = args.output if args.save else None
    
    # Determine what to plot
    if args.all or (not args.adaptive and not args.dashboard and not args.consensus and not args.network):
        # Generate all plots
        if args.save:
            viz.generate_all_plots(output_dir=args.output)
        else:
            viz.plot_adaptive_metrics()
            viz.plot_summary_dashboard()
            viz.plot_consensus_timeline()
            viz.plot_network_health()
    else:
        # Generate specific plots
        if args.adaptive:
            viz.plot_adaptive_metrics(save_path=os.path.join(save_path, "adaptive.png") if save_path else None)
        
        if args.dashboard:
            viz.plot_summary_dashboard(save_path=os.path.join(save_path, "dashboard.png") if save_path else None)
        
        if args.consensus:
            viz.plot_consensus_timeline(save_path=os.path.join(save_path, "consensus.png") if save_path else None)
        
        if args.network:
            viz.plot_network_health(save_path=os.path.join(save_path, "network.png") if save_path else None)
    
    print("\n[DONE] Visualization complete!")


if __name__ == "__main__":
    main()