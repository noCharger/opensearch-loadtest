#!/usr/bin/env python3
"""
Complete flow to rebalance OpenSearch cluster with single-shard indices
"""

import requests
import json
import time
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class ClusterRebalancer:
    def __init__(self, host, port=443, ssl=True, username=None, password=None):
        self.base_url = f"{'https' if ssl else 'http'}://{host}:{port}"
        self.auth = (username, password) if username and password else None
        
    def _request(self, method, endpoint, data=None):
        url = f"{self.base_url}{endpoint}"
        kwargs = {"verify": False}
        if self.auth:
            kwargs["auth"] = self.auth
        if data:
            kwargs["json"] = data
            kwargs["headers"] = {"Content-Type": "application/json"}
        
        response = getattr(requests, method.lower())(url, **kwargs)
        if not response.ok:
            print(f"Error {response.status_code}: {response.text}")
        response.raise_for_status()
        return response.json() if response.content else {}

    def check_current_distribution(self):
        """Check current shard distribution for big5* indices only"""
        shards = self._request("GET", "/_cat/shards/big5*?format=json&h=index,shard,prirep,state,node")
        
        node_counts = {}
        for shard in shards:
            if shard['prirep'] == 'p':  # Only primary shards
                node = shard['node']
                node_counts[node] = node_counts.get(node, 0) + 1
        
        print("Current big5* distribution:")
        for node, count in sorted(node_counts.items()):
            print(f"  {node}: {count} shards")
        return node_counts

    def cancel_running_queries(self):
        """Cancel all running search tasks"""
        print("Cancelling running queries...")
        try:
            self._request("POST", "/_tasks/_cancel?actions=indices:data/read/search*")
            print("✓ Search tasks cancelled")
        except Exception as e:
            print(f"⚠ Error cancelling tasks: {e}")

    def set_cluster_timeouts(self):
        """Set aggressive timeouts to prevent long-running queries"""
        print("Setting cluster timeouts...")
        try:
            settings = {
                "transient": {
                    "search.default_search_timeout": "30s"
                }
            }
            self._request("PUT", "/_cluster/settings", settings)
            print("✓ Search timeout set to 30s")
        except Exception as e:
            print(f"⚠ Error setting timeouts: {e}")

    def reindex_with_more_shards(self, source_index, target_shards=3):
        """Reindex single-shard index with more shards"""
        target_index = f"{source_index}_resharded"
        
        print(f"Reindexing {source_index} with {target_shards} shards...")
        
        # Create target index with more shards
        index_settings = {
            "settings": {
                "number_of_shards": target_shards,
                "number_of_replicas": 0
            }
        }
        
        try:
            self._request("PUT", f"/{target_index}", index_settings)
            print(f"✓ Created {target_index}")
        except Exception as e:
            if "already exists" not in str(e):
                raise
        
        # Reindex data
        reindex_body = {
            "source": {"index": source_index},
            "dest": {"index": target_index}
        }
        
        task = self._request("POST", "/_reindex?wait_for_completion=false", reindex_body)
        task_id = task.get("task")
        
        print(f"✓ Reindex started, task: {task_id}")
        return target_index, task_id

    def monitor_all_tasks(self):
        """Monitor all running tasks"""
        print("=== Monitoring All Tasks ===\n")
        
        # Get all tasks and show what's actually running
        tasks = self._request("GET", "/_tasks?detailed=true")
        
        print(f"Total tasks found: {len(tasks.get('tasks', {}))}")
        
        # Show all task actions to debug
        all_actions = set()
        for task_data in tasks.get("tasks", {}).values():
            action = task_data.get("action", "")
            all_actions.add(action)
        
        print(f"\nAll task actions: {sorted(all_actions)}")
        
        # Look for any task that might be reindexing
        relevant_tasks = []
        for task_data in tasks.get("tasks", {}).values():
            action = task_data.get("action", "")
            # Broader search - any task that might be related to indexing
            if any(keyword in action.lower() for keyword in ["reindex", "index", "bulk", "write"]):
                relevant_tasks.append(task_data)
        
        if not relevant_tasks:
            print("\nNo indexing-related tasks found.")
            return
        
        print(f"\nFound {len(relevant_tasks)} potentially relevant tasks:\n")
        
        for task in relevant_tasks:
            task_id = f"{task['node']}:{task['id']}"
            action = task.get("action", "unknown")
            status = task.get("status", {})
            created = status.get("created", 0)
            total = status.get("total", 0)
            
            if total > 0:
                progress = (created / total) * 100
                print(f"Task {task_id} ({action}): {progress:.1f}% ({created:,}/{total:,})")
            else:
                print(f"Task {task_id} ({action}): {status}")

    def create_alias(self, indices, alias_name):
        """Create alias for load balancing"""
        print(f"Creating alias {alias_name}...")
        
        actions = []
        for index in indices:
            actions.append({"add": {"index": index, "alias": alias_name}})
        
        alias_body = {"actions": actions}
        self._request("POST", "/_aliases", alias_body)
        print(f"✓ Alias {alias_name} created")

    def start_rebalance_flow(self, indices_to_rebalance=None):
        """Start rebalancing flow without monitoring"""
        print("=== OpenSearch Cluster Rebalancing Flow ===\n")
        
        # Step 1: Check current state
        print("1. Checking current distribution...")
        self.check_current_distribution()
        
        # Step 2: Stop problematic queries
        print("\n2. Stopping running queries...")
        self.cancel_running_queries()
        self.set_cluster_timeouts()
        
        # Step 3: Get indices to rebalance
        if not indices_to_rebalance:
            shards = self._request("GET", "/_cat/indices/big5*?format=json&h=index")
            indices_to_rebalance = [s["index"] for s in shards]
        
        print(f"\n3. Starting reindex for {len(indices_to_rebalance)} indices...")
        
        # Step 4: Start reindex tasks
        tasks = []
        for index in indices_to_rebalance:
            new_index, task_id = self.reindex_with_more_shards(index, target_shards=3)
            if task_id:
                tasks.append((index, new_index, task_id))
        
        print(f"\n✓ Reindex tasks started: {len(tasks)} tasks")
        print("\nTo monitor progress, run:")
        print("  python rebalance_cluster.py --monitor")
        print("\nTo complete setup after reindexing:")
        print("  python rebalance_cluster.py --finalize")
        
        return tasks
    
    def finalize_rebalance(self):
        """Finalize rebalancing by creating alias"""
        print("=== Finalizing Rebalance ===\n")
        
        # Get resharded indices
        indices = self._request("GET", "/_cat/indices/big5*_resharded?format=json&h=index")
        resharded_indices = [s["index"] for s in indices]
        
        if not resharded_indices:
            print("No resharded indices found. Run rebalance first.")
            return
        
        print(f"Found {len(resharded_indices)} resharded indices")
        
        # Create alias
        self.create_alias(resharded_indices, "big5_balanced")
        
        # Final distribution check
        print("\nFinal distribution:")
        self.check_current_distribution()
        
        print(f"\n✓ Rebalancing complete!")
        print(f"✓ Use 'big5_balanced*' in queries instead of 'big5*'")
        print(f"✓ New indices: {', '.join(resharded_indices)}")

def main():
    import sys
    
    # Configuration
    HOST = "opense-clust-F0oiBIzi4TAq-6ffabb460181b586.elb.us-west-2.amazonaws.com"
    USERNAME = "admin"
    PASSWORD = "CalciteLoadTest@123"
    
    rebalancer = ClusterRebalancer(HOST, username=USERNAME, password=PASSWORD)
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--monitor":
            rebalancer.monitor_all_tasks()
        elif sys.argv[1] == "--finalize":
            rebalancer.finalize_rebalance()
        else:
            print("Usage: python rebalance_cluster.py [--monitor|--finalize]")
    else:
        # Start rebalancing
        rebalancer.start_rebalance_flow()

if __name__ == "__main__":
    main()