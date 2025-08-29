#!/bin/bash

# Quick rebalancing script for OpenSearch cluster
HOST="opense-clust-F0oiBIzi4TAq-6ffabb460181b586.elb.us-west-2.amazonaws.com"
AUTH="admin:CalciteLoadTest@123"

echo "=== OpenSearch Cluster Rebalancing ==="

# 1. Cancel running queries
echo "1. Cancelling running queries..."
curl -k -u "$AUTH" -X POST "https://$HOST/_tasks/_cancel?actions=indices:data/read/search*"

# 2. Set timeouts
echo "2. Setting cluster timeouts..."
curl -k -u "$AUTH" -X PUT "https://$HOST/_cluster/settings" -H "Content-Type: application/json" -d '{
  "transient": {
    "search.default_search_timeout": "30s",
    "plugins.ppl.query.timeout": "30s"
  }
}'

# 3. Check current distribution
echo "3. Current shard distribution:"
curl -k -u "$AUTH" "https://$HOST/_cat/shards/big5*?v&h=index,shard,prirep,node"

# 4. Create new indices with 3 shards each
echo "4. Creating resharded indices..."
for i in {01..10}; do
  echo "Creating big5-${i}_resharded..."
  curl -k -u "$AUTH" -X PUT "https://$HOST/big5-${i}_resharded" -H "Content-Type: application/json" -d '{
    "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 0
    }
  }'
done

# 5. Start reindexing (background tasks)
echo "5. Starting reindex operations..."
for i in {01..10}; do
  echo "Reindexing big5-${i}..."
  curl -k -u "$AUTH" -X POST "https://$HOST/_reindex?wait_for_completion=false" -H "Content-Type: application/json" -d "{
    \"source\": {\"index\": \"big5-${i}\"},
    \"dest\": {\"index\": \"big5-${i}_resharded\"}
  }"
done

# 6. Create alias for load balancing
echo "6. Creating load-balanced alias..."
curl -k -u "$AUTH" -X POST "https://$HOST/_aliases" -H "Content-Type: application/json" -d '{
  "actions": [
    {"add": {"index": "big5-*_resharded", "alias": "big5_balanced"}}
  ]
}'

echo "✓ Rebalancing initiated!"
echo "✓ Monitor progress: curl -k -u '$AUTH' 'https://$HOST/_cat/recovery?v&active_only=true'"
echo "✓ Use 'big5_balanced*' in queries instead of 'big5*'"