from opensearchpy import OpenSearch

def test_cluster_health(client: OpenSearch):
    """Test cluster health endpoint"""
    return client.cluster.health()

def test_index_search(client: OpenSearch):
    """Test basic search on an index"""
    return client.search(
        index="test-index",
        body={"query": {"match_all": {}}}
    )

def test_ppl_query(client: OpenSearch):
    """Test PPL query execution"""
    return client.transport.perform_request(
        'POST',
        '/_plugins/_ppl',
        body={"query": "source=test-index | head 10"}
    )

def test_document_index(client: OpenSearch):
    """Test document indexing"""
    import time
    doc = {
        "timestamp": time.time(),
        "message": "Load test document",
        "value": 42
    }
    return client.index(
        index="load-test",
        body=doc
    )