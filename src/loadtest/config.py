from dataclasses import dataclass
from typing import Dict, Any, List, Union, Optional
from enum import Enum
from ..utils.query_groups import QueryGroup

class QueryType(Enum):
    DSL = "dsl"
    PPL = "ppl"

class LoadMode(Enum):
    QPS = "qps"
    CONCURRENCY = "concurrency"

@dataclass
class QPSRamp:
    qps: float
    duration_seconds: int

@dataclass
class ConcurrencyRamp:
    concurrency: int
    duration_seconds: int

@dataclass
class QueryConfig:
    name: str
    query_type: QueryType
    query: str
    load_mode: LoadMode = LoadMode.QPS
    target_qps: Union[float, List[QPSRamp]] = None
    target_concurrency: Union[int, List[ConcurrencyRamp]] = None
    index: str = None
    query_group: Optional[QueryGroup] = None

@dataclass
class LoadTestConfig:
    host: str = "localhost"
    port: int = 9200
    use_ssl: bool = False
    username: str = None
    password: str = None
    duration_seconds: int = 60
    queries: List[QueryConfig] = None
    index_pattern: str = "big5*"  # Default index pattern
    # Metrics export configuration
    metrics_host: str = None
    metrics_port: int = 9200
    metrics_use_ssl: bool = False
    metrics_username: str = None
    metrics_password: str = None
    
    def to_client_config(self) -> Dict[str, Any]:
        config = {
            'hosts': [{'host': self.host, 'port': self.port}],
            'use_ssl': self.use_ssl,
            'verify_certs': False,
            'ssl_show_warn': False
        }
        if self.username and self.password:
            config['http_auth'] = (self.username, self.password)
        return config
    
    def to_metrics_client_config(self) -> Dict[str, Any]:
        # Use metrics cluster config if specified, otherwise use main cluster
        host = self.metrics_host or self.host
        port = self.metrics_port
        use_ssl = self.metrics_use_ssl
        username = self.metrics_username or self.username
        password = self.metrics_password or self.password
        
        config = {
            'hosts': [{'host': host, 'port': port}],
            'use_ssl': use_ssl,
            'verify_certs': False,
            'ssl_show_warn': False
        }
        if username and password:
            config['http_auth'] = (username, password)
        return config