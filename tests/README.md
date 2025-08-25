# OpenSearch PPL Load Test Framework

## Setup

1. Install dependencies:
   ```bash
   ./scripts/install.sh
   ```

2. Load sample data (run once):
   ```bash
   PYTHONPATH=. python scripts/load_sample_data.py
   ```

3. Run load tests:
   ```bash
   python run_load_test.py
   ```

## Project Structure

```
├── src/
│   ├── loadtest/          # Core load testing framework
│   │   ├── config.py       # Configuration classes
│   │   ├── load_tester.py  # Main load tester
│   │   └── metrics.py      # Metrics collection
│   └── utils/              # Utility modules
│       ├── wal_logger.py   # WAL logging
│       └── observability.py # Real-time monitoring
├── scripts/                # Setup and utility scripts
├── tests/                  # Test examples
└── logs/                   # Generated log files
```

## Framework assumes test data is already loaded in OpenSearch indices.