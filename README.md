# 🛫 SkyMonitor - Real-time Flight Tracking

A Lambda Architecture system for flight tracking using OpenSky API, Kafka, Spark, MongoDB, and Elasticsearch.

---

## Quick Start

### 1. Clone & Setup
```bash
git clone https://github.com/yourname/skymonitor.git
cd skymonitor
cp .env.example .env
```

### 2. Start Services
```bash
docker-compose up -d
sleep 60
docker-compose ps
```

### 3. Create Kafka Topic
```bash
docker exec kafka kafka-topics --create \
  --topic flight-live \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 4. Install & Run Producer
```bash
pip install -r requirements.txt
python -m src.ingestion.producer
```

### 5. Start Spark Streaming (Terminal 2)
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/src/speed_layer/spark_streaming.py
```

### 6. Open Dashboards (Terminal 3)
```bash
streamlit run src/visualization/streamlit_dashboard.py
# Then open: http://localhost:8501
```

---

## Architecture

```
OpenSky API (every 15s)
    ↓
Producer → Kafka (flight-live topic)
    ├→ Spark Streaming → MongoDB → Streamlit Dashboard
    └→ MinIO (year/month/day/hour/) → Daily Batch Job → Elasticsearch → Kibana
```

---

## Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | - |
| Spark Master | http://localhost:8080 | - |
| Airflow | http://localhost:8888 | admin/admin |
| Kibana | http://localhost:5601 | - |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin |

---

## Troubleshooting

### Kafka unavailable
```bash
docker-compose restart kafka
sleep 10
```

### No data in MongoDB
```bash
docker exec mongo mongosh --eval "db.flights_realtime.count()"
```

### Spark job crashed
```bash
docker logs <spark-worker-id>
docker-compose restart spark-master spark-worker
```

### Check Kafka messages
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic flight-live \
  --max-messages 5
```

---

## Components

| Layer | Tech | Purpose |
|-------|------|---------|
| Ingestion | Python | Fetch OpenSky API |
| Hot Path | Kafka → Spark → MongoDB | Real-time (< 60s) |
| Cold Path | MinIO → Spark SQL → Elasticsearch | Historical (daily) |
| Orchestration | Airflow | Schedule batch jobs |
| UI | Streamlit + Kibana | Dashboards |

---

## Configuration

Edit `.env`:
```bash
OPENSKY_USERNAME=          # Leave empty for public API
OPENSKY_PASSWORD=          # Optional for unlimited requests
KAFKA_BROKERS=kafka:9092
MONGODB_URI=mongodb://mongo:27017
ELASTICSEARCH_HOST=elasticsearch
```

---

## Features

- ✅ Real-time flight tracking (Vietnam airspace)
- ✅ Rapid descent detection alerts
- ✅ Historical batch analytics
- ✅ Live Streamlit dashboard
- ✅ Elasticsearch analytics
- ✅ Airflow DAG scheduling
- ✅ Docker containerized

---

## Project Structure

```
skymonitor/
├── src/
│   ├── ingestion/          # OpenSky API producer
│   ├── speed_layer/        # Spark streaming
│   ├── batch_layer/        # Spark batch jobs
│   ├── orchestration/       # Airflow DAGs
│   ├── visualization/       # Streamlit dashboard
│   └── utils/              # Helpers
├── docker-compose.yml      # All services
├── requirements.txt        # Python packages
└── .env.example           # Configuration template
```

---

## Tech Stack

- **API**: OpenSky Network REST API
- **Ingestion**: Python 3.9+
- **Messaging**: Apache Kafka
- **Streaming**: Spark Structured Streaming
- **Storage**: MongoDB, MinIO, Elasticsearch
- **Orchestration**: Airflow
- **UI**: Streamlit, Kibana
- **Infrastructure**: Docker Compose

---

## Data Flow

### Speed Layer (Real-time)
OpenSky API → Producer → Kafka → Spark Streaming → MongoDB → Streamlit

**Latency**: 30-60 seconds

### Batch Layer (Daily)
MinIO → Spark SQL (00:00 UTC) → Elasticsearch → Kibana

**Frequency**: Daily aggregations

