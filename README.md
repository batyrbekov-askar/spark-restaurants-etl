# Spark Restaurants ETL

## 1. Description
This project implements a simple **ETL pipeline using Apache Spark** to process restaurant-related data.
The pipeline extracts raw data, enriches it using the OpenCage Geocoding API, and writes the result into
partitioned **Parquet** files.

The project is designed to be:
- reproducible
- idempotent
- runnable locally or via Docker

---

## 2. Requirements

- **Python**: 3.11
- **Docker** and **Docker Compose** (optional, for Spark UI and cluster mode)
- **OpenCage API Key**

---

## 3. Preparation

### 3.1 Create virtual environment (local run)
```bash
python -m venv .venv
source .venv/bin/activate   # Linux / macOS
.venv\Scripts\activate      # Windows

pip install -r requirements.txt

OPENCAGE_API_KEY=your_api_key_here

docker compose up -d

http://localhost:8080
```

## 4. Run ETL

### Option 1: Module execution
```
python -m etl.job
```

### Option 2: Script execution
```
python src/etl/job.py
```

## 5. Output Data
Location
```
data/output/
```



