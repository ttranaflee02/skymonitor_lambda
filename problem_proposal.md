# ✈️ PROJECT PROPOSAL: SKYMONITOR
## SYSTEM FOR REAL-TIME FLIGHT TRACKING & AVIATION BIG DATA ANALYTICS

---

## 1. Background & Problem Statement

### 1.1. Context
In the era of hyper-connectivity, the aviation industry generates a massive volume of data (Big Data) every second. Every aircraft in the sky acts as a mobile IoT data source, constantly transmitting coordinates, altitude, velocity, and status. 
Harnessing this data stream not only assists in Air Traffic Control (ATC) but also supports the analysis of economic trends, tourism, and flight safety.

### 1.2. Problem
Traditional tracking systems often face several challenges:
1.  **Long-term Storage Difficulties:** Streaming data is generated continuously. If only Message Queues (like Kafka) are used, data is deleted after a short retention period. A **Data Lake** solution is required for permanent raw data storage.
2.  **Latency vs. Accuracy:** There is a need to balance *immediate* aircraft position updates (Streaming) with accurate statistical analysis on large *historical* datasets (Batch).
3.  **Lack of Automation:** Running manual data analysis jobs is time-consuming and error-prone. An **Orchestration** mechanism is needed to automate the pipeline.

>**Proposed Solution:**
Build the **SkyMonitor** system based on **Lambda Architecture** to comprehensively solve problems ranging from collection and storage to processing and visualization.

---

## 2. Project Objectives

### 2.1. Technical Goals
*   **Build a Data Lake:** Implement **MinIO** to store raw data long-term, simulating a Cloud Storage (S3) architecture.
*   **Hybrid Processing:** Run **Spark Structured Streaming** (Real-time) and **Spark Batch** (Historical) in parallel.
*   **Automation:** Utilize **Apache Airflow** to schedule, monitor, and manage the data pipeline automatically.
*   **Visualization:** Develop interactive real-time dashboards.

### 2.2. Business Goals
*   **Real-time Monitoring:** Monitor aircraft positions within Vietnamese airspace with low latency (< 1 minute).
*   **Safety Alerts:** Detect and warn of aircraft with rapid descent rates—a sign of landing or emergency.
*   **Market Insight:** Statistics on flight market share and flight density of airlines in the SE Asia region by day/week.

---

## 3. Scope & Data

*   **Data Source:** OpenSky Network API (REST API).
*   **Geographic Scope:**
    *   *Storage & Batch:* Entire Southeast Asia region (Wide Bounding Box).
    *   *Real-time:* Vietnam region (Narrow Bounding Box for speed optimization).
*   **Key Data Fields:** `icao24` (ID), `callsign`, `origin_country`, `longitude`, `latitude`, `geo_altitude`, `velocity`.

---

## 4. Architecture & Tech Stack

The system follows an extended **Lambda Architecture**:

### 4.1. Ingestion Layer
*   **Tech:** Python Script, **Apache Kafka**, **MinIO**.
*   **Data Flow:**
    *   Python Script calls the OpenSky API periodically (every 15s).
    *   **Flow 1 (Hot Data):** Push data into the Kafka topic `flight-live` for immediate processing.
    *   **Flow 2 (Cold Data):** Save raw data as `.json` or `.parquet` files into **MinIO** (Data Lake) using a directory structure: `year/month/day/hour`.

### 4.2. Processing Layer
*   **Speed Layer (Real-time):**
    *   **Tech:** **Spark Structured Streaming**.
    *   **Logic:** Read from Kafka -> Filter Vietnam coordinates -> Calculate `delta_altitude` (descent rate) -> Detect "Landing/Emergency."
    *   **Output:** Write to **MongoDB**.
*   **Batch Layer (Historical):**
    *   **Tech:** **Spark SQL** (triggered by Airflow).
    *   **Logic:** Read data from **MinIO** -> Group by `Country`/`Airline` -> Calculate average flight density -> Aggregation.
    *   **Output:** Write to **Elasticsearch**.

### 4.3. Orchestration Layer
*   **Tech:** **Apache Airflow**.
*   **Role:** Automated workflow management.
    *   *DAG 1:* Trigger Spark Batch Job at 00:00 daily to process the previous day's data from MinIO.
    *   *DAG 2:* Periodic health checks; send alerts if Kafka or the API fails.

### 4.4. Serving & Visualization Layer
*   **Real-time Dashboard:** **Streamlit** (connected to MongoDB) to render moving aircraft on a map with auto-updates.
*   **Analytics Dashboard:** **Kibana** (connected to Elasticsearch) to display pie/bar charts regarding aviation market share.

---

## 5. Implementation Plan

| Phase | Key Tasks | Tools |
| :--- | :--- | :--- |
| **Phase 1** | Set up Docker infrastructure (Kafka, Spark, MinIO, Airflow, DBs). | Docker Compose |
| **Phase 2** | Build Ingestion Pipeline: Push data to Kafka and MinIO. | Python, Kafka, MinIO |
| **Phase 3** | Develop Speed Layer: Streaming processing and MongoDB storage. | Spark Streaming, MongoDB |
| **Phase 4** | Develop Batch Layer & Automation: Write Airflow DAGs for Spark Batch. | Spark SQL, Airflow, MinIO |
| **Phase 5** | Build Dashboard & Visualization. | Streamlit, Kibana |
| **Phase 6** | Optimization, end-to-end testing, and final reporting. | All |

---

## 6. Technology Stack Summary

The **SkyMonitor** system is built on a standard Open Source ecosystem in the Data Engineering industry, ensuring modernity, scalability, and high performance.

| Layer | Technology | Description |
| :--- | :--- | :--- |
| **Programming** | **Python 3.9+** | Primary language for collection scripts (Producer), Spark logic, and Streamlit Dashboard. |
| **Infrastructure** | **Docker & Compose** | Containerizes all services to ensure a consistent environment and easy scaling. |
| **Ingestion** | **OpenSky API** | Reliable flight data source, free for educational purposes. |
| **Message Queue** | **Apache Kafka** | High-speed data buffer; decouples ingestion from processing to prevent bottlenecks. |
| **Data Lake** | **MinIO** | Low-cost long-term raw data storage, simulating professional S3-compatible Object Storage. |
| **Processing** | **Apache Spark** | Most powerful distributed processing engine: <br>• **Spark Structured Streaming:** Real-time.<br>• **Spark SQL:** Batch processing from Data Lake. |
| **Orchestration** | **Apache Airflow** | Schedules daily Spark jobs, manages workflows (DAGs), and sends error alerts. |
| **Storage (Serving)** | **MongoDB** | NoSQL database optimized for real-time JSON read/write (Hot Data). |
| | **Elasticsearch** | Powerful search engine for aggregated data, supporting fast statistical queries. |
| **Visualization** | **Streamlit** | Python framework for rapid interactive web app development (Flight Maps). |
| | **Kibana** | Dashboard tool for Elasticsearch, used for historical BI analysis. |

### Integration Flow
1.  **Python** calls API $\rightarrow$ Sends messages to **Kafka** (Stream) & Saves files to **MinIO** (Batch).
2.  **Spark Streaming** reads **Kafka** $\rightarrow$ Processing $\rightarrow$ **MongoDB** $\rightarrow$ Displayed on **Streamlit**.
3.  **Airflow** triggers **Spark Batch** $\rightarrow$ Reads **MinIO** $\rightarrow$ Aggregation $\rightarrow$ **Elasticsearch** $\rightarrow$ Displayed on **Kibana**.

---

## 7. Challenges & Solutions

1.  **API Rate Limits:** OpenSky limits the number of requests.
    *   *Solution:* Implement caching mechanisms and optimize the Bounding Box to avoid redundant API calls.
2.  **Small Files Problem on MinIO:** Storing too many small files degrades Spark performance.
    *   *Solution:* Use Airflow to periodically run "Compaction" jobs that merge small JSON files into larger Parquet files.
3.  **Data Synchronization:** Real-time and Batch data may diverge.
    *   *Solution:* Accept "Eventual Consistency" for Batch data and prioritize Real-time display for monitoring tasks.

---

## 8. Expected Outcomes

1.  A **complete Big Data Pipeline** operating stably on Docker.
2.  **Data Lake (MinIO):** Successfully stored historical flight data, ready for future Machine Learning training.
3.  **Streamlit Dashboard:** Intuitive visual map of aircraft operating in Vietnam with landing alerts.
4.  **Automated Reports:** Airflow automatically running and updating daily statistics without manual intervention.