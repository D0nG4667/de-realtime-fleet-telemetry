# Real-Time Fleet Telemetry: Developer Setup & Architecture Manual

This onboarding guide provides comprehensive, step-by-step instructions for establishing the Real-Time Fleet Telemetry Ingestion local development environment. It is designed to bridge the gap between high-level stream-processing architecture and developer-level execution semantics.

---

## 1. System Prerequisites & Environment Context

To replicate the production-grade streaming topologies locally, the development machine must support containerized virtualization and isolated python compilation.

### System Requirements
*   **Docker Engine & Compose (v24.0.0+)**: Must support Compose file format `3.8+` and container resource limit enforcement.
    *   *WSL2 Backend (Windows)*: Ensure WSL2 is configured with at least 8GB of RAM allocated in `~/.wslconfig` to accommodate Kafka and Flink TaskManager runtimes.
*   **Python (v3.10.x to v3.12.x)**: Managed locally via `uv`. The containerized environment compiles python runtimes on **Python 3.12** using Astral's package manager.
*   **Astral `uv` (v0.1.0+)**: A high-performance Python package installer and resolver. `uv` handles PEP 735 dependency groups to ensure the host and containers use identical package lock constraints.

### Networking & Security Topography
All local cluster components are isolated within a private Docker bridge network (`fleet-telemetry-net`).
*   **Internal Service DNS**: Containers communicate with one another using Docker-internal DNS (e.g., `kafka:9092`, `redis:6379`, `flink-jobmanager:8081`).
*   **Host Mappings**: The Docker Compose file maps public ingress listeners (e.g. `localhost:29092` for Kafka and `localhost:8082` for the Flink Web UI) to allow host-side monitoring and debugging without exposing core internal communication channels.

---

## 2. Step-by-Step Environment Bootstrapping

### Step A: Download the Kafka Connector JAR Dependency
Flink's core distribution is engine-agnostic and does not bundle message broker connectors by default. We utilize the Flink SQL Kafka connector to bridge the Java streaming runtime to the Kafka cluster.

Execute the following commands at the root of the project to create the binary library directory and download the matching connector JAR:

```bash
# 1. Create the dedicated lib folder
mkdir lib

# 2. Fetch the Flink SQL Kafka Connector JAR
curl -L https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar -o lib/flink-sql-connector-kafka-5.0.0-2.2.jar
```

> [!NOTE]
> **Version Matching Rationale**: 
> The JAR file name `flink-sql-connector-kafka-5.0.0-2.2.jar` represents:
> *   `5.0.0`: The connector release version.
> *   `2.2`: The target Apache Flink major version compatibility. If you mismatch the Flink version (2.2.x vs 2.3.x), the classloader will fail at runtime with JVM linkage or `ClassNotFoundException` errors.

---

### Step B: Establish Environment Variables
We enforce a **Zero Hardcoded Secrets** policy. Copy the provided `.env.example` file to create your active environment file:

```bash
cp .env.example .env
```

Review the parameters in `.env`:
*   `REDIS_PASSWORD`: Secures the Redis instance container.
*   `KAFKA_PORT_HOST` and `REDIS_PORT_HOST`: Define the host-port mappings to prevent conflicts with pre-existing services on your development machine.

---

### Step C: Provision the Infrastructure Stack
Run the containerized services in detached mode. This command builds the custom PyFlink and Python Simulator images, sets up isolated networks, and starts the brokers:

```bash
docker compose up -d
```

Verify that all five services are active and running in a healthy state:

```bash
docker compose ps
```

The output should show:
*   `kafka`: Up and healthy (listening on `29092` for host connections).
*   `redis`: Up and healthy (listening on `6379` with password enforcement).
*   `flink-jobmanager`: Up and healthy (Web UI available at [http://localhost:8082](http://localhost:8082)).
*   `flink-taskmanager`: Up (registered with the JobManager).
*   `telemetry-generator`: Up (running the Python simulator in the background).

---

### Step D: Initialize Host Python Virtual Environment
For local IDE autocomplete, linting, and standalone testing of python scripts, sync the dependencies using `uv`:

```bash
# Sync host dependencies using uv
uv sync
```

Activate the virtual environment:
*   **Windows (PowerShell)**: `.venv\Scripts\Activate.ps1`
*   **macOS / Linux**: `source .venv/bin/activate`

---

## 3. Pipeline Ingestion & Run Execution

The telemetry stream is processed in two decoupled phases: message generation (producing to Kafka) and stream aggregation (consuming from Kafka, writing state to Redis).

```
[generator.py] ──► (Kafka Topic: fleet-telemetry) ──► [pipeline.py] ──► (Redis Hash)
```

### Step 1: Manage the Telemetry Generator
The `telemetry-generator` container starts automatically and publishes simulated vehicle coordinates to Kafka.

If you need to run the simulator locally on your host for testing, override the default bootstrap server mapping to reference the host listener port (`29092`):

```bash
uv run python src/generator.py --bootstrap-servers localhost:29092 --rate 10 --vehicles 8 --late-pct 0.15
```

#### CLI Parameters Explained:
*   `--rate`: The number of events emitted per second across the active fleet.
*   `--vehicles`: The number of unique vehicle IDs (e.g. `TX-TRUCK-1000`) simulated.
*   `--late-pct`: The percentage of events simulated as "late arrivals" (out-of-order data).
*   `--max-delay-sec`: The maximum age delay (in seconds) applied to simulated late events. Useful for verifying event-time watermarking bounds.

---

### Step 2: Submit the PyFlink Streaming Job
Submit the streaming aggregation pipeline script to the containerized Flink JobManager. Flink compiles the Python pipeline code, builds the logical stream graph, and schedules tasks to execute on the TaskManager worker slots:

```bash
docker exec flink-jobmanager flink run -py /opt/flink/usrlib/pipeline.py
```

Upon successful submission, the JobManager REST client will output a JobID:
```text
Job has been submitted with JobID: e1b1cf63cd3edbacc4e37ab7140a8cb4
```
You can view the active JobGraph, backpressure metrics, checkpoint counts, and operator watermarks by visiting the Flink Dashboard at [http://localhost:8082](http://localhost:8082).

---

## 4. Cache Verification & Event-Time Idempotency

### Step 1: Query the Ingestion Cache
Log into the Redis container with password authentication:

```bash
docker exec -it redis redis-cli -a aSecureRedisPassword123
```

Check for the populated fleet state keys:

```redis
# List all keys matching the fleet state prefix
KEYS "fleet:state:*"
```

Verify that the key outputs correspond to the vehicle IDs generated by the simulator:
1. `fleet:state:TX-TRUCK-1000`
2. `fleet:state:TX-TRUCK-1001`
... (etc.)

---

### Step 2: Inspect Cache Aggregates
Retrieve the state hash values for a single vehicle:

```redis
HGETALL "fleet:state:TX-TRUCK-1001"
```

The hash map contains the running sliding aggregates computed by Flink:
```text
1) "avg_speed_mph"
2) "41.1"                             # 5-minute rolling average speed
3) "avg_engine_temp_f"
4) "198.36"                           # 5-minute rolling engine temperature
5) "total_sampled_signals"
6) "563"                              # Cumulative count of valid processed events
7) "pipeline_ingestion_epoch_ms"
8) "1783651980000"                    # The ending timestamp of the Flink sliding window
```

---

### Step 3: Event-Time Idempotency Mechanism
Because telemetry signals travel across unstable mobile networks, data packets can arrive out of order (late). If a late-arriving event is processed after a newer sliding window has already been committed to Redis, it could overwrite the cache with stale data.

To prevent this, `src/pipeline.py` implements a **monotonic event-time filter** inside the custom Redis sink:

```
                  ┌──────────────────────────────┐
                  │   Incoming Flink Aggregate   │
                  │ (pipeline_ingestion_epoch_ms)│
                  └──────────────┬───────────────┘
                                 │
                                 ▼
              HGET "fleet:state:ID" pipeline_ingestion_epoch_ms
                                 │
                                 ▼
                     /───────────────────────\
                    <  Incoming Epoch > Cache? >
                     \───────────────────────/
                                 │
                    ┌────────────┴────────────┐
                 Yes│                         │No (Late Event)
                    ▼                         ▼
            ┌───────────────┐         ┌───────────────┐
            │  Update Redis │         │ Log Warning & │
            │  HSET State   │         │ Discard Write │
            └───────────────┘         └───────────────┘
```

You can witness this in the TaskManager worker logs. When Flink processes out-of-order sliding windows that contain delayed data for a window epoch that has already been superseded by a more recent window, it drops the update:

```text
# docker logs flink-taskmanager
2026-07-10 02:53:12 INFO PythonWorker - IDEMPOTENCY: Dropped stale update for fleet:state:TX-TRUCK-1001. Incoming epoch 1783651950000 is older than current cache epoch 1783651980000.
```

---

## 5. MinIO S3 Dead-Letter Queue (DLQ) Verification

To handle and audit validation-failed messages locally, the pipeline routes anomalies to a local MinIO S3 emulator.

### Step 1: Access the MinIO Console Dashboard
Open [http://localhost:9001](http://localhost:9001) in your browser.
*   **Credentials**:
    *   *Access Key*: `minioadmin`
    *   *Secret Key*: `minioadminpassword`
*   Select **Object Browser** on the left menu, then click on the **`fleet-telemetry-dlq`** bucket. Here, you will see files partitioned chronologically: `year=YYYY/month=MM/day=DD/`.

### Step 2: Query the DLQ from the Cluster Command Line
You can list and verify the written dead-letter payloads by executing a Python script using `boto3` inside the running Flink JobManager container:

```bash
# List all files inside the fleet-telemetry-dlq bucket
docker exec flink-jobmanager python3 -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadminpassword'); print([obj['Key'] for obj in s3.list_objects_v2(Bucket='fleet-telemetry-dlq').get('Contents', [])])"
```

---

## 6. PII Pseudonymization Verification

To comply with GDPR data minimization and privacy standards, vehicle identifiers can be dynamically pseudonymized (hashed) at the Flink boundary before caching.

### Step 1: Enable Pseudonymization in the Environment
Open your local `.env` file and modify the following parameters:

```ini
PSEUDONYMIZE_PII=true
PII_SALT=my_secure_salt_value_123
```

### Step 2: Restart Flink Job to Apply Configurations
Cancel the current job and resubmit it to reload environment variables:

```bash
# 1. List active jobs to find your JobID
docker exec flink-jobmanager flink list

# 2. Cancel the job
docker exec flink-jobmanager flink cancel <JobID>

# 3. Resubmit the job
docker exec flink-jobmanager flink run -d -py /opt/flink/usrlib/pipeline.py
```

### Step 3: Verify Hashed Redis Keys
Connect to the Redis container CLI and list the keys:

```bash
docker exec -it redis redis-cli -a aSecureRedisPassword123 KEYS "fleet:state:*"
```

*   **Pseudonymization Disabled (`false`)**: Redis keys appear as human-readable vehicle IDs:
    `fleet:state:TX-TRUCK-1001`
*   **Pseudonymization Enabled (`true`)**: Redis keys appear as salted SHA-256 hex pseudonyms:
    `fleet:state:9a8d7c6b5a4f...` (protecting the identity of individual assets/drivers).

---

## 7. Architectural Resolutions & Troubleshooting

Below are the key systemic blockers resolved:

### 1. PyFlink Host Java dependency
*   **Blocker**: Running PyFlink locally on a developer host requires installing a specific matching JDK version and configuring `JAVA_HOME`.
*   **Resolution**: Submitting Python scripts container-native via `docker exec flink-jobmanager flink run -py ...` maps execution directly into pre-configured JVM environments inside the container, eliminating host JDK installation requirements.

### 2. PyFlink Accumulator Pickling Errors
*   **Blocker**: Defining custom Python classes for window aggregations (e.g. `class AggregatedState(Accumulator)`) throws `PicklingError: Can't pickle <class ...>` when Flink distributes computations to python worker subprocesses.
*   **Resolution**: Aggregator state schemas are serialized as standard Python dictionaries (`dict`). This ensures native JSON serialization compatibility over the Flink-Python RPC boundary.

### 3. Flink TaskManager JVM Out-Of-Memory (OOM) Crashes
*   **Blocker**: Python execution in Flink requires spawning a Python worker daemon alongside the Java TaskManager process. In resource-restricted containers, the TaskManager frequently crashes when the OS kills the Python daemon for exceeding container memory.
*   **Resolution**: In `compose.yml`, memory limits are explicitly configured. We tuned the Flink TaskManager config properties in `config/flink-conf.yaml` to allocate dedicated off-heap framework and task memory, leaving headroom for Python worker memory processes:
    *   `taskmanager.memory.framework.heap.size: 128mb`
    *   `taskmanager.memory.task.heap.size: 256mb`

### 4. Slow JVM Zookeeper/KRaft Healthchecks
*   **Blocker**: Default healthcheck configurations for Kafka containers using built-in java connection scripts block the container thread, leading to timeout cascades that mark the broker unhealthy and halt dependencies.
*   **Resolution**: Implemented a lightweight, non-blocking bash socket verification check (`bash -c 'echo > /dev/tcp/localhost/9092'`) that provides instantaneous, low-overhead readiness status.

### 5. Docker Desktop Windows Named Pipe 500 Bottlenecks
*   **Blocker**: Under WSL2 and Windows environments under high concurrent container startup cycles, Docker Engine socket buffers occasionally return 500 API errors when trying to read container JSON properties.
*   **Resolution**: Added decoupled dependencies in `compose.yml` and structured health checks (`minio-init` waits for healthy `minio`, etc.) to pace service bootstrapping, protecting WSL2 virtual socket endpoints from traffic spikes.

### 6. Defunct/Zombie Python Processes on Container Shutdown
*   **Blocker**: When stopping containers via `docker compose down`, Flink TaskManager containers can get stuck in a "Stopping" state, throwing errors indicating that a process ID is a "zombie" and cannot be killed. This occurs because the JVM running as PID 1 does not reap orphaned PyFlink Python worker subprocesses after they exit.
*   **Resolution**: Added `init: true` to `flink-jobmanager` and `flink-taskmanager` in `compose.yml`. This automatically runs the container using a lightweight init daemon (`tini`) as PID 1, which properly forwards signal drops and reaps zombie processes instantly.

