# System Topology & Network Architecture

This blueprint documents the distributed system topology, port mappings, network boundaries, and data flow pathways for the Real-Time Fleet Telemetry Ingestion Engine.

---

## 1. Network Flow Diagram

The following Mermaid sequence diagram outlines how data traverses from edge IoT sensors through the local multi-container network layers into the low-latency caching storage.

```mermaid
graph TD
    subgraph Edge_IoT_Fleet["Edge IoT Fleet (Simulated)"]
        Sensors["50k GPS/Diag Sensors<br>(JSON payload)"]
        Simulator["src/generator.py<br>(telemetry-generator container)"]
        Sensors -->|Raw Sensor Readings| Simulator
    end

    subgraph Private_Bridge_Network["Isolated Docker Compose Network (fleet-telemetry-net)"]
        Broker["Apache Kafka Broker<br>(Port 9092/29092)"]
        JobManager["Flink JobManager (Master)<br>(Port 8081 Internal / 8082 Host UI)"]
        TaskManager["Flink TaskManager (Worker)<br>(Slots: 4, JVM Managed Memory)"]
        Cache["Redis Serving Layer<br>(Port 6379)"]
        MinIOS3["MinIO S3 Emulator<br>(Port 9000 API / 9001 Console)"]
        MinIOInit["minio-init Provisioner<br>(Creates fleet-telemetry-dlq bucket)"]

        Simulator -->|Producer Client| Broker
        Broker -->|Source Stream| TaskManager
        JobManager -.->|Orchestrates Execution Graph| TaskManager
        TaskManager -->|State Updates (hset)| Cache
        TaskManager -->|Malformed Payload DLQ (boto3)| MinIOS3
        MinIOInit -.->|One-time Setup| MinIOS3
    end
    subgraph Downstream_Clients["External Consumer Client Layers"]
        UI["Real-Time Route Dashboard"]
        API["Fleet Serving Rest API"]
        Audit["Compliance / Data Quality Audit"]
        UI -->|Sub-millisecond lookups| Cache
        API -->|Get State| Cache
        Audit -->|Inspect DLQ logs| MinIOS3
    end

    style Private_Bridge_Network fill:#f5f7fa,stroke:#b0c4de,stroke-width:2px;
    style Edge_IoT_Fleet fill:#fff5ee,stroke:#ffa07a,stroke-width:1px;
    style Downstream_Clients fill:#f0fff0,stroke:#3cb371,stroke-width:1px;
```

---

## 2. Ingestion Port Allocations & Configurations

All containers communicate through the isolated, bridge-driven virtual network `fleet-telemetry-net`. The following table lists the container network interfaces, internal-to-host port mappings, and security access levels:

| Container / Service | Internal Port | Host Port | Protocol | Security Boundaries | Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `kafka` (Broker) | `9092` | *N/A* | TCP | Internal-only within `fleet-telemetry-net` | Internal broker communications and Flink consumer ingest. |
| `kafka` (Client Interface) | `29092` | `29092` | TCP | Exposed to host localhost | External telemetry clients and local `src/generator.py` scripts. |
| `flink-jobmanager` | `6123` | *N/A* | TCP | Internal-only within `fleet-telemetry-net` | RPC communication interface for TaskManager workers. |
| `flink-jobmanager` (UI) | `8081` | `8082` | TCP | Exposed to host localhost | Apache Flink Job dashboard and performance metric graphing (configurable via `FLINK_JOBMANAGER_PORT`). |
| `flink-taskmanager` | Dynamic | *N/A* | TCP | Internal-only within `fleet-telemetry-net` | Data processing slots; executes PyFlink worker threads. |
| `redis` | `6379` | `6379` | TCP | Exposed to host localhost | Serving caching layer; state maps read by downstream dashboards (configurable via `REDIS_PORT`). |
| `minio` (S3 API) | `9000` | `9000` | TCP | Exposed to host localhost | S3-compatible REST API endpoints used by boto3 to write DLQ payloads. |
| `minio` (Console) | `9001` | `9001` | TCP | Exposed to host localhost | Browser-based administrator dashboard for inspecting S3 buckets and files. |
| `minio-init` | *N/A* | *N/A* | TCP | Short-lived init container | Automatically sets alias and runs bucket creation for `fleet-telemetry-dlq`. |

---

## 3. Network Isolation & Security Principles

1. **Decoupled Architecture**: Telemetry producers (simulators) do not communicate directly with the Flink stream engine. They publish messages to Apache Kafka, which acts as a backpressure buffer, preventing Flink from being overwhelmed by unexpected network spikes.
2. **Private DNS Resolution**: Services resolve each other within the bridge network via internal DNS names (e.g. `kafka:9092`, `redis:6379`, `flink-jobmanager:6123`) configured automatically by Docker Compose.
3. **Read-Only Bind Mounts**: The Flink configuration file (`flink-conf.yaml`) is mounted using read-only permissions (`:ro`). This ensures that worker nodes cannot modify central configurations at runtime, maintaining infrastructure idempotency.
4. **Port Boundaries**: Production deployments must restrict port `8081` (Flink UI) and `6379` (Redis) to authenticated VPN subnets to prevent unauthorized client writes or execution plan tampering.
