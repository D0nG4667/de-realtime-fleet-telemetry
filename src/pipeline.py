#!/usr/bin/env python3
"""
Principal Data Engineer's Blueprint: PyFlink Real-Time Ingestion Pipeline.
Consumes telemetry events from Kafka, applies watermarks, runs sliding aggregations,
and commits structured payloads to a Redis serving cache.
"""

import os
import sys
import json
import logging
from typing import Dict, Any, Generator

from pyflink.common import WatermarkStrategy, Duration, Types, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction, AggregateFunction, WindowFunction, RuntimeContext
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import SlidingEventTimeWindows

# ==============================================================================
# 1. LOGGING & OBSERVABILITY CONFIGURATION
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PrincipalDE-Pipeline] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# ==============================================================================
# 2. DATA REPRESENTATION & QUALITY VALIDATION FUNCTIONS
# ==============================================================================
class TelemetryParser(MapFunction):
    """
    Parses incoming raw string messages into structured Python dictionaries.
    Filters out and logs/routes bad records to a Dead-Letter Queue (DLQ) in MinIO S3.
    """
    def __init__(self):
        self.s3_client = None
        self.s3_bucket = None

    def open(self, runtime_context: RuntimeContext) -> None:
        """Establishes connections to MinIO S3 client inside TaskManager executor."""
        try:
            import boto3
            from botocore.client import Config
            
            s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
            aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
            aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadminpassword")
            self.s3_bucket = os.environ.get("DLQ_S3_BUCKET", "fleet-telemetry-dlq")
            
            self.s3_client = boto3.client(
                's3',
                endpoint_url=s3_endpoint,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                config=Config(signature_version='s3v4')
            )
            # Lightweight head check to confirm bucket readiness
            try:
                self.s3_client.head_bucket(Bucket=self.s3_bucket)
                logger.info(f"STRUCTURE: Connected successfully to S3 DLQ Bucket '{self.s3_bucket}' on {s3_endpoint}")
            except Exception as bucket_exc:
                logger.warning(f"CONTROL: S3 DLQ Bucket '{self.s3_bucket}' not ready or accessible. Error: {bucket_exc}")
                self.s3_client = None
        except Exception as exc:
            logger.warning(f"CONTROL: S3 client initialization failed. Falling back to stdout warnings. Error: {exc}")
            self.s3_client = None

    def write_to_dlq(self, raw_payload: str, error_message: str) -> None:
        """Routes malformed or invalid records to the MinIO S3 DLQ bucket."""
        import time
        logger.warning(f"CONTROL: Routing malformed payload to DLQ. Error: {error_message}")
        
        if self.s3_client and self.s3_bucket:
            try:
                import uuid
                dlq_envelope = {
                    "raw_payload": raw_payload,
                    "error_message": error_message,
                    "dlq_timestamp_ms": int(time.time() * 1000)
                }
                
                # Format key to partition by date
                date_str = time.strftime("%Y-%m-%d")
                key = f"year={date_str[:4]}/month={date_str[5:7]}/day={date_str[8:10]}/{uuid.uuid4()}.json"
                
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=key,
                    Body=json.dumps(dlq_envelope),
                    ContentType="application/json"
                )
                logger.warning(f"CONTROL: Successfully committed DLQ log to MinIO: {key}")
            except Exception as s3_exc:
                logger.error(f"CONTROL: Failed pushing record to S3 DLQ: {s3_exc}")

    def map(self, value: str) -> Any:
        if value is None:
            return None
        
        try:
            record = json.loads(value)
            
            # 1. Schema Validation - check required fields
            required_keys = ["event_id", "vehicle_id", "latitude", "longitude", "speed_mph", "engine_temp_f", "timestamp_ns"]
            for key in required_keys:
                if key not in record:
                    self.write_to_dlq(value, f"Missing required key: {key}")
                    return None
            
            # 2. Boundary and Types Validation
            try:
                lat = float(record["latitude"])
                lon = float(record["longitude"])
                speed = float(record["speed_mph"])
                temp = float(record["engine_temp_f"])
                
                if not (-90.0 <= lat <= 90.0):
                    self.write_to_dlq(value, f"Latitude out of bounds: {lat}")
                    return None
                if not (-180.0 <= lon <= 180.0):
                    self.write_to_dlq(value, f"Longitude out of bounds: {lon}")
                    return None
                if speed < 0.0 or speed > 120.0:
                    self.write_to_dlq(value, f"Speed out of bounds: {speed}")
                    return None
                if not (100.0 <= temp <= 300.0):
                    self.write_to_dlq(value, f"Engine temp out of bounds: {temp}")
                    return None
            except (ValueError, TypeError) as val_err:
                self.write_to_dlq(value, f"Field type conversion failed: {val_err}")
                return None
            
            # Map sub-second timestamp_ns to millisecond timestamps for Flink
            record["timestamp_ms"] = int(record["timestamp_ns"]) // 1000000
            return record
            
        except json.JSONDecodeError as exc:
            self.write_to_dlq(value, f"JSON decode failed: {exc}")
            return None


class TelemetryTimestampAssigner(TimestampAssigner):
    """
    Extracts event timestamps from Telemetry messages to align sliding windows.
    Required by PyFlink to run event-time computations.
    """
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value["timestamp_ms"])


# ==============================================================================
# 3. STATEFUL ROLLING WINDOW AGGREGATOR
# ==============================================================================
class TelemetryAggregator(AggregateFunction):
    """
    Flink accumulator to compute running averages for vehicle diagnostics.
    Minimizes memory footprint by accumulating scalar sums instead of raw lists.
    Uses a standard Python dictionary to prevent PicklingErrors in worker UDF environments.
    """
    def create_accumulator(self) -> Dict[str, Any]:
        return {"count": 0, "sum_speed": 0.0, "sum_temp": 0.0}

    def add(self, value: Dict[str, Any], accumulator: Dict[str, Any]) -> Dict[str, Any]:
        if value is None:
            return accumulator
        accumulator["count"] += 1
        accumulator["sum_speed"] += float(value["speed_mph"])
        accumulator["sum_temp"] += float(value["engine_temp_f"])
        return accumulator

    def get_result(self, accumulator: Dict[str, Any]) -> Dict[str, Any]:
        if accumulator["count"] == 0:
            return {"avg_speed": 0.0, "avg_temp": 0.0, "event_count": 0}
        return {
            "avg_speed": round(accumulator["sum_speed"] / accumulator["count"], 2),
            "avg_temp": round(accumulator["sum_temp"] / accumulator["count"], 2),
            "event_count": accumulator["count"]
        }

    def merge(self, a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        merged = {"count": 0, "sum_speed": 0.0, "sum_temp": 0.0}
        merged["count"] = a["count"] + b["count"]
        merged["sum_speed"] = a["sum_speed"] + b["sum_speed"]
        merged["sum_temp"] = a["sum_temp"] + b["sum_temp"]
        return merged


class TelemetryWindowFunction(WindowFunction):
    """
    Passes grouping key (vehicle_id) and time-window attributes downstream,
    preventing state separation bottlenecks and data enrichment failures.
    """
    def apply(self, key: str, window, input_data) -> Generator[Dict[str, Any], None, None]:
        for val in input_data:
            # Enrich aggregated metrics with grouping key and window boundaries
            val["vehicle_id"] = key
            val["window_end_timestamp"] = window.end
            yield val


# ==============================================================================
# 4. REDIS INGESTION SINK
# ==============================================================================
class RedisSinkMapFunction(MapFunction):
    """
    Translates processed PyFlink records into low-latency Redis cache commands.
    This replaces typical database bottlenecks with high-throughput key-value sets.
    Features event-time-monotonic comparison to enforce idempotency and order safety.
    """
    def __init__(self, host: str = "redis", port: int = 6379, password: str = None):
        self.host = host
        self.port = port
        self.password = password
        self.client = None
        self.pseudonymize = os.environ.get("PSEUDONYMIZE_PII", "false").lower() == "true"
        self.salt = os.environ.get("PII_SALT", "default_secure_salt_value")

    def open(self, runtime_context: RuntimeContext) -> None:
        """Establishes connections inside the Flink executor process."""
        try:
            import redis
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=0,
                decode_responses=True,
                socket_timeout=1.0,
                socket_connect_timeout=1.0
            )
            self.client.ping()
            logger.info(f"STRUCTURE: Flink TaskManager connected successfully to Redis at {self.host}:{self.port}")
        except Exception as exc:
            logger.warning(
                f"CONTROL: Redis connection failed on {self.host}:{self.port}. "
                f"Falling back to local stdout emulator. Reason: {exc}"
            )
            self.client = None

    def map(self, value: Dict[str, Any]) -> Any:
        if value is None:
            return None
        
        vehicle_id = value.get("vehicle_id", "UNKNOWN")
        
        # Apply SHA-256 PII Pseudonymization if enabled
        if self.pseudonymize:
            import hashlib
            vehicle_id = hashlib.sha256((vehicle_id + self.salt).encode('utf-8')).hexdigest()[:12]
            
        redis_key = f"fleet:state:{vehicle_id}"
        
        redis_payload = {
            "avg_speed_mph": str(value.get("avg_speed", 0.0)),
            "avg_engine_temp_f": str(value.get("avg_temp", 0.0)),
            "total_sampled_signals": str(value.get("event_count", 0)),
            "pipeline_ingestion_epoch_ms": str(value.get("window_end_timestamp", 0))
        }
        
        # Ingestion step with event-time monotonic tracking for idempotency
        if self.client:
            try:
                incoming_ts = int(value.get("window_end_timestamp", 0))
                # Check the current timestamp in Redis to avoid stale out-of-order overrides
                existing_ts_str = self.client.hget(redis_key, "pipeline_ingestion_epoch_ms")
                existing_ts = int(existing_ts_str) if existing_ts_str else 0
                
                if incoming_ts >= existing_ts:
                    # Commit data state to Redis hash map
                    self.client.hset(redis_key, mapping=redis_payload)
                    # Set TTL of 24 hours (86400 seconds) for automatic cache expiry
                    self.client.expire(redis_key, 86400)
                    logger.info(f"SUCCESS: Committed state to Redis Hash '{redis_key}' with payload: {redis_payload}")
                else:
                    logger.warning(
                        f"IDEMPOTENCY: Ignored stale window update for '{redis_key}'. "
                        f"Incoming timestamp {incoming_ts} < Existing timestamp {existing_ts}"
                    )
            except Exception as exc:
                logger.error(f"CONTROL: Redis idempotent hset write failed for key {redis_key}. Exception: {exc}")
        else:
            logger.info(f"SUCCESS (EMULATION): Writing state update to Key Space -> '{redis_key}' with payload: {redis_payload}")
            
        return json.dumps({redis_key: redis_payload})


# ==============================================================================
# 5. CORE EXECUTION ENGINE SETUP
# ==============================================================================
def main() -> None:
    logger.info("STRUCTURE: Initializing stream execution environment...")
    
    # Establish local Flink Stream Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Retrieve configurations from environment
    kafka_host = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    redis_host = os.environ.get("REDIS_HOST", "redis")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    redis_password = os.environ.get("REDIS_PASSWORD", None)

    # Add required JAR dependencies to integrate Flink Kafka Connector
    # Check current directory or Docker Flink lib path
    kafka_jar_path = os.path.join(os.getcwd(), "lib", "flink-sql-connector-kafka-5.0.0-2.2.jar")
    if os.path.exists(kafka_jar_path):
        env.add_jars(f"file://{kafka_jar_path}")
        logger.info(f"STRUCTURE: Registered Kafka connector jar from path: {kafka_jar_path}")
    else:
        # Fallback to check default task manager JAR paths inside compose container
        default_container_jar = "/opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar"
        if os.path.exists(default_container_jar):
            env.add_jars(f"file://{default_container_jar}")
            logger.info("STRUCTURE: Registered Kafka connector jar from container library.")
        else:
            logger.warning("STRUCTURE: Local Kafka JAR not found. Proceeding with system jars.")

    # --------------------------------------------------------------------------
    # INGEST: Define Kafka Consumer Config
    # --------------------------------------------------------------------------
    # Configure modern Flink 2.x KafkaSource builder
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_host)
        .set_topics("fleet-telemetry")
        .set_group_id("flink-telemetry-ingestion-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Convert Kafka consumer configurations to DataStream
    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Fleet Telemetry Kafka Source"
    )

    # --------------------------------------------------------------------------
    # CONTROL: Custom Timestamp Watermarking Strategy for Delayed Events
    # --------------------------------------------------------------------------
    # Bounded out of orderness allows events to be late by up to 10 seconds (10000ms)
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(TelemetryTimestampAssigner())
    )

    # Parse and validate unstructured schemas
    parsed_stream = (
        raw_stream.map(TelemetryParser(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda x: x is not None)
    )

    # Apply Event-Time watermarking to handle out-of-order networks
    timestampped_stream = parsed_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # --------------------------------------------------------------------------
    # TRANSFORM: Stateful Sliding Window Aggregations
    # --------------------------------------------------------------------------
    # Sliding window of 5 minutes (300 seconds), computed/slid every 30 seconds
    # Preserving grouping keys and window boundaries using custom WindowFunction
    aggregated_stream = (
        timestampped_stream.key_by(lambda x: x["vehicle_id"])
        .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))
        .aggregate(
            TelemetryAggregator(),
            TelemetryWindowFunction(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Types.PICKLED_BYTE_ARRAY()
        )
    )

    # --------------------------------------------------------------------------
    # SUCCESS: Map Stream Outputs to Redis Cache Sinks
    # --------------------------------------------------------------------------
    redis_sync_stream = aggregated_stream.map(
        RedisSinkMapFunction(host=redis_host, port=redis_port, password=redis_password), 
        output_type=Types.STRING()
    )
    
    # Print verified output to console logs for visual trace
    redis_sync_stream.print()

    logger.info("STRUCTURE: Executing Flink Pipeline Execution Graph...")
    env.execute("Fleet-Telemetry-Ingestion-Engine")


if __name__ == "__main__":
    main()
