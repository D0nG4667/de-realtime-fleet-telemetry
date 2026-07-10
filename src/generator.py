#!/usr/bin/env python3
"""
Fleet Telemetry Generator: Synthetic IoT Stream Simulator.
Generates realistic spatial trajectories and engine diagnostics for a fleet of trucks,
intentionally injecting out-of-order events to test downstream watermark thresholds.
"""

import argparse
import json
import logging
import random
import sys
import time
import uuid
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [TelemetrySimulator] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Check if confluent-kafka is available
try:
    from confluent_kafka import Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning("confluent-kafka not found. Running in stdout emulation mode.")

# Constants for Austin, TX bounding region for geospatial realistic walk
START_LAT = 30.2672
START_LON = -97.7431


class VehicleState:
    """Represents the real-time physical state of a single transport asset."""
    
    def __init__(self, vehicle_id: str):
        self.vehicle_id: str = vehicle_id
        self.lat: float = START_LAT + random.uniform(-0.1, 0.1)
        self.lon: float = START_LON + random.uniform(-0.1, 0.1)
        self.speed_mph: float = random.uniform(45.0, 70.0)
        self.engine_temp_f: float = random.uniform(185.0, 195.0)
        self.overheating: bool = False

    def update(self) -> None:
        """Applies a random walk to update telemetry state realistically."""
        # Update coordinates based on speed (heading south-west/north-east)
        speed_factor = self.speed_mph / 3600.0  # approximate scale
        self.lat += speed_factor * random.choice([-0.01, 0.01]) * random.uniform(0.5, 1.2)
        self.lon += speed_factor * random.choice([-0.01, 0.01]) * random.uniform(0.5, 1.2)

        # Update speed - random walk bounded between 0 and 75 mph
        self.speed_mph += random.uniform(-3.0, 3.0)
        self.speed_mph = max(0.0, min(self.speed_mph, 75.0))

        # Update engine temperature - random walk bounded
        # With 1% chance, trigger an overheating incident
        if not self.overheating and random.random() < 0.01:
            self.overheating = True
            logger.warning(f"SIMULATOR: Vehicle {self.vehicle_id} started OVERHEATING.")

        if self.overheating:
            # Temperature rises towards overheating point
            self.engine_temp_f += random.uniform(1.0, 4.0)
            if self.engine_temp_f > 225.0:
                self.overheating = False  # cooldown starts or incident resolved
                logger.info(f"SIMULATOR: Vehicle {self.vehicle_id} overheating event resolved.")
        else:
            self.engine_temp_f += random.uniform(-1.0, 1.0)
            self.engine_temp_f = max(175.0, min(self.engine_temp_f, 205.0))


def delivery_report(err: Optional[Exception], msg) -> None:
    """Callback triggered upon Kafka message delivery outcome."""
    if err is not None:
        logger.error(f"KAFKA_CLIENT: Message delivery failed: {err}")
    # Omit logging every successful message to avoid clogging console logs under load


def generate_payload(state: VehicleState, custom_timestamp_ns: int) -> dict:
    """Structures raw vehicle telemetry data as a JSON object."""
    return {
        "event_id": str(uuid.uuid4()),
        "vehicle_id": state.vehicle_id,
        "latitude": round(state.lat, 6),
        "longitude": round(state.lon, 6),
        "speed_mph": round(state.speed_mph, 2),
        "engine_temp_f": round(state.engine_temp_f, 2),
        "timestamp_ns": custom_timestamp_ns
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Real-Time IoT & Fleet Telemetry Ingestion Simulator")
    parser.add_argument("--bootstrap-servers", default="localhost:29092", help="Kafka bootstrap broker list")
    parser.add_argument("--topic", default="fleet-telemetry", help="Target Kafka topic")
    parser.add_argument("--rate", type=int, default=10, help="Standard messaging output rate (events/second)")
    parser.add_argument("--vehicles", type=int, default=5, help="Number of distinct vehicles in fleet")
    parser.add_argument("--late-pct", type=float, default=0.10, help="Percentage of events that are delayed (0.0 to 1.0)")
    parser.add_argument("--max-delay-sec", type=int, default=12, help="Maximum delay for late events in seconds")

    args = parser.parse_args()

    # Initialize fleet
    fleet: List[VehicleState] = [VehicleState(f"TX-TRUCK-{1000 + i}") for i in range(args.vehicles)]
    
    # Initialize Kafka Producer
    producer: Optional[Producer] = None
    if KAFKA_AVAILABLE:
        try:
            producer = Producer({
                "bootstrap.servers": args.bootstrap_servers,
                "client.id": "telemetry-simulator",
                "queue.buffering.max.messages": 100000
            })
            logger.info(f"KAFKA_CLIENT: Connected to broker list -> {args.bootstrap_servers}")
        except Exception as exc:
            logger.error(f"KAFKA_CLIENT: Connection failed. Switched to stdout. Error: {exc}")
            producer = None

    # Late buffers queue for out-of-order simulation: holds (send_time_seconds, payload_str)
    late_buffer: List[tuple] = []
    
    interval = 1.0 / args.rate
    logger.info(f"SIMULATOR: Telemetry simulation started. Target Rate = {args.rate} events/sec. Fleet Size = {args.vehicles} trucks.")

    try:
        while True:
            start_time = time.time()
            
            # Select random vehicle and update its state
            vehicle = random.choice(fleet)
            vehicle.update()
            
            # Current time stamps
            now_sec = time.time()
            now_ns = int(now_sec * 1000000000)
            
            # Decide if message will arrive late (out-of-order)
            is_late = random.random() < args.late_pct
            
            if is_late:
                # Assign an earlier timestamp to simulate network drops (late by 3 to max-delay seconds)
                delay_sec = random.uniform(3.0, float(args.max_delay_sec))
                event_timestamp_ns = now_ns - int(delay_sec * 1000000000)
                payload = generate_payload(vehicle, event_timestamp_ns)
                
                # Buffer to send at actual wall-clock time in future
                send_at_sec = now_sec + random.uniform(0.5, 2.0)
                late_buffer.append((send_at_sec, payload))
            else:
                # Normal event - current timestamp
                payload = generate_payload(vehicle, now_ns)
                payload_str = json.dumps(payload)
                
                # Emit normal event
                if producer:
                    producer.produce(
                        args.topic, 
                        key=payload["vehicle_id"], 
                        value=payload_str, 
                        callback=delivery_report
                    )
                else:
                    print(f"EMULATION: {payload_str}")

            # Process late buffer items whose time has arrived
            still_pending = []
            for send_at, late_payload in late_buffer:
                if now_sec >= send_at:
                    payload_str = json.dumps(late_payload)
                    if producer:
                        # Produce delayed message with historical timestamp
                        producer.produce(
                            args.topic,
                            key=late_payload["vehicle_id"],
                            value=payload_str,
                            callback=delivery_report
                        )
                        logger.info(f"SIMULATOR: Injected LATE (out-of-order) event for {late_payload['vehicle_id']} (delayed by {round((now_sec - late_payload['timestamp_ns']/1e9), 1)}s)")
                    else:
                        print(f"EMULATION_LATE: {payload_str}")
                else:
                    still_pending.append((send_at, late_payload))
            late_buffer = still_pending

            # Poll producer to execute callbacks
            if producer:
                producer.poll(0)
                
            # Throttle loop to match target rate
            elapsed = time.time() - start_time
            sleep_time = max(0.0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("SIMULATOR: Simulation interrupted by user. Shutting down...")
    finally:
        if producer:
            logger.info("KAFKA_CLIENT: Flushing pending callbacks...")
            producer.flush(timeout=5.0)


if __name__ == "__main__":
    main()
