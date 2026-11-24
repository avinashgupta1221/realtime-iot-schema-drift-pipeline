# Real-Time IoT Ingestion Pipeline with Schema Drift Handling

A runnable Python + Apache Kafka streaming project. It consumes JSON telemetry, detects schema changes, validates and normalizes records, enriches them, and writes analytics-ready rows to a SQLite warehouse in micro-batches.

## Architecture

```text
IoT simulator ──> Kafka topic: iot-telemetry ──> streaming consumer
                                                   │
                                  schema registry (versioned JSON schemas)
                                                   │
                         validation / normalization / enrichment / DQ checks
                                                   │
                            SQLite warehouse + quarantine table
                                                   │
                           metrics endpoint + structured application logs
```

## Features

- Kafka-based event ingestion (Apache Kafka in KRaft mode; no ZooKeeper).
- Versioned schema registry with backward-compatible additive evolution.
- Handles unknown fields safely: records are accepted, drift is recorded, and new fields are retained in a JSON `extra_fields` column.
- Required-field and type validation; invalid records go to a quarantine table rather than stopping the stream.
- Field normalization, timestamp parsing, numeric casting, unit normalization, and device/site enrichment.
- Micro-batch writes, idempotent event IDs, consumer offset commits after processing.
- Monitoring counters exposed at `http://localhost:8000/metrics` in JSON.
- Alert-style warnings for schema drift and data-quality failures.
- Unit tests and sample event payloads.

## Requirements

- Docker Desktop / Docker Engine with Docker Compose
- Python 3.10+

## Run it

1. Start Kafka:

   ```bash
   docker compose up -d kafka
   ```

2. Create and activate a virtual environment, then install dependencies:

   ```bash
   python -m venv .venv
   # Windows PowerShell:
   .venv\Scripts\Activate.ps1
   # macOS/Linux:
   source .venv/bin/activate

   pip install -r requirements.txt
   ```

3. In terminal 1, start the consumer:

   ```bash
   python -m app.consumer
   ```

4. In terminal 2, generate telemetry (includes legacy and evolved payloads):

   ```bash
   python -m app.producer --count 100 --interval 0.25
   ```

5. In terminal 3, inspect the warehouse:

   ```bash
   python -m app.inspect
   ```

6. Open the monitoring endpoint:

   `http://localhost:8000/metrics`

## Run tests

```bash
python -m unittest discover -s tests -v
```

## Schema evolution policy

Schemas are stored in `schemas/` and are versioned explicitly.

- Version 1 defines the original telemetry contract.
- Version 2 adds optional `battery_pct` and `firmware_version` fields.
- Version 3 adds optional `signal_strength` and `metadata`.
- Compatibility rule: existing required fields and their types cannot be removed or changed. New fields must be optional or have a safe default.
- Unknown fields are not discarded. They are preserved in `extra_fields`, counted as schema drift, and surfaced in logs/metrics.
- A missing required field or invalid core type is a data-quality failure and is written to `quarantine_events`.

This is a lightweight application-level registry for a self-contained assignment. In production, the registry can be replaced by Confluent Schema Registry or AWS Glue Schema Registry.

## Warehouse tables

- `telemetry`: clean, analytics-ready records; `event_id` is unique for idempotent writes.
- `quarantine_events`: invalid payloads and rejection reasons.
- `schema_drift_events`: observed schema changes and unknown field names.

## Example analytics query

```sql
SELECT device_type, AVG(temperature_c) AS avg_temperature
FROM telemetry
GROUP BY device_type;
```

The SQLite database is created at `data/warehouse.db`.

## Project structure

```text
app/
  consumer.py       Kafka consumer and micro-batch loop
  producer.py       IoT telemetry simulator
  schemas.py        Versioned schema registry and compatibility checks
  transform.py      Validation, normalization, enrichment
  warehouse.py      SQLite schema and batch persistence
  monitoring.py     JSON metrics endpoint
  inspect.py        Warehouse summary utility
schemas/            Versioned JSON schema documents
tests/              Unit tests for drift and transformation behavior
data/               Runtime SQLite database (created automatically)
docker-compose.yml  Kafka broker
requirements.txt
```

## Notes

- The default broker is `localhost:9092`.
- Set `KAFKA_BOOTSTRAP_SERVERS` to override it.
- This project is designed to be easy to run locally and demonstrate the requested concepts. For production deployment, add TLS/SASL, secrets management, distributed schema registry, dead-letter topic, retention policies, and external warehouse credentials.
