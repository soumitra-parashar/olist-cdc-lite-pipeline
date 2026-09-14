# olist-cdc-lite-pipeline

A change-data-capture-style project pipeline built on the `Olist e-commerce dataset`, simulating a live OLTP system and propagating inserts/updates/deletes through a streaming pipeline into a versioned analytical store.

## Status: In progress - initial structure

## Architecture (planned)

Python event generator (SQLAlchemy) → dual-writes to Postgres + Kafka/Redpanda → Spark Structured Streaming (Delta MERGE INTO)
→ dbt snapshots(SCD Type 2) → orchestrated by Airflow

## Tech Stack

• Postgres
• Kafka (Redpanda)
• Spark Structured Streaming
• Delta Lake
• Airflow

## NOTE: Why cdc-lite ?
This project simulates CDC via a dual-write pattern, the generator writes to postgres and publishes the matching event to Kafka directly, rather than true log-based CDC.

This is a deliberate scope decision - see[notes/design/decision.md] which will be added later for details.