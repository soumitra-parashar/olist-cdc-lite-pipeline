# olist-cdc-lite-pipeline

A change-data-capture-style project pipeline built on the `Olist e-commerce dataset`, simulating a live OLTP system and propagating inserts/updates/deletes through a streaming pipeline into a versioned analytical store.

## Project Overview

My previous project (CoinGecko API project) proved a batch medallion pipeline with orchestration, This project proves something different: handling state that changes over time, inserts, updates, and deletes arriving continously, and correctly reflecting that in an analytical store.

 
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

## Target Tables

Simulating change events on 3 of Olists tables, chosen specifically because they have a  believable change story unlike static/refernce tables such as 'geolocation'.
The tables are:

- **`orders`** - insert (order placed) → update (status:pending → shipped → delivered) → occasional cancellation
- **`order_items`** - items added to an order after creation
- **`order_reviews`** - review submitted, then edited or removed.

## NOTE: Why cdc-lite ?
This project simulates CDC via a dual-write pattern, the generator writes to postgres and publishes the matching event to Kafka directly, rather than true log-based CDC.

This is a deliberate scope decision - see[notes/design/decision.md] which will be added later for details.