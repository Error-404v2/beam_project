# Apache Beam & Data Pipelines

## Apache Beam Basics

Apache Beam is a **unified data processing framework** that lets you define **pipelines** once and run them on different **execution engines** (**runners**). In practice, it is not the system doing the work — it is the **abstraction layer for pipeline logic**, while execution happens on engines like **Dataflow**, **Spark**, or **Flink**.

The core abstraction is the **PCollection**, which represents a **distributed** dataset (**bounded = batch, unbounded = streaming**). It is immutable and processed via **PTransforms** such as:

- Map / ParDo → element-wise logic
- Filter → remove invalid data
- GroupByKey / Combine → aggregation
- Flatten / Join → merging datasets

Practically, Beam pipelines look like:

Read → Clean → Transform → Aggregate → Write

## Streaming, Time Handling and how it is handled in Apache Beam

Real-world data is not ordered and often delayed (e.g., logs, sensors, APIs). Traditional batch systems assume ordered data, but streaming systems must handle **late** and **out-of-order** events correctly. Apache Beam addresses this through a time-aware processing model:

- **Watermark** — A reasonable **grace period** indicating that "most data up to this point has arrived." It helps decide if it is safe to compute before finalizing windows.

- **Windowing Functions** — Beam provides core transforms to handle **time, ordering, and delayed data** in streaming pipelines:
  - **WindowInto** — Assigns elements to windows (Fixed, Sliding, Session, Global) → controls how data is grouped over time
  - **WithTimestamps** — Sets or overrides event time → ensures processing uses actual event time, not arrival time
  - **Reify.timestamps()** — Exposes timestamp as part of the element → enables time-aware logic inside transforms

- **Late Data Handling** — Data arriving after the expected time can be:
  - dropped
  - or reprocessed within an allowed lateness threshold

## Other Frameworks (Candidates to Apache Beam)

- **Common frameworks**
  - Apache Spark → batch + micro-batch, strong ecosystem
  - Apache Flink → true real-time streaming, low latency
  - Apache Kafka Streams → event-driven processing within Kafka

- **Positioning of Apache Beam**
  - Unified model for **batch + streaming**
  - **Portable pipelines** across multiple runners (Dataflow, Spark, Flink)
  - Decouples **pipeline logic from execution engine**

- **Why it stands out**
  - Single codebase for different workloads
  - Flexible for evolving architecture
  - Strong fit for cloud-native (especially GCP) environments
