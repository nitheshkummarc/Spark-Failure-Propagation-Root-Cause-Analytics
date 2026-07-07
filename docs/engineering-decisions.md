# Engineering Decisions

This document explains the key technical choices behind the Spark Root-Cause Analytics project. Each section addresses a question that an interviewer, reviewer, or contributor would reasonably ask.

---

## Why Reverse BFS?

Spark failures propagate forward through stage dependencies. A failed shuffle stage causes all downstream consumers to abort. The observable symptom — the terminal failure — is often a victim, not the root cause.

Reverse BFS starts at the terminal failed stage and walks backward through parent edges. At each step, if a parent stage also failed, the current stage is a victim and traversal continues upstream. When no failed parents remain, the current stage is the root cause.

This is the natural traversal direction for root-cause tracing:

- **Forward traversal** requires knowing the root cause first — circular.
- **Reverse BFS** requires only the set of failed stages and parent edges — both available from Spark event logs.
- Time complexity is O(V + E) where V = stages and E = dependency edges.

---

## Why not GraphX?

Apache Spark GraphX is designed for iterative graph-parallel algorithms: PageRank, connected components, triangle counting. These algorithms require multiple supersteps over a distributed graph.

Root-cause analysis traversal is a single-pass BFS over a DAG with typically fewer than 100 stages. The graph fits entirely in driver memory. GraphX would introduce:

- Distributed partitioning overhead for a small graph
- RDD materialization for each iteration
- Pregel API complexity for what is structurally a simple BFS

Direct in-memory traversal on the driver has lower overhead and is simpler to reason about for DAGs of this scale.

---

## Why not GNNs?

Graph Neural Networks require substantially larger labeled graph datasets and introduce model complexity that is difficult to justify at this scale. With 79 applications, there is insufficient data for graph neural network training or meaningful generalization.

Reverse BFS is deterministic and interpretable — given the same DAG, it always produces the same root cause. A GNN would be a learned approximation of what is already a well-defined graph algorithm.

The combination of deterministic graph traversal (Reverse BFS) with interpretable classical ML (Random Forest for classification) is a more appropriate engineering choice for this problem size.

---

## Why Random Forest?

Four classifiers were evaluated: Random Forest, Gradient Boosted Trees, Decision Tree, and Logistic Regression.

Random Forest was selected because:

1. **Ensemble stability** — 100 decision trees reduce variance compared to a single tree.
2. **Feature importance** — The model provides a natural ranking of which telemetry features matter most, which is valuable for interpretability and for validating that the model learns meaningful signals.
3. **Robustness to small datasets** — Bagging with replacement creates diverse training subsets, reducing overfit risk on 79 samples.
4. **Best empirical performance** — Highest weighted F1 among all evaluated models.

Logistic Regression served as a linear baseline. Decision Tree provided interpretability at the cost of stability. Random Forest balanced all three concerns.

---

## Why Spark?

The project diagnoses failures in Apache Spark applications. Spark's event log format (`SparkListenerTaskEnd`, `SparkListenerStageCompleted`, `SparkListenerStageSubmitted`) is the richest source of execution telemetry available for post-mortem analysis.

Additionally:

- The preprocessing pipeline itself runs on Spark, processing event logs at scale.
- Feature extraction uses Spark SQL aggregations across all tasks and stages.
- The ML classifier uses Spark MLlib, keeping the entire pipeline within a single framework.

---

## Why Scala?

Scala is Spark's native language. Writing the pipeline in Scala provides:

- Direct access to `SparkListener` event types without serialization overhead.
- Assembly JAR deploys natively on YARN without Python dependency management.
- Type safety via sealed traits for failure scenario definitions.
- Compatibility with Spark's internal APIs (e.g., `SparkLauncher` for programmatic job submission).

The ML evaluation scripts are in PySpark because PySpark's interactive workflow (notebooks, rapid iteration) is more practical for exploratory model comparison and ablation studies.

---

## Why HDFS?

Spark event logs are written to HDFS by default when running on YARN. Processing them where they already reside avoids data movement.

The entire data lifecycle stays on HDFS:

```
TPC-H Raw Data  →  Parquet  →  Event Logs  →  Features  →  Model
   /project/raw   /project/parquet  /project/spark-logs  /project/features  /project/models
```

This mirrors how production Spark clusters operate and keeps the project realistic.

---

## Why YARN?

YARN is the standard resource manager for Hadoop clusters and the dominant deployment model for Spark in enterprise environments. Running on YARN means:

- Realistic resource contention and scheduling behavior
- Event logs contain YARN-specific failure modes (container killed, executor lost)
- Failure injection scenarios (OOM, disk space, network timeout) manifest authentically under YARN's resource management

Running on `local[*]` would mask the distributed failure patterns this project exists to diagnose.

---

## Why not streaming?

Batch post-mortem analysis is the correct model for failure diagnosis. A Spark event log is only complete after the job finishes or fails. Streaming analysis would require:

- Parsing partial event logs in real time
- Making root-cause judgments before all stages have completed
- Handling log ordering and late-arriving events

The current design processes complete event logs, which guarantees that the DAG is fully reconstructed and all stage outcomes are known before analysis begins. This is both simpler and more accurate.

Streaming diagnosis is a valid future extension but represents a fundamentally different problem.

---

## Why 25 features?

The 25-feature set was designed to capture four categories of execution behavior:

| Category | Count | Examples |
|----------|-------|----------|
| Stage-level aggregates | 8 | `mean_task_duration`, `total_memory_spilled`, `total_gc_time` |
| Structural features | 4 | `total_stages`, `failed_stages`, `max_stage_parallelism` |
| Ratio features | 4 | `completed_stage_ratio`, `spill_per_stage`, `gc_per_stage` |
| Derived features | 9 | `duration_heterogeneity_ratio`, `spill_ratio`, `gc_time_ratio` |

Confound analysis (see [confound-analysis.md](confound-analysis.md)) identified 3 features (`total_stages`, `stage_depth_of_failure`, `peak_memory_ratio`) as potential query fingerprints. These were removed in the final model (Model C, 22 features) with no meaningful drop in performance — confirming that the model learns runtime behavior, not query structure.
