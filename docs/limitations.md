# Limitations & Future Work

This document honestly states the known limitations of the current system and identifies directions for future improvement.

---

## Known Limitations

### 1. Single Root Cause Assumption

The current Reverse BFS implementation identifies **one** root cause per application. It starts from the terminal failed stage and traverses backward until it finds a failed stage with no failed parents.

In practice, a Spark application can have **multiple independent failure trees** — for example, two disconnected subgraphs failing from unrelated causes. The current algorithm will select one and miss the other.

**Impact**: For the 7 failure scenarios in this dataset, each application has a single dominant failure mode, so this limitation does not affect results. It would become relevant for production workloads with complex multi-query DAGs.

### 2. Small Dataset

| Property | Value |
|----------|-------|
| Total applications | 79 |
| Average per class | ~11 |
| Smallest class | ~9 |
| Largest class | ~14 |

79 samples is sufficient for proof-of-concept validation but insufficient for production deployment. With ~11 samples per class, the model's generalization to unseen workloads is uncertain. 5-fold cross-validation provides bounded confidence but does not eliminate this risk.

### 3. TPC-H Workload Diversity

All failure scenarios use TPC-H benchmark queries. This means:

- Query plans are relatively uniform within each class.
- The model has not been tested on non-TPC-H workloads (ETL pipelines, graph processing, streaming jobs).
- Feature distributions may differ substantially for production workloads.

### 4. Batch Post-Mortem Only

The system analyzes Spark event logs after the job has completed or failed. It does not support real-time or streaming failure detection. The event log must be complete before analysis begins.

### 5. YARN-Specific Telemetry

The failure injection scenarios and telemetry features assume a YARN-managed Spark deployment. Kubernetes-based Spark deployments would generate different failure signatures (pod OOMKilled vs. container killed by YARN, for example).

---

## Future Work

### Multi-Root Traversal

Extend Reverse BFS to identify **all** root causes in an application, not just the first one found. This requires:

1. Identifying all connected components in the failure subgraph.
2. Running Reverse BFS independently on each component.
3. Returning a list of `RootCauseResult` objects per application.

The `DAGBuilder` already computes disconnected subgraph information (root stages with no parents), so the infrastructure is partially in place.

### Dataset Augmentation

- Run the failure injection campaign with additional TPC-H queries to break the query-to-class correlation.
- Include non-TPC-H workloads (ETL, ML training, graph processing).
- Increase to 500+ applications for more robust model training.

### Additional Failure Modes

The current 7-class taxonomy covers common Spark failures but omits:

- **Garbage collection thrashing** (distinct from OOM)
- **Speculative execution conflicts**
- **Driver-side failures** (as opposed to executor-side)
- **Shuffle service failures**

### Online Diagnosis

Build an incremental version of the pipeline that:

1. Monitors the Spark event log directory for new files.
2. Parses and classifies completed applications automatically.
3. Sends alerts when failures are detected.

This would transform the system from a research tool into an operational monitoring component.

### Kubernetes Support

Adapt the feature extraction and failure injection to Spark-on-Kubernetes deployments. This would require:

- Mapping Kubernetes pod events to the existing telemetry schema.
- Adding Kubernetes-specific failure modes (pod eviction, node pressure).
- Testing with `spark-submit --master k8s://...`.
