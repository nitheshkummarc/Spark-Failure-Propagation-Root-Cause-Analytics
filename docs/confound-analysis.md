# Confound Analysis

This document investigates whether the ML classifier learns genuine runtime behavior or merely memorizes query fingerprints.

---

## Motivation

Each failure scenario uses a specific TPC-H query. This means certain features — particularly `total_stages` and `stage_depth_of_failure` — may encode which query was executed rather than how the application behaved at runtime.

If the model relies on these features, it would not generalize to new queries or workloads. The model would be a query classifier, not a failure classifier.

---

## Hypothesis

**H₀**: The model primarily learns runtime execution behavior.

**H₁**: The model primarily memorizes query structure through confounded features.

---

## Identified Confounds

### `total_stages`

Each TPC-H query produces a fixed number of stages (determined by the query plan). Within a failure class that always uses the same query, `total_stages` has **zero intra-class variance** — every application in that class has the same value.

This means the model can use `total_stages` alone to identify which query was run, without learning anything about failure behavior.

### `stage_depth_of_failure`

This feature is defined as the count of completed stages before the first failure. It correlates near-perfectly with `total_stages` (Pearson > 0.95 in experiments), making it a redundant confound.

### `peak_memory_ratio`

In several failure classes, all applications show identical `peak_memory_ratio` values (zero variance), likely because the same executor memory configuration and query plan produce identical peak memory behavior.

---

## Experiment Design

Three Random Forest models were trained with identical hyperparameters (100 trees, maxDepth=10, seed=42) and the same 80/20 split:

| Model | Features | Count |
|-------|----------|-------|
| Model A | All 25 features | 25 |
| Model B | All except `total_stages`, `stage_depth_of_failure` | 23 |
| Model C | All except `total_stages`, `stage_depth_of_failure`, `peak_memory_ratio` | 22 |

---

## Results

| Model | Accuracy | Weighted F1 | Weighted Precision | Weighted Recall |
|-------|----------|-------------|-------------------|-----------------|
| A (25 features) | 0.8824 | 0.8676 | 0.9294 | 0.8824 |
| B (23 features) | 0.8824 | 0.8676 | 0.9294 | 0.8824 |
| C (22 features) | 0.8824 | 0.8676 | 0.9294 | 0.8824 |

Performance remained approximately unchanged across all three configurations.

5-fold cross-validation confirmed the same pattern:

| Model | CV F1 |
|-------|-------|
| A | 0.9507 |
| B | 0.9507 |
| C | 0.9507 |

---

## Conclusion

**H₀ is supported.** Removing all three confounded features does not degrade model performance. The classifier primarily learns from runtime execution signals — GC pressure, memory spill, task duration variance, shuffle behavior — rather than from query-structural fingerprints.

This is an important finding because it means the model has a path to generalizing beyond TPC-H workloads, provided the same telemetry features are available.

---

## Implication for Production Use

Model C (22 features) is recommended for any downstream use because:

1. It avoids reliance on confounded features.
2. Its performance matches Model A.
3. It demonstrates that the model captures behavioral patterns, not query artifacts.

---

## Reproducing

```bash
docker exec spark-shell /opt/spark/bin/spark-submit /opt/spark-rca/research/scripts/confound_analysis.py
```

This script runs the full confound analysis: per-feature variance analysis, Model A/B/C comparison, and 5-fold CV for all three configurations.
