# Experimental Design

This document describes the dataset, training methodology, and evaluation protocol used in the Spark RCA project.

---

## Dataset

| Property | Value |
|----------|-------|
| Total applications | 79 |
| Failure classes | 7 (1 baseline + 6 injected failures) |
| Data source | TPC-H benchmark queries (~25GB scale factor) |
| Event log format | Spark event logs (JSON, ZSTD-compressed) |
| Storage | HDFS |

### Failure Categories

| Label | Category | Applications | TPC-H Query | Injection Method |
|-------|----------|-------------|-------------|------------------|
| 0 | Baseline | 12 | Q21 | Normal execution |
| 1 | Out of Memory | ~10-12 | Q9 | BROADCAST hint on lineitem |
| 2 | Data Skew | 12 | Q18 | Salted join key (99% skew) |
| 3 | Serialization | ~9-11 | Q2 | Non-serializable Socket in UDF |
| 4 | Network Timeout | ~11-14 | Q1 | UDF sleep exceeding heartbeat timeout |
| 5 | Disk Space | ~11 | Cross Join | Massive shuffle spill via cross join |
| 6 | Metadata | ~11 | Iterative | HDFS path deleted mid-read |

Application counts vary slightly per class because some injected failures cause the Spark application to crash before generating a usable event log.

### Data Generation

All data was generated using `CampaignRunner.scala`, which orchestrates the failure injection campaign:

1. For each failure scenario, a new `SparkSession` is created with event logging enabled.
2. The session runs a TPC-H query with the specified failure injection.
3. Event logs are written to scenario-specific HDFS directories (`/project/spark-logs/<scenario>/`).
4. The session is stopped to finalize the event log before the next run.

---

## Feature Extraction

Features are extracted by `FeatureExtractor.scala` using app-level aggregation:

- Task metrics are grouped by `app_id` across **all** stages (not just the root cause stage).
- Stage metrics are aggregated separately and joined.
- 25 raw features are computed, covering runtime behavior, structural characteristics, and derived ratios.

See [engineering-decisions.md](engineering-decisions.md) for the rationale behind the 25-feature design.

---

## Train/Test Split

| Property | Value |
|----------|-------|
| Method | `DataFrame.randomSplit([0.8, 0.2], seed=42)` |
| Training set | ~62 applications |
| Test set | ~17 applications |
| Seed | 42 (fixed for reproducibility) |

Note: Spark's `randomSplit` performs a random partition, not a stratified split. With a well-balanced dataset (imbalance ratio < 2.0x), the random split produces a representative distribution across classes.

---

## Preprocessing Pipeline

1. **VectorAssembler** — Combines 25 (or 22, for Model C) feature columns into a single feature vector.
2. **StandardScaler** — Normalizes features to zero mean and unit variance (`withMean=True, withStd=True`).
3. **Classifier** — Random Forest, Decision Tree, or Logistic Regression.

These three stages are composed into a Spark ML `Pipeline` for atomic fit/transform.

---

## Evaluation Metrics

All models are evaluated using four metrics from `MulticlassClassificationEvaluator`:

| Metric | Description |
|--------|-------------|
| Accuracy | Fraction of correctly classified samples |
| Weighted F1 | Harmonic mean of precision and recall, weighted by class support |
| Weighted Precision | Fraction of true positives among predicted positives, weighted |
| Weighted Recall | Fraction of true positives among actual positives, weighted |

Weighted variants account for class imbalance by scaling each class's contribution by its support.

---

## Cross-Validation

5-fold cross-validation is performed on the full dataset (79 applications) using Spark ML's `CrossValidator`:

- Estimator: Full pipeline (VectorAssembler → StandardScaler → RandomForestClassifier)
- Evaluator: Weighted F1
- Folds: 5
- Seed: 42

Cross-validation provides a more robust estimate of generalization performance than a single train/test split, especially with a small dataset.

---

## Reproducibility

All experiments can be reproduced by:

1. Running the preprocessing pipeline (`PreprocessRunner`) to generate feature parquet files.
2. Running the ML pipeline script (`docker exec spark-shell /opt/spark/bin/spark-submit /opt/spark-rca/research/scripts/run_ml_pipeline.py`).

Fixed random seeds ensure deterministic splits and model training.
