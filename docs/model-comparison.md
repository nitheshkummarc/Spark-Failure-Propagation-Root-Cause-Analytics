# Model Comparison

This document compares the classifiers evaluated for failure category prediction.

---

## Models Evaluated

| Model | Configuration | Library |
|-------|--------------|---------|
| Random Forest | 100 trees, maxDepth=10, maxBins=32 | PySpark MLlib |
| Decision Tree | maxDepth=5, entropy impurity | PySpark MLlib |
| Logistic Regression | Multinomial, L2 regularization (λ=0.01), 100 iterations | PySpark MLlib |

All models use the same preprocessing pipeline: `VectorAssembler` → `StandardScaler` → Classifier.

---

## Results

### Test Set Performance (80/20 split, seed=42)

| Model | Accuracy | Weighted F1 | Weighted Precision | Weighted Recall |
|-------|----------|-------------|-------------------|-----------------|
| **Random Forest** | **0.8824** | **0.8676** | **0.9294** | **0.8824** |
| Decision Tree | 0.7647 | 0.7108 | 0.7529 | 0.7647 |
| Logistic Regression | 0.7059 | 0.7098 | 0.7647 | 0.7059 |

> Exact values depend on the random split and the specific features in HDFS at evaluation time. The relative ordering (RF > DT > LR) is consistent across all runs.

### 5-Fold Cross-Validation (Random Forest)

| Metric | Score |
|--------|-------|
| CV F1 | 0.9507 |
| CV Accuracy | 0.9607 |

Cross-validation scores are higher than single-split scores because the model trains on more data per fold.

---

## Selection Rationale

Random Forest was selected as the primary model based on:

1. **Best empirical performance** — Highest F1 across all evaluated models.
2. **Ensemble stability** — 100-tree ensemble reduces variance compared to a single Decision Tree.
3. **Feature importance** — Provides a natural ranking of telemetry features, which was used for ablation study and confound analysis.
4. **Generalization** — 5-fold CV confirms that performance is not an artifact of a favorable train/test split.

### Why not Gradient Boosted Trees?

GBTs were considered but not included in the primary evaluation because:

- With 79 samples, the risk of overfitting with boosting is higher than with bagging.
- Random Forest already achieved strong performance, making the marginal benefit of GBTs unlikely to justify the added complexity.

### Why not deep learning?

Neural networks require significantly more training data to generalize. With ~11 samples per class, a deep model would almost certainly overfit. Classical ML with feature engineering is the appropriate choice at this scale.

---

## Per-Class Analysis

Random Forest achieves high per-class accuracy across most failure categories. Confusion matrix analysis (available in `research/scripts/run_ml_pipeline.py`) shows:

- **Baseline**, **Serialization**, **Metadata** — Near-perfect classification due to distinctive execution signatures.
- **Data Skew**, **Network Timeout** — Occasionally confused due to similar task duration distributions.
- **OOM**, **Disk Space** — Well-separated by spill and memory features.

---

## Reproducing Results

```bash
# Run from within the Spark cluster environment
docker exec spark-shell /opt/spark/bin/spark-submit /opt/spark-rca/research/scripts/run_ml_pipeline.py
```

This script trains all three models, prints metrics, confusion matrices, feature importance, and 5-fold CV scores.
