# Ablation Study

This document describes how feature importance was evaluated and how the feature set was refined.

---

## Methodology

Feature importance is computed from the trained Random Forest model. Each feature's importance score represents the total reduction in impurity (Gini) it provides across all 100 trees in the ensemble.

Two levels of analysis were performed:

1. **Feature ranking** — Ordering all features by importance score.
2. **Feature group contribution** — Aggregating importance by feature category.

---

| Rank | Feature | Category | Importance |
|------|---------|----------|------------|
| 1 | `total_shuffle_write` | Stage-level aggregate | 0.1197 |
| 2 | `total_stages` | Structural | 0.1157 |
| 3 | `mean_task_duration` | Stage-level aggregate | 0.1022 |
| 4 | `peak_memory_ratio` | Ratio | 0.0887 |
| 5 | `stage_depth_of_failure` | Structural | 0.0855 |
| 6 | `total_gc_time` | Stage-level aggregate | 0.0615 |
| 7 | `total_shuffle_read` | Stage-level aggregate | 0.0597 |
| 8 | `gc_per_stage` | Ratio | 0.0584 |
| 9 | `duration_heterogeneity_ratio` | Derived | 0.0504 |
| 10 | `max_stage_parallelism` | Structural | 0.0409 |

> Exact ranking varies slightly between runs. Structural features and resource consumption aggregates consistently rank high.

---

## Feature Group Contribution

| Category | Feature Count | Approximate Importance Share |
|----------|--------------|------------------------------|
| Stage-level aggregates | 8 | ~35-40% |
| Structural features | 4 | ~25-30% |
| Ratio features | 4 | ~15-20% |
| Derived features | 9 | ~10-15% |

**Key observations:**

- **Structural features** (`total_stages`, `stage_depth_of_failure`) are highly ranked. This is problematic because they carry information about query structure rather than runtime behavior, heavily motivating the confound analysis detailed in [confound-analysis.md](confound-analysis.md).
- **Resource aggregates** (`total_shuffle_write`, `mean_task_duration`, `total_gc_time`) provide the strongest runtime signals, indicating massive data movement or heavy processing delays prior to failure.
- **Derived ratios** (`peak_memory_ratio`, `gc_per_stage`) are effective because they normalize raw metrics, making them comparable across applications with different workload sizes.

---

## Top-5 / Top-10 Contribution

| Set | Approximate Cumulative Importance |
|-----|-----------------------------------|
| Top 5 features | 51.2% |
| Top 10 features | 78.3% |
| Remaining 15 features | 21.7% |

The long tail of low-importance features contributes marginally. However, removing them was not necessary because Random Forest handles irrelevant features gracefully (they are simply not selected for splits).

---

## Connection to Confound Analysis

The ablation study motivated the confound analysis. The observation that `total_stages` and `stage_depth_of_failure` had low but non-zero importance — combined with the observation that they had zero intra-class variance in several classes — raised the question of whether these features were acting as categorical query identifiers rather than behavioral signals.

This led to the Model A → Model B → Model C comparison described in [confound-analysis.md](confound-analysis.md).

---

## Reproducing

Feature importance is printed by `research/scripts/run_ml_pipeline.py` (Step 5) and `research/scripts/confound_analysis.py` (per-model top-10).
