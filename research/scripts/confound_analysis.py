"""
Confound Analysis — Quantify query fingerprint vs behavioral signal.
Retrains RF under 3 feature configurations and compares metrics.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, mean, stddev, min as spark_min, max as spark_max,
    when, sum as spark_sum, corr, abs as spark_abs
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import json

spark = SparkSession.builder \
    .appName("Confound-Analysis") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

FEATURES_PATH = "hdfs://namenode:8020/project/features"
LABEL_NAMES = {0:"Baseline",1:"OOM",2:"Data Skew",3:"Serialization",4:"Network Timeout",5:"Disk Space",6:"Metadata"}

ALL_FEATURES = [
    "mean_task_duration", "std_task_duration", "max_task_duration",
    "total_memory_spilled", "total_disk_spilled",
    "total_shuffle_read", "total_shuffle_write", "total_gc_time",
    "total_stages", "failed_stages", "max_stage_parallelism", "stage_depth_of_failure",
    "completed_stage_ratio", "failed_stage_ratio", "spill_per_stage", "gc_per_stage",
    "duration_heterogeneity_ratio", "duration_variance", "max_min_duration_ratio",
    "spill_ratio", "disk_spill_ratio", "peak_memory_ratio",
    "gc_time_ratio", "task_failure_rate", "retry_count"
]

# Load data
df = spark.read.parquet(FEATURES_PATH).fillna(0, subset=ALL_FEATURES)
total = df.count()

print("=" * 90)
print("CONFOUND ANALYSIS — QUERY FINGERPRINT vs BEHAVIORAL SIGNAL")
print("=" * 90)

# ======================================================================
# SECTION 1: Per-Feature Confound Analysis
# ======================================================================
confound_features = ["total_stages", "stage_depth_of_failure", "peak_memory_ratio", "duration_heterogeneity_ratio"]

print("\n" + "=" * 90)
print("SECTION 1: INDIVIDUAL FEATURE CONFOUND ANALYSIS")
print("=" * 90)

for feat in confound_features:
    print(f"\n{'─' * 80}")
    print(f"  FEATURE: {feat}")
    print(f"{'─' * 80}")

    # Per-class statistics
    stats = df.groupBy("label").agg(
        mean(col(feat)).alias("mean"),
        stddev(col(feat)).alias("std"),
        spark_min(col(feat)).alias("min"),
        spark_max(col(feat)).alias("max"),
        count("*").alias("n")
    ).orderBy("label").collect()

    print(f"\n  {'Label':<6} {'Class':<22} {'N':>3} {'Mean':>12} {'Std':>10} {'Min':>10} {'Max':>10} {'ZeroVar?'}")
    print(f"  {'─'*82}")

    zero_var_classes = 0
    total_zero_var_samples = 0
    for row in stats:
        lbl = int(row["label"])
        name = LABEL_NAMES.get(lbl, "?")
        n = int(row["n"])
        m = row["mean"] or 0.0
        s = row["std"] or 0.0
        mn = row["min"] or 0.0
        mx = row["max"] or 0.0
        zv = "YES ❌" if (s == 0 or s is None or mn == mx) else "no"
        if s == 0 or s is None or mn == mx:
            zero_var_classes += 1
            total_zero_var_samples += n
        print(f"  {lbl:<6} {name:<22} {n:>3} {m:>12.4f} {s:>10.4f} {mn:>10.4f} {mx:>10.4f} {zv}")

    # Correlation with label (Pearson)
    pearson = df.stat.corr(feat, "label")
    print(f"\n  Pearson correlation with label: {pearson:.4f}")
    print(f"  Zero-variance classes: {zero_var_classes}/7 ({total_zero_var_samples}/{total} samples)")

    # Decision
    if zero_var_classes >= 4:
        print(f"  ⚠️ VERDICT: STRONG CONFOUND — {zero_var_classes}/7 classes have zero intra-class variance")
        print(f"     This feature acts as a categorical query-ID, not a continuous behavioral metric")
    elif zero_var_classes >= 2:
        print(f"  ⚠️ VERDICT: MODERATE CONFOUND — {zero_var_classes}/7 classes have zero variance")
    else:
        print(f"  ✅ VERDICT: LOW CONFOUND — feature has genuine variance within classes")

# ======================================================================
# SECTION 2: Correlation between total_stages and stage_depth_of_failure
# ======================================================================
print("\n" + "=" * 90)
print("SECTION 2: CROSS-FEATURE REDUNDANCY")
print("=" * 90)

cross_corr = df.stat.corr("total_stages", "stage_depth_of_failure")
print(f"\n  Pearson(total_stages, stage_depth_of_failure) = {cross_corr:.4f}")

identical = df.filter(col("total_stages") == col("stage_depth_of_failure")).count()
print(f"  Identical values: {identical}/{total} ({100*identical/total:.1f}%)")

if cross_corr > 0.95:
    print("  ⚠️ NEAR-PERFECT CORRELATION — these two features carry essentially the same information")
    print("     Removing both simultaneously is valid (not double-counting the penalty)")

# ======================================================================
# SECTION 3: RF RETRAINING — 3 CONDITIONS
# ======================================================================
print("\n" + "=" * 90)
print("SECTION 3: RANDOM FOREST RETRAINING — 3 CONDITIONS")
print("=" * 90)

# Define feature sets
FEATURES_A = ALL_FEATURES[:]  # All 25

FEATURES_B = [f for f in ALL_FEATURES if f not in ["total_stages", "stage_depth_of_failure"]]  # 23

FEATURES_C = [f for f in ALL_FEATURES if f not in ["total_stages", "stage_depth_of_failure", "peak_memory_ratio"]]  # 22

configs = [
    ("Model A (All 25 features)", FEATURES_A),
    ("Model B (−total_stages, −stage_depth_of_failure)", FEATURES_B),
    ("Model C (−total_stages, −stage_depth, −peak_memory_ratio)", FEATURES_C),
]

# Train/test split (same seed as original)
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
train_df.cache()
test_df.cache()
train_count = train_df.count()
test_count = test_df.count()
print(f"\n  Train: {train_count}, Test: {test_count}")

results = {}

for model_name, feature_cols in configs:
    print(f"\n{'─' * 80}")
    print(f"  {model_name} ({len(feature_cols)} features)")
    print(f"{'─' * 80}")

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100, maxDepth=10, seed=42)
    pipeline = Pipeline(stages=[assembler, scaler, rf])

    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    metrics = {}
    for metric_name, key in [("accuracy","accuracy"),("f1","f1"),
                              ("weightedPrecision","precision"),("weightedRecall","recall")]:
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName=metric_name
        )
        metrics[key] = evaluator.evaluate(predictions)

    results[model_name] = metrics

    print(f"  Accuracy:  {metrics['accuracy']:.4f}")
    print(f"  F1:        {metrics['f1']:.4f}")
    print(f"  Precision: {metrics['precision']:.4f}")
    print(f"  Recall:    {metrics['recall']:.4f}")

    # Per-class accuracy
    print(f"\n  Per-class accuracy:")
    total_per_class = predictions.groupBy("label").count().collect()
    correct_per_class = predictions.filter(col("prediction") == col("label")).groupBy("label").count().collect()
    correct_dict = {row["label"]: row["count"] for row in correct_per_class}

    for row in sorted(total_per_class, key=lambda r: r["label"]):
        lbl = int(row["label"])
        tot = row["count"]
        cor = correct_dict.get(row["label"], 0)
        acc = cor / tot if tot > 0 else 0
        name = LABEL_NAMES.get(lbl, "?")
        status = "✅" if acc >= 0.5 else "❌"
        print(f"    {status} Label {lbl} ({name:<20}): {cor}/{tot} = {acc:.0%}")

    # Feature importance (top 10)
    importances = model.stages[-1].featureImportances.toArray()
    feat_imp = sorted(zip(feature_cols, importances), key=lambda x: x[1], reverse=True)
    print(f"\n  Top 10 Feature Importance:")
    for rank, (feat, imp) in enumerate(feat_imp[:10], 1):
        print(f"    {rank:>2}. {feat:<30} {imp:.4f}")

# ======================================================================
# SECTION 4: 5-FOLD CROSS-VALIDATION — ALL 3 MODELS
# ======================================================================
print("\n" + "=" * 90)
print("SECTION 4: 5-FOLD CROSS-VALIDATION — ALL 3 MODELS")
print("=" * 90)

cv_results = {}
for model_name, feature_cols in configs:
    print(f"\n  Running 5-fold CV for {model_name}...")
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100, maxDepth=10, seed=42)
    cv_pipeline = Pipeline(stages=[assembler, scaler, rf])

    paramGrid = ParamGridBuilder().build()
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    crossval = CrossValidator(
        estimator=cv_pipeline, estimatorParamMaps=paramGrid,
        evaluator=evaluator, numFolds=5, seed=42
    )
    cv_model = crossval.fit(df)
    cv_f1 = cv_model.avgMetrics[0]
    cv_results[model_name] = cv_f1
    print(f"  {model_name}: CV F1 = {cv_f1:.4f}")

# ======================================================================
# SECTION 5: COMPARISON TABLE
# ======================================================================
print("\n" + "=" * 90)
print("SECTION 5: FINAL COMPARISON TABLE")
print("=" * 90)

model_a_name = list(results.keys())[0]
model_a_acc = results[model_a_name]["accuracy"]
model_a_f1 = results[model_a_name]["f1"]

print(f"\n  {'Model':<55} {'Accuracy':>10} {'F1':>10} {'Precision':>10} {'Recall':>10} {'CV F1':>10} {'F1 Drop':>10}")
print(f"  {'─'*115}")

for model_name in results:
    m = results[model_name]
    cv = cv_results.get(model_name, 0)
    drop = m["f1"] - model_a_f1
    drop_str = f"{drop:+.4f}" if model_name != model_a_name else "baseline"
    print(f"  {model_name:<55} {m['accuracy']:>10.4f} {m['f1']:>10.4f} {m['precision']:>10.4f} {m['recall']:>10.4f} {cv:>10.4f} {drop_str:>10}")

print(f"\n  Performance from query fingerprints (A→C drop): {model_a_f1 - results[list(results.keys())[2]]['f1']:.4f}")
print(f"  Performance from genuine behavioral signals (Model C F1): {results[list(results.keys())[2]]['f1']:.4f}")
pct_confound = (model_a_f1 - results[list(results.keys())[2]]["f1"]) / model_a_f1 * 100
print(f"  Confound contribution: {pct_confound:.1f}% of total F1 comes from confounded features")

print("\n" + "=" * 90)
print("ANALYSIS COMPLETE")
print("=" * 90)

spark.stop()
