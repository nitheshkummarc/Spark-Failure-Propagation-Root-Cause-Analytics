package com.sparkrca.preprocessing

import org.apache.spark.sql.{SparkSession, DataFrame}
import scala.collection.mutable

/**
 * Propagation Analyzer: The "Brain" of the RCA system.
 * Uses Reverse Breadth-First Search (BFS) to identify the Root Cause Stage.
 */
object PropagationAnalyzer {

  /**
   * Result of root cause analysis.
   */
  case class RootCauseResult(
    appId: String,
    rootCauseStageIds: Set[Int],
    rootCauseStageNames: String,
    failureReasons: String,
    propagationPath: Seq[Int],    // Path from root cause to terminal failure
    victimStages: Set[Int],       // Stages that failed due to propagation
    analysisConfidence: Double    // Confidence in the analysis (0-1)
  ) {
    
    def printSummary(): Unit = {
      println("\n" + "=" * 60)
      println("ROOT CAUSE ANALYSIS RESULT")
      println("=" * 60)
      println(s"Application: $appId")
      println(s"Root Cause Stages: ${rootCauseStageIds.mkString(", ")} ($rootCauseStageNames)")
      println(s"Failure Reasons: $failureReasons")
      println(s"Propagation Path: ${propagationPath.mkString(" → ")}")
      println(s"Victim Stages: ${victimStages.mkString(", ")}")
      println(f"Analysis Confidence: ${analysisConfidence * 100}%.1f%%")
      println("=" * 60)
    }
  }

  // ============================================================================
  // Core Analysis Algorithm
  // ============================================================================

  /**
   * Identifies the Root Cause Stage using Reverse BFS.
   * 
   * Algorithm:
   * 1. Start at Terminal Failed Stage
   * 2. Traverse to Parent Stages
   * 3. If a Parent failed, current stage is a "Victim" - move upstream
   * 4. If no parents failed (or parents are successful), Current Stage = Root Cause
   * 
   * Refinement: Logical aborts (cancelled/skipped stages) are excluded.
   * 
   * @param dag The execution DAG
   * @return RootCauseResult — always returns a result (NO_FAILURE for healthy apps)
   */
   
  def analyzeRootCause(dag: DAGBuilder.ExecutionDAG): RootCauseResult = {
    
    println(s"\n>>> Analyzing root cause for: ${dag.appId}")
    
    // Find failed stages
    val failedStages = dag.failedStages
    
    if (failedStages.isEmpty) {
      println("No failed stages found - job completed successfully")
      // Return a default result using the leaf stage (healthy app)
      val leafStageId = dag.leafStageIds.headOption.getOrElse(
        dag.stages.keys.toSeq.sorted.lastOption.getOrElse(0)
      )
      val leafStage = dag.stages.getOrElse(leafStageId,
        DAGBuilder.StageNode(leafStageId, "Unknown", Set.empty))
      return RootCauseResult(
        appId = dag.appId,
        rootCauseStageIds = Set(leafStage.stageId),
        rootCauseStageNames = leafStage.stageName,
        failureReasons = "NO_FAILURE",
        propagationPath = Seq(leafStage.stageId),
        victimStages = Set.empty,
        analysisConfidence = 1.0
      )
    }
    
    println(s"Found ${failedStages.size} failed stage(s)")
    
    // Find terminal failed stage (leaf stage that failed, or most downstream failed stage)
    val terminalFailedStage = findTerminalFailedStage(dag, failedStages)
    
    if (terminalFailedStage.isEmpty) {
      println("Could not identify terminal failed stage")
      // Fallback: use first failed stage
      val fallback = failedStages.head
      return RootCauseResult(
        appId = dag.appId,
        rootCauseStageIds = Set(fallback.stageId),
        rootCauseStageNames = fallback.stageName,
        failureReasons = fallback.failureReason.getOrElse("Unknown"),
        propagationPath = Seq(fallback.stageId),
        victimStages = Set.empty,
        analysisConfidence = 0.3
      )
    }
    
    val terminal = terminalFailedStage.get
    println(s"Terminal failed stage: ${terminal.stageId}")
    
    // Reverse BFS to find root cause
    val (rootCauseStages, path, victims) = reverseBFS(dag, terminal.stageId)
    
    RootCauseResult(
      appId = dag.appId,
      rootCauseStageIds = rootCauseStages.map(_.stageId),
      rootCauseStageNames = rootCauseStages.map(_.stageName).mkString(" | "),
      failureReasons = rootCauseStages.map(_.failureReason.getOrElse("Unknown")).mkString(" | "),
      propagationPath = path,
      victimStages = victims,
      analysisConfidence = calculateConfidence(dag, rootCauseStages, path)
    )
  }

  /**
   * Finds the terminal (most downstream) failed stage.
   */
  private def findTerminalFailedStage(
    dag: DAGBuilder.ExecutionDAG,
    failedStages: Seq[DAGBuilder.StageNode]
  ): Option[DAGBuilder.StageNode] = {
    
    // Check leaf stages first
    val failedLeaves = failedStages.filter(s => dag.leafStageIds.contains(s.stageId))
    if (failedLeaves.nonEmpty) {
      return Some(failedLeaves.maxBy(_.stageId))
    }
    
    // Find the stage with no failed children (most downstream failure)
    // Sort by stageId descending for deterministic selection
    val terminalFailed = failedStages.sortBy(_.stageId).reverse.find { stage =>
      val children = dag.getChildren(stage.stageId)
      children.forall(!_.isFailed)
    }
    
    terminalFailed.orElse(failedStages.sortBy(_.stageId).reverse.headOption)
  }

  /**
   * Reverse BFS to trace failure back to root cause.
   * 
   * @param dag The execution DAG
   * @param startStageId The terminal failed stage ID
   * @return Tuple of (root cause stage, propagation path, victim stages)
   */
  private def reverseBFS(
    dag: DAGBuilder.ExecutionDAG,
    startStageId: Int
  ): (Set[DAGBuilder.StageNode], Seq[Int], Set[Int]) = {
    
    val visited = mutable.Set[Int]()
    val queue = mutable.Queue[Int](startStageId)
    val path = mutable.ListBuffer[Int](startStageId)
    val victims = mutable.Set[Int]()
    
    val rootCauseStageIds = mutable.Set[Int]()
    
    while (queue.nonEmpty) {
      val currentId = queue.dequeue()
      
      if (!visited.contains(currentId)) {
        visited.add(currentId)
        
        val currentStage = dag.stages.get(currentId)
        
        if (currentStage.isDefined && currentStage.get.isFailed) {
          // Get parent stages
          val parents = dag.getParents(currentId)
          
          // Filter to failed parents (excluding logical aborts)
          val failedParents = parents.filter { p =>
            p.isFailed && !isLogicalAbort(p)
          }
          
          if (failedParents.isEmpty) {
            // No failed parents → this is a ROOT CAUSE
            rootCauseStageIds.add(currentId)
            println(s"  Root cause identified: Stage $currentId (no failed parents)")
          } else {
            // Has failed parents → this stage is a VICTIM
            victims.add(currentId)
            println(s"  Stage $currentId is a victim (has ${failedParents.size} failed parent(s))")
            
            // Continue BFS to parents
            failedParents.foreach { parent =>
              if (!visited.contains(parent.stageId)) {
                queue.enqueue(parent.stageId)
                path.prepend(parent.stageId)
              }
            }
          }
        }
      }
    }
    
    val rootCauseStages = if (rootCauseStageIds.nonEmpty) {
      rootCauseStageIds.map(id => dag.stages.getOrElse(id, DAGBuilder.StageNode(id, "Unknown", Set.empty))).toSet
    } else {
      Set(dag.stages.getOrElse(startStageId, DAGBuilder.StageNode(startStageId, "Unknown", Set.empty)))
    }
    
    (rootCauseStages, path.toSeq, victims.toSet)
  }

  /**
   * Determines if a stage failure is a logical abort (cancelled, not a true failure).
   * Logical aborts should not be considered as root causes.
   */
  private def isLogicalAbort(stage: DAGBuilder.StageNode): Boolean = {
    stage.status == "SKIPPED" || 
    stage.failureReason.exists(reason =>
      reason.toLowerCase.contains("cancelled") ||
      reason.toLowerCase.contains("aborted") ||
      reason.toLowerCase.contains("stage was cancelled") ||
      reason.toLowerCase.contains("job was cancelled")
    )
  }

  /**
   * Calculates confidence score for the root cause analysis.
   * Higher confidence when:
   * - Clear propagation path
   * - Root cause has distinct failure reason
   * - No ambiguous multi-parent scenarios
   */
  private def calculateConfidence(
    dag: DAGBuilder.ExecutionDAG,
    rootCauseStages: Set[DAGBuilder.StageNode],
    path: Seq[Int]
  ): Double = {
    
    var confidence = 0.5  // Base confidence
    
    // Boost if any root cause has a clear failure reason
    if (rootCauseStages.exists(s => s.failureReason.isDefined && s.failureReason.get.length > 10)) {
      confidence += 0.2
    }
    
    // Boost if propagation path is clear (single parent chain)
    val pathStages = path.flatMap(id => dag.stages.get(id))
    if (pathStages.forall(_.parentIds.size <= 1)) {
      confidence += 0.15
    }
    
    // Boost if all parent stages of all root causes are successful
    val allRootParents = rootCauseStages.flatMap(s => dag.getParents(s.stageId))
    if (allRootParents.forall(_.isCompleted)) {
      confidence += 0.15
    }
    
    // Multiple true root causes is valid now, but we penalize if it's highly fragmented
    if (rootCauseStages.size > 2) {
      confidence -= 0.1 * (rootCauseStages.size - 2)
    }
    
    math.max(0.0, math.min(1.0, confidence))
  }

  // ============================================================================
  // Batch Analysis
  // ============================================================================

  /**
   * Analyzes root causes for all applications in the DAG collection.
   */
  def analyzeAllApplications(dags: Map[String, DAGBuilder.ExecutionDAG]): Seq[RootCauseResult] = {
    println(s"\n>>> Analyzing ${dags.size} applications")
    
    dags.values.map { dag =>
      analyzeRootCause(dag)
    }.toSeq
  }

  /**
   * Converts analysis results to DataFrame.
   */
  def resultsToDataFrame(spark: SparkSession, results: Seq[RootCauseResult]): DataFrame = {
    import spark.implicits._
    
    results.map { r =>
      (
        r.appId,
        r.rootCauseStageIds.mkString(","),
        r.rootCauseStageNames,
        r.failureReasons,
        r.propagationPath.mkString(","),
        r.victimStages.mkString(","),
        r.analysisConfidence
      )
    }.toDF(
      "app_id",
      "root_cause_stage_ids",
      "root_cause_stage_names",
      "failure_reasons",
      "propagation_path",
      "victim_stages",
      "analysis_confidence"
    )
  }
}
