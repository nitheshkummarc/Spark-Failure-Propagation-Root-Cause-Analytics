package com.sparkrca.preprocessing

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import scala.collection.mutable

/**
 * DAG Builder: Reconstructs the Acyclic Dependency Graph from Spark event logs.
 * 
 * Uses parent_ids extracted from SparkListenerStageSubmitted events (via LogParser)
 * to build true parent-child dependency edges.
 * Falls back to heuristic sequential ordering ONLY when parent_ids data is absent.
 */
object DAGBuilder {

  /**
   * Represents a node in the DAG (a stage).
   */
  case class StageNode(
    stageId: Int,
    stageName: String,
    parentIds: Set[Int],
    childIds: mutable.Set[Int] = mutable.Set.empty,
    status: String = "UNKNOWN",
    failureReason: Option[String] = None
  ) {
    def isFailed: Boolean = status == "FAILED"
    def isCompleted: Boolean = status == "COMPLETED"
    def isSkipped: Boolean = status == "SKIPPED"
  }

  /**
   * Represents the complete DAG for a Spark application.
   */
  case class ExecutionDAG(
    appId: String,
    stages: Map[Int, StageNode],
    rootStageIds: Set[Int],    // Stages with no parents
    leafStageIds: Set[Int],    // Stages with no children
    usedTrueParentIds: Boolean = false  // Whether DAG was built from real parentIds
  ) {
    
    /**
     * Gets all failed stages in the DAG.
     */
    def failedStages: Seq[StageNode] = stages.values.filter(_.isFailed).toSeq
    
    /**
     * Gets the terminal (leaf) failed stage if any.
     */
    def terminalFailedStage: Option[StageNode] = {
      leafStageIds.flatMap(id => stages.get(id)).find(_.isFailed)
    }
    
    /**
     * Gets all parent stages for a given stage.
     */
    def getParents(stageId: Int): Set[StageNode] = {
      stages.get(stageId).map(_.parentIds).getOrElse(Set.empty)
        .flatMap(id => stages.get(id))
    }
    
    /**
     * Gets all child stages for a given stage.
     */
    def getChildren(stageId: Int): Set[StageNode] = {
      stages.get(stageId).map(_.childIds.toSet).getOrElse(Set.empty)
        .flatMap(id => stages.get(id))
    }
    
    /**
     * Gets all ancestor stages (transitive parents).
     */
    def getAncestors(stageId: Int): Set[Int] = {
      val ancestors = mutable.Set[Int]()
      val queue = mutable.Queue[Int]()
      
      stages.get(stageId).foreach(s => s.parentIds.foreach(queue.enqueue(_)))
      
      while (queue.nonEmpty) {
        val parentId = queue.dequeue()
        if (!ancestors.contains(parentId)) {
          ancestors.add(parentId)
          stages.get(parentId).foreach(p => p.parentIds.foreach(queue.enqueue(_)))
        }
      }
      
      ancestors.toSet
    }
    
    /**
     * Gets all descendant stages (transitive children).
     */
    def getDescendants(stageId: Int): Set[Int] = {
      val descendants = mutable.Set[Int]()
      val queue = mutable.Queue[Int]()
      
      stages.get(stageId).foreach(s => s.childIds.foreach(queue.enqueue(_)))
      
      while (queue.nonEmpty) {
        val childId = queue.dequeue()
        if (!descendants.contains(childId)) {
          descendants.add(childId)
          stages.get(childId).foreach(c => c.childIds.foreach(queue.enqueue(_)))
        }
      }
      
      descendants.toSet
    }
    
    /**
     * Validates that the DAG is acyclic.
     */
    def isAcyclic: Boolean = {
      val visited = mutable.Set[Int]()
      val recursionStack = mutable.Set[Int]()
      
      def hasCycle(stageId: Int): Boolean = {
        if (recursionStack.contains(stageId)) return true
        if (visited.contains(stageId)) return false
        
        visited.add(stageId)
        recursionStack.add(stageId)
        
        val hasCycleInChildren = stages.get(stageId) match {
          case Some(stage) => stage.childIds.exists(hasCycle)
          case None => false
        }
        
        recursionStack.remove(stageId)
        hasCycleInChildren
      }
      
      !stages.keys.exists(hasCycle)
    }
    
    /**
     * Gets the depth (longest path) of the DAG.
     */
    def depth: Int = {
      if (stages.isEmpty) return 0
      
      val memoized = mutable.Map[Int, Int]()
      
      def stageDepth(stageId: Int): Int = {
        memoized.getOrElseUpdate(stageId, {
          val parents = stages.get(stageId).map(_.parentIds).getOrElse(Set.empty)
          if (parents.isEmpty) 1
          else 1 + parents.map(stageDepth).max
        })
      }
      
      stages.keys.map(stageDepth).max
    }
    
    /**
     * Prints the DAG structure.
     */
    def printDAG(): Unit = {
      println("\n" + "=" * 60)
      println(s"EXECUTION DAG: $appId")
      println("=" * 60)
      println(s"Total Stages: ${stages.size}")
      println(s"Root Stages: ${rootStageIds.mkString(", ")}")
      println(s"Leaf Stages: ${leafStageIds.mkString(", ")}")
      println(s"DAG Depth: $depth")
      println(s"Is Acyclic: $isAcyclic")
      println(s"Used True ParentIds: $usedTrueParentIds")
      println("-" * 60)
      
      stages.values.toSeq.sortBy(_.stageId).foreach { stage =>
        val statusIcon = stage.status match {
          case "COMPLETED" => "✓"
          case "FAILED" => "✗"
          case "SKIPPED" => "○"
          case _ => "?"
        }
        val parents = if (stage.parentIds.isEmpty) "none" else stage.parentIds.mkString(",")
        val children = if (stage.childIds.isEmpty) "none" else stage.childIds.mkString(",")
        
        println(f"$statusIcon Stage ${stage.stageId}%3d [${stage.status}%-10s] Parents: $parents%-15s Children: $children")
        if (stage.failureReason.isDefined) {
          println(s"              └─ Failure: ${stage.failureReason.get.take(50)}...")
        }
      }
      println("=" * 60)
    }
  }

  // ============================================================================
  // DAG Construction Functions
  // ============================================================================

  /**
   * Builds the execution DAG from stage information DataFrame.
   * Uses true parent_ids from SparkListenerStageSubmitted when available.
   * Falls back to heuristic sequential ordering when parent_ids is absent or empty.
   * 
   * @param spark SparkSession
   * @param stageDf DataFrame with stage information (must include parent_ids column)
   * @param appId Application ID
   * @return ExecutionDAG
   */
  def buildDAG(spark: SparkSession, stageDf: DataFrame, appId: String): ExecutionDAG = {
    import spark.implicits._
    
    println(s"Building DAG for application: $appId")
    
    // Check if parent_ids column exists
    val hasParentIds = stageDf.columns.contains("parent_ids")
    
    // Collect stage information
    val selectCols = if (hasParentIds) {
      Seq("stage_id", "stage_name", "status", "failure_reason", "parent_ids")
    } else {
      Seq("stage_id", "stage_name", "status", "failure_reason")
    }
    
    val stageRows = stageDf
      .filter(col("app_id") === appId)
      .select(selectCols.map(col): _*)
      .distinct()
      .collect()
    
    // Build stage nodes
    val stageNodes = mutable.Map[Int, StageNode]()
    var anyTrueParentIds = false
    
    stageRows.foreach { row =>
      val stageId = row.getAs[Int]("stage_id")
      val stageName = Option(row.getAs[String]("stage_name")).getOrElse("")
      val status = Option(row.getAs[String]("status")).getOrElse("UNKNOWN")
      val failureReason = Option(row.getAs[String]("failure_reason"))
      
      // Extract true parent IDs from parsed StageSubmitted events
      val parentIds: Set[Int] = if (hasParentIds) {
        val raw = Option(row.getAs[String]("parent_ids")).getOrElse("")
        if (raw.nonEmpty) {
          val parsed = raw.split(",").flatMap(s => scala.util.Try(s.trim.toInt).toOption).toSet
          if (parsed.nonEmpty) anyTrueParentIds = true
          parsed
        } else {
          Set.empty[Int]
        }
      } else {
        Set.empty[Int]
      }
      
      stageNodes(stageId) = StageNode(stageId, stageName, parentIds, mutable.Set.empty, status, failureReason)
    }
    
    if (anyTrueParentIds) {
      // === TRUE DEPENDENCY MODE ===
      // Build child relationships from actual parent_ids
      println("  Using TRUE parent_ids from SparkListenerStageSubmitted")
      
      stageNodes.values.foreach { stage =>
        stage.parentIds.foreach { parentId =>
          stageNodes.get(parentId).foreach(_.childIds.add(stage.stageId))
        }
      }
      
      // Compute root stages: stages with no parents
      val rootStageIds = stageNodes.values.filter(_.parentIds.isEmpty).map(_.stageId).toSet
      
      // Compute leaf stages: stages with no children
      val leafStageIds = stageNodes.values.filter(_.childIds.isEmpty).map(_.stageId).toSet
      
      val dag = ExecutionDAG(appId, stageNodes.toMap, rootStageIds, leafStageIds, usedTrueParentIds = true)
      println(s"  Built TRUE DAG with ${stageNodes.size} stages, ${rootStageIds.size} roots, ${leafStageIds.size} leaves, depth ${dag.depth}")
      dag
      
    } else {
      // === FALLBACK HEURISTIC MODE ===
      // Build linear chain by stage ordering (original behavior)
      println("  WARNING: No parent_ids found — falling back to heuristic stage ordering")
      
      val sortedStageIds = stageNodes.keys.toSeq.sorted
      for (i <- 1 until sortedStageIds.length) {
        val childId = sortedStageIds(i)
        val parentId = sortedStageIds(i - 1)
        stageNodes.get(childId).foreach { child =>
          val updatedParents = child.parentIds + parentId
          stageNodes(childId) = child.copy(parentIds = updatedParents)
        }
        stageNodes.get(parentId).foreach(_.childIds.add(childId))
      }
      
      val rootStageIds = if (sortedStageIds.nonEmpty) Set(sortedStageIds.head) else Set.empty[Int]
      val leafStageIds = if (sortedStageIds.nonEmpty) Set(sortedStageIds.last) else Set.empty[Int]
      
      val dag = ExecutionDAG(appId, stageNodes.toMap, rootStageIds, leafStageIds, usedTrueParentIds = false)
      println(s"  Built HEURISTIC DAG with ${stageNodes.size} stages, depth ${dag.depth}")
      dag
    }
  }

  /**
    * Builds DAGs for all applications in the stage DataFrame.
    * Collects all data once and groups by app_id in memory to avoid
    * per-app .filter().collect() which creates thousands of shuffle tasks.
    */
  def buildAllDAGs(spark: SparkSession, stageDf: DataFrame): Map[String, ExecutionDAG] = {
    import spark.implicits._
    
    val hasParentIds = stageDf.columns.contains("parent_ids")
    
    val selectCols = if (hasParentIds) {
      Seq("app_id", "stage_id", "stage_name", "status", "failure_reason", "parent_ids")
    } else {
      Seq("app_id", "stage_id", "stage_name", "status", "failure_reason")
    }
    
    // Single collect — all stages for all apps at once
    val allRows = stageDf.select(selectCols.map(col): _*).distinct().collect()
    
    // Group by app_id in memory
    val rowsByApp = allRows.groupBy(_.getAs[String]("app_id"))
    
    println(s"Building DAGs for ${rowsByApp.size} applications (${allRows.length} total stage rows)")
    
    rowsByApp.map { case (appId, rows) =>
      appId -> buildDAGFromRows(appId, rows, hasParentIds)
    }
  }
  
  /**
   * Builds a DAG from pre-collected Row data (no Spark operations needed).
   */
  private def buildDAGFromRows(appId: String, stageRows: Array[Row], hasParentIds: Boolean): ExecutionDAG = {
    val stageNodes = mutable.Map[Int, StageNode]()
    var anyTrueParentIds = false
    
    stageRows.foreach { row =>
      val stageId = row.getAs[Int]("stage_id")
      val stageName = Option(row.getAs[String]("stage_name")).getOrElse("")
      val status = Option(row.getAs[String]("status")).getOrElse("UNKNOWN")
      val failureReason = Option(row.getAs[String]("failure_reason"))
      
      val parentIds: Set[Int] = if (hasParentIds) {
        val raw = Option(row.getAs[String]("parent_ids")).getOrElse("")
        if (raw.nonEmpty) {
          val parsed = raw.split(",").flatMap(s => scala.util.Try(s.trim.toInt).toOption).toSet
          if (parsed.nonEmpty) anyTrueParentIds = true
          parsed
        } else Set.empty[Int]
      } else Set.empty[Int]
      
      stageNodes(stageId) = StageNode(stageId, stageName, parentIds, mutable.Set.empty, status, failureReason)
    }
    
    if (stageNodes.isEmpty) {
      return ExecutionDAG(appId, Map.empty, Set.empty, Set.empty, usedTrueParentIds = false)
    }
    
    // Wire up child pointers from parent pointers
    stageNodes.values.foreach { node =>
      node.parentIds.foreach { parentId =>
        stageNodes.get(parentId).foreach(_.childIds.add(node.stageId))
      }
    }
    
    if (anyTrueParentIds) {
      val rootStageIds = stageNodes.values.filter(_.parentIds.isEmpty).map(_.stageId).toSet
      val leafStageIds = stageNodes.values.filter(_.childIds.isEmpty).map(_.stageId).toSet
      val dag = ExecutionDAG(appId, stageNodes.toMap, rootStageIds, leafStageIds, usedTrueParentIds = true)
      println(s"  Built TRUE DAG for $appId: ${stageNodes.size} stages, depth ${dag.depth}")
      dag
    } else {
      // Fallback: heuristic sequential ordering
      val sortedStageIds = stageNodes.keys.toSeq.sorted
      for (i <- 1 until sortedStageIds.length) {
        val childId = sortedStageIds(i)
        val parentId = sortedStageIds(i - 1)
        stageNodes.get(childId).foreach { child =>
          val updatedParents = child.parentIds + parentId
          stageNodes(childId) = child.copy(parentIds = updatedParents)
        }
        stageNodes.get(parentId).foreach(_.childIds.add(childId))
      }
      
      val rootStageIds = if (sortedStageIds.nonEmpty) Set(sortedStageIds.head) else Set.empty[Int]
      val leafStageIds = if (sortedStageIds.nonEmpty) Set(sortedStageIds.last) else Set.empty[Int]
      
      val dag = ExecutionDAG(appId, stageNodes.toMap, rootStageIds, leafStageIds, usedTrueParentIds = false)
      println(s"  Built HEURISTIC DAG for $appId: ${stageNodes.size} stages, depth ${dag.depth}")
      dag
    }
  }

  /**
   * Converts DAG edges to DataFrame for visualization or analysis.
   */
  def dagToEdgesDataFrame(spark: SparkSession, dag: ExecutionDAG): DataFrame = {
    import spark.implicits._
    
    val edges = dag.stages.values.flatMap { stage =>
      stage.parentIds.map(parentId => (dag.appId, parentId, stage.stageId))
    }.toSeq
    
    edges.toDF("app_id", "parent_stage_id", "child_stage_id")
  }
}
