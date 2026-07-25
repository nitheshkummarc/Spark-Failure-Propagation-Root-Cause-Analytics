package com.sparkrca.preprocessing

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.sparkrca.preprocessing.DAGBuilder.{StageNode, ExecutionDAG}
import scala.collection.mutable

class PropagationAnalyzerTest extends AnyFunSuite with Matchers {

  test("analyzeRootCause should correctly identify multiple independent root causes") {
    // Synthetic DAG:
    // Stage 1 (Failed, Root Cause A)
    // Stage 2 (Failed, Root Cause B)
    // Stage 3 (Failed, Victim) depends on Stage 1 and Stage 2
    
    val stage1 = StageNode(stageId = 1, stageName = "Read Data A", parentIds = Set.empty, childIds = mutable.Set(3), status = "FAILED", failureReason = Some("Timeout A"))
    val stage2 = StageNode(stageId = 2, stageName = "Read Data B", parentIds = Set.empty, childIds = mutable.Set(3), status = "FAILED", failureReason = Some("Timeout B"))
    val stage3 = StageNode(stageId = 3, stageName = "Join A and B", parentIds = Set(1, 2), childIds = mutable.Set.empty, status = "FAILED", failureReason = Some("Fetch Failed (Victim)"))
    
    val stages = Map(
      1 -> stage1,
      2 -> stage2,
      3 -> stage3
    )
    
    val dag = ExecutionDAG(
      appId = "app_test_multiple_roots",
      stages = stages,
      rootStageIds = Set(1, 2),
      leafStageIds = Set(3),
      usedTrueParentIds = true
    )
    
    val result = PropagationAnalyzer.analyzeRootCause(dag)
    
    // Both stage 1 and 2 failed independently and have no failed parents
    result.rootCauseStageIds should contain theSameElementsAs Set(1, 2)
    
    // Stage 3 is a victim because it failed, but its parents also failed
    result.victimStages should contain (3)
    result.propagationPath should contain (3)
  }

  test("analyzeRootCause should trace back through multiple victims to a single root cause") {
    // Stage 1 (Completed)
    // Stage 2 (Failed, Root Cause)
    // Stage 3 (Failed, Victim) depends on 2
    // Stage 4 (Failed, Victim) depends on 3
    
    val stage1 = StageNode(stageId = 1, stageName = "S1", parentIds = Set.empty, childIds = mutable.Set(2), status = "COMPLETED", failureReason = None)
    val stage2 = StageNode(stageId = 2, stageName = "S2", parentIds = Set(1), childIds = mutable.Set(3), status = "FAILED", failureReason = Some("OOM"))
    val stage3 = StageNode(stageId = 3, stageName = "S3", parentIds = Set(2), childIds = mutable.Set(4), status = "FAILED", failureReason = Some("Fetch Failed"))
    val stage4 = StageNode(stageId = 4, stageName = "S4", parentIds = Set(3), childIds = mutable.Set.empty, status = "FAILED", failureReason = Some("Fetch Failed"))
    
    val stages = Map(1 -> stage1, 2 -> stage2, 3 -> stage3, 4 -> stage4)
    val dag = ExecutionDAG("app_single_root", stages, Set(1), Set(4), usedTrueParentIds = true)
    
    val result = PropagationAnalyzer.analyzeRootCause(dag)
    
    result.rootCauseStageIds should contain theSameElementsAs Set(2)
    result.victimStages should contain theSameElementsAs Set(3, 4)
    result.propagationPath should contain inOrder (2, 3, 4)
  }
}
