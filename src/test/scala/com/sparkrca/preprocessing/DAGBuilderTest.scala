package com.sparkrca.preprocessing

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.sparkrca.preprocessing.DAGBuilder.{StageNode, ExecutionDAG}
import scala.collection.mutable

class DAGBuilderTest extends AnyFunSuite with Matchers {
  
  test("ExecutionDAG should accurately report depth and ancestors/descendants") {
    // Stage 1 -> Stage 2 -> Stage 3
    val stage1 = StageNode(1, "S1", Set.empty, mutable.Set(2))
    val stage2 = StageNode(2, "S2", Set(1), mutable.Set(3))
    val stage3 = StageNode(3, "S3", Set(2), mutable.Set.empty)
    
    val dag = ExecutionDAG(
      "app_depth_test",
      Map(1 -> stage1, 2 -> stage2, 3 -> stage3),
      Set(1),
      Set(3)
    )
    
    dag.depth shouldBe 3
    dag.getAncestors(3) should contain theSameElementsAs Set(1, 2)
    dag.getDescendants(1) should contain theSameElementsAs Set(2, 3)
    dag.isAcyclic shouldBe true
  }
  
  test("ExecutionDAG should detect cycles") {
    // Stage 1 -> Stage 2 -> Stage 1 (Cycle)
    val stage1 = StageNode(1, "S1", Set(2), mutable.Set(2))
    val stage2 = StageNode(2, "S2", Set(1), mutable.Set(1))
    
    val dag = ExecutionDAG(
      "app_cycle_test",
      Map(1 -> stage1, 2 -> stage2),
      Set.empty,
      Set.empty
    )
    
    dag.isAcyclic shouldBe false
  }
}
