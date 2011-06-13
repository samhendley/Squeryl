package org.squeryl.test

import java.util.UUID

import org.squeryl._
import org.squeryl.framework.{RunTestsInsideTransaction, SchemaTester}

import org.squeryl.PrimitiveTypeMode._

object SubQueryTestSchema{
  class Entity(
    val name: String) extends KeyedEntity[UUID] {
    var id: UUID = new UUID(0,0)
  }

  class EntityToTypeJoins(
    val entityId: UUID,
    val entType: String) {}

  class EntityEdge(
      val parentId: UUID,
      val childId: UUID,
      val relationship: String,
      val distance: Int) extends KeyedEntity[Long]{
    var id: Long = 0

  }

  object TestSchema extends Schema {
    val entity = table[Entity]
    val entityType = table[EntityToTypeJoins]
    val entityEdges = table[EntityEdge]

    override def drop = {
      Session.cleanupResources
      super.drop
    }
  }
}

abstract class SubQueryTests extends SchemaTester with RunTestsInsideTransaction{
  import SubQueryTestSchema._

  final def schema = TestSchema

  test("Missing internal state, cant copy") {
    import TestSchema._

    val name = "llll"
    val typeName = "mmmm"
    val relType = "owns"

    val nameQuery = from(entity)(e => where(e.name === name)select(e))
    val nameQueryId = from(nameQuery)(i => select(i.id))

    val typeQuery = from(entityType)((eType) => where(eType.entType === typeName) select(eType.entityId))

    val entEdges = from(entity, entityEdges)((e, edge) => where((e.id === edge.childId) and (edge.parentId in nameQueryId) and (e.id in typeQuery) and (edge.relationship === relType)) select(e, edge))

    val blowup = from(entEdges)(ee => select(ee._1))
    /*
    java.util.NoSuchElementException: None.get
	at scala.None$.get(Option.scala:185)
	at scala.None$.get(Option.scala:183)
	at org.squeryl.dsl.ast.ExportedSelectElement.innerTarget(SelectElement.scala:369)
	at org.squeryl.dsl.ast.ExportedSelectElement.needsOuterScope(SelectElement.scala:353)
	at org.squeryl.dsl.ast.NestedExpression$$anonfun$propagateOuterScope$1.gd1$1(ExpressionNode.scala:604)
	at org.squeryl.dsl.ast.NestedExpression$$anonfun$propagateOuterScope$1.apply(ExpressionNode.scala:603)
	at org.squeryl.dsl.ast.NestedExpression$$anonfun$propagateOuterScope$1.apply(ExpressionNode.scala:602)
	at org.squeryl.dsl.ast.ExpressionNode$class.org$squeryl$dsl$ast$ExpressionNode$$_visitDescendants(ExpressionNode.scala:54)
	at org.squeryl.dsl.ast.ExpressionNode$$anonfun$org$squeryl$dsl$ast$ExpressionNode$$_visitDescendants$1.apply(ExpressionNode.scala:55)
	at org.squeryl.dsl.ast.ExpressionNode$$anonfun$org$squeryl$dsl$ast$ExpressionNode$$_visitDescendants$1.apply(ExpressionNode.scala:55)
	at scala.collection.LinearSeqOptimized$class.foreach(LinearSeqOptimized.scala:61)
	at scala.collection.immutable.List.foreach(List.scala:45)
	at org.squeryl.dsl.ast.ExpressionNode$class.org$squeryl$dsl$ast$ExpressionNode$$_visitDescendants(ExpressionNode.scala:55)
	at org.squeryl.dsl.ast.ExpressionNode$$anonfun$org$squeryl$dsl$ast$ExpressionNode$$_visitDescendants$1.apply(ExpressionNode.scala:55)
	at org.squeryl.dsl.ast.ExpressionNode$$anonfun$org$squeryl$dsl$ast$ExpressionNode$$_visitDescendants$1.apply(ExpressionNode.scala:55)
	at scala.collection.LinearSeqOptimized$class.foreach(LinearSeqOptimized.scala:61)
	at scala.collection.immutable.List.foreach(List.scala:45)
	at org.squeryl.dsl.ast.ExpressionNode$class.org$squeryl$dsl$ast$ExpressionNode$$_visitDescendants(ExpressionNode.scala:55)
	at org.squeryl.dsl.ast.ExpressionNode$class.visitDescendants(ExpressionNode.scala:84)
	at org.squeryl.dsl.ast.RightHandSideOfIn.visitDescendants(ExpressionNode.scala:571)
	at org.squeryl.dsl.ast.NestedExpression$class.propagateOuterScope(ExpressionNode.scala:602)
	at org.squeryl.dsl.ast.RightHandSideOfIn.propagateOuterScope(ExpressionNode.scala:571)
	at org.squeryl.dsl.ast.QueryExpressionNode$$anonfun$propagateOuterScope$1.apply(QueryExpressionNode.scala:120)
	at org.squeryl.dsl.ast.QueryExpressionNode$$anonfun$propagateOuterScope$1.apply(QueryExpressionNode.scala:120)
	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:57)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:43)
	at org.squeryl.dsl.ast.QueryExpressionNode.propagateOuterScope(QueryExpressionNode.scala:120)
	at org.squeryl.dsl.AbstractQuery.buildAst(AbstractQuery.scala:102)
	at org.squeryl.dsl.boilerplate.Query2.<init>(Query1.scala:53)
	at org.squeryl.dsl.boilerplate.Query2.createCopy(Query1.scala:46)
	at org.squeryl.dsl.boilerplate.Query2.createCopy(Query1.scala:38)
	at org.squeryl.dsl.AbstractQuery.copy(AbstractQuery.scala:114)
	at org.squeryl.dsl.AbstractQuery.createSubQueryable(AbstractQuery.scala:250)
	at org.squeryl.dsl.boilerplate.Query1.<init>(Query1.scala:27)
	at org.squeryl.dsl.boilerplate.FromSignatures$class.from(FromSignatures.scala:25)
	at org.squeryl.PrimitiveTypeMode$.from(PrimitiveTypeMode.scala:40)
     */
  }

  test("Badly formatted from statement") {
    import TestSchema._

    val name = "llll"
    val typeName = "mmmm"
    val relType = "owns"

    val nameQuery = from(entity)(e => where(e.name === name)select(e.name, e.id))
    // notice we are getting part of a tuple, only difference from previous query
    val nameQueryId = from(nameQuery)(i => select(i._2))

    val typeQuery = from(entityType)((eType) => where(eType.entType === typeName) select(eType.entityId))

    val entEdges = from(entity, entityEdges)((e, edge) => where((e.id === edge.childId) and (edge.parentId in nameQueryId) and (e.id in typeQuery) and (edge.relationship === relType)) select(e, edge))

    entEdges.toList
    /*
    Select
  Entity8.name as Entity8_name,
  Entity8.id as Entity8_id,
  EntityEdge9.childId as EntityEdge9_childId,
  EntityEdge9.relationship as EntityEdge9_relationship,
  EntityEdge9.id as EntityEdge9_id,
  EntityEdge9.parentId as EntityEdge9_parentId,
  EntityEdge9.distance as EntityEdge9_distance
From
  Entity Entity8,
  EntityEdge EntityEdge9
Where
  ((((Entity8.id = EntityEdge9.childId) and (EntityEdge9.parentId in ((Select
                                                                    <------------ Missing id here
   From
     (Select
        Entity14.name as Entity14_name,
        Entity14.id as Entity14_id
      From
        Entity Entity14
      Where
        (Entity14.name = ?)
     )  q11
  ) ))) and (Entity8.id in ((Select
     EntityToTypeJoins17.entityId as EntityToTypeJoins17_entityId
   From
     EntityToTypeJoins EntityToTypeJoins17
   Where
     (EntityToTypeJoins17.entType = ?)
  ) ))) and (EntityEdge9.relationship = ?))
     */
  }

}