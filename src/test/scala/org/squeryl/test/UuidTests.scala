package org.squeryl.test

import org.squeryl._
import org.squeryl.framework.{SchemaTester, RunTestsInsideTransaction}
import java.util.UUID

import PrimitiveTypeMode._

object UuidTests {
  class UuidAsProperty extends KeyedEntity[Long] {
    val id: Long = 0
    val uuid = UUID.randomUUID
  }

  class UuidAsId extends KeyedEntity[UUID] {
    var id = UUID.randomUUID
    lazy val foreigns = TestSchema.uuidOneToMany.left(this)
  }

  class UuidAsForeignKey(val foreignUuid: UUID) extends KeyedEntity[Long] {
    val id: Long = 0
  }

  class UuidAsOption(val optionalUuid: Option[UUID], val optionalInt : Option[Int]) extends KeyedEntity[Long] {
    val id: Long = 0
  }

  object TestSchema extends Schema {
    val uuidAsProperty = table[UuidAsProperty]
    val uuidAsId = table[UuidAsId]
    val uuidAsForeignKey = table[UuidAsForeignKey]
    val uuidAsOption = table[UuidAsOption]

    val uuidOneToMany = oneToManyRelation(uuidAsId, uuidAsForeignKey).via(_.id === _.foreignUuid)

    override def drop = {
      Session.cleanupResources
      super.drop
    }
  }

}

abstract class UuidTests extends SchemaTester with RunTestsInsideTransaction{
  import UuidTests._

  final def schema = TestSchema

  test("UuidAsProperty") {
    import TestSchema._

    val testObject = new UuidAsProperty
    uuidAsProperty.insert(testObject)

    testObject.uuid should equal(uuidAsProperty.where(_.id === testObject.id).single.uuid)

    testObject.uuid should equal(uuidAsProperty.where(_.uuid in List(testObject.uuid)).single.uuid)
  }

  test("UuidAsId") {
    import TestSchema._

    val testObject = new UuidAsId
    uuidAsId.insert(testObject)

    testObject.id should equal(uuidAsId.where(_.id === testObject.id).single.id)

    testObject.id should equal(uuidAsId.where(_.id in List(testObject.id)).single.id)

    val lookup = uuidAsId.lookup(testObject.id)
    lookup.get.id should equal(testObject.id)
  }

  test("UuidAsForeignKey") {
    import TestSchema._

    val primaryObject = new UuidAsId
    uuidAsId.insert(primaryObject)

    val secondaryObject = new UuidAsForeignKey(primaryObject.id)
    uuidAsForeignKey.insert(secondaryObject)

    secondaryObject.id should equal(uuidAsForeignKey.where(_.id === secondaryObject.id).single.id)

    List(secondaryObject.id) should equal(primaryObject.foreigns.map(_.id).toList)

    val query = join(uuidAsId, uuidAsForeignKey.leftOuter)((e, f) =>
      where(e.id === primaryObject.id)
      select (e, f.map{_.id})
      on (Some(e.id) === f.map(_.foreignUuid)))

    query.toList.size should equal(1)

    val query2 = join(uuidAsId, uuidAsForeignKey.leftOuter)((e, f) =>
      where(e.id === primaryObject.id)
      select (e, f)
      on (Some(e.id) === f.map(_.foreignUuid)))

    query2.toList.size should equal(1)

  }

  test("UuidAsOption") {
    import TestSchema._

    val hasOption = new UuidAsOption(Some(new UUID(1,1)), None)
    uuidAsOption.insert(hasOption)

    val noOption = new UuidAsOption(None, Some(1))
    uuidAsOption.insert(noOption)

    hasOption.id should equal(uuidAsOption.where(_.optionalUuid === hasOption.optionalUuid).single.id)

    uuidAsOption.where(_.optionalUuid === Some(new UUID(2,2))).toList.size should equal(0)

    uuidAsOption.where(_.optionalInt === Some(2)).toList.size should equal(0)

    noOption.id should equal(uuidAsOption.where(_.optionalInt === noOption.optionalInt).single.id)

  }

}