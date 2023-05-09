package org.plexq.aws.dynamo

import com.amazonaws.services.dynamodbv2.document.Item

abstract class DynamoWrites[T] {
  import DynamoRepository.DynamoKeyValue
  def HK(item: T): DynamoKeyValue
  def SK(item: T): DynamoKeyValue

  def GSI1(item: T): (DynamoKeyValue, DynamoKeyValue) = (null, null)
  def GSI2(item: T): (DynamoKeyValue, DynamoKeyValue) = (null, null)
  def GSI3(item: T): (DynamoKeyValue, DynamoKeyValue) = (null, null)
  def GSI4(item: T): (DynamoKeyValue, DynamoKeyValue) = (null, null)

  val mappings:  Map[String, T => Any]

  def apply(value: T): Item = {
    (Map[String, T => Any](
      Dynamo.HK -> HK,
      Dynamo.SK -> SK,
      Dynamo.GSI_1_HK -> (x => GSI1(x)._1),
      Dynamo.GSI_1_SK -> (x => GSI1(x)._2),
      Dynamo.GSI_2_HK -> (x => GSI2(x)._1),
      Dynamo.GSI_2_SK -> (x => GSI2(x)._2),
      Dynamo.GSI_3_HK -> (x => GSI3(x)._1),
      Dynamo.GSI_3_SK -> (x => GSI3(x)._2),
      Dynamo.GSI_4_HK -> (x => GSI4(x)._1),
      Dynamo.GSI_4_SK -> (x => GSI4(x)._2)
    ) ++ mappings).foldLeft(new Item())((a, b) => if (b._2(value)!=null) a.`with`(b._1, b._2(value)) else a)
  }
}
