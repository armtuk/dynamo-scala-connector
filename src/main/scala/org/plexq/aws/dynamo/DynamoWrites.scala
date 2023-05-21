package org.plexq.aws.dynamo

import com.amazonaws.services.dynamodbv2.document.Item
import play.api.libs.json.{JsArray, JsObject, Json}

abstract class DynamoWrites[T <: DynamoEntity] {
  import DynamoRepository.DynamoKeyValue
  def HK(item: T): DynamoKeyValue = item.hashKey
  def SK(item: T): DynamoKeyValue = item.sortKey

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
    ) ++ mappings).foldLeft(new Item())((a, b) => if (b._2(value)!=null) {
      b._2(value) match {
        // Not sure if this is really a good idea... but I _think_ so.
        case v: BigDecimal => a.`with`(b._1, v.toString)
        // Serialize complex Json objects as strings here
        case v: JsArray => a.withString(b._1, Json.prettyPrint(v))
        case v: JsObject => a.withString(b._1, Json.prettyPrint(v))
        case _ => a.`with`(b._1, b._2(value))
      }
    } else a)
  }
}
