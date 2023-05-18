package org.plexq.aws.dynamo

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.typesafe.config.Config
import org.plexq.aws.dynamo.Dynamo.{GSI_1_HK, GSI_1_SK, GSI_2_HK, GSI_2_SK, GSI_3_HK, GSI_3_SK, isoDateFormatter}
import play.api.libs.json.{JsNull, JsPath, JsString, JsValue, Json, Writes}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsScala}
import scala.language.implicitConversions

// TODO this is probably actually almost entirely clientId AND clientCycleId.
// Whilst for the general security of the multi-homed application clientId is the guardrail, for practical purposes
// The real silo is by cycle.
// TODO when we come back around to turning this into a published entity, we should separate at that time to cycle Id.

trait DynamoRepository {
  import DynamoRepository._

  val dynamo: Dynamo
  val config: Config
  val tableName: String = config.getString(ConfigKeys.AWS.Dynamo.tableName)
  implicit val executionContext: ExecutionContext

  def readByKey[T](hashKey: String, sortKey: String)(implicit converter: Item => T): Future[Option[T]] =
    dynamo.readByKey(hashKey, sortKey)(converter)

  def readByHashKey[T](hashKey: String, converter: Item => T): Future[Seq[T]] =
    dynamo.readByHashKey(hashKey).map(_.map(converter))


  def putItem[T](t: T)(implicit ev: T => Item): Future[T] = dynamo.putItem(t).map(_ => t)

  def queryGsi1Hk[T](queryString: DynamoKeyValue)(implicit ev: Item => T): Future[Seq[T]] = {
    dynamo.queryIndexBySpec(dynamo.gsi1Index, new QuerySpec()
      .withKeyConditionExpression("%s=:gsi1_hk_expr".format(GSI_1_HK))
      .withValueMap(new ValueMap()
        .withString(":gsi1_hk_expr", queryString)))(ev)
  }

  def queryGsi1HkSk[T](hkString: DynamoKeyValue, skString: DynamoKeyValue)(implicit ev: Item => T): Future[Seq[T]] = {
    dynamo.queryIndexBySpec(dynamo.gsi1Index, new QuerySpec()
      .withKeyConditionExpression("%s=:gsi1_hk_expr and %s=:gsi1_sk_expr".format(GSI_1_HK, GSI_1_SK))
      .withValueMap(new ValueMap()
        .withString(":gsi1_hk_expr", hkString)
        .withString(":gsi1_sk_expr", skString)))(ev)
  }

  def queryGsi2Hk[T](queryString: DynamoKeyValue)(implicit ev: Item => T): Future[Seq[T]] = {
    dynamo.queryIndexBySpec(dynamo.gsi2Index, new QuerySpec()
      .withKeyConditionExpression("%s=:gsi2_hk_expr".format(GSI_2_HK))
      .withValueMap(new ValueMap()
        .withString(":gsi2_hk_expr", queryString)))(ev)
  }

  def queryGsi2HkSk[T](pkString: DynamoKeyValue, skString: DynamoKeyValue)(implicit ev: Item => T): Future[Seq[T]] = {
    dynamo.queryIndexBySpec(dynamo.gsi2Index, new QuerySpec()
      .withKeyConditionExpression("%s=:gsi2_pk_expr and %s=:gsi2_sk_expr".format(GSI_2_HK, GSI_2_SK))
      .withValueMap(new ValueMap()
        .withString(":gsi2_pk_expr", pkString)
        .withString(":gsi2_sk_expr", skString)))(ev)
  }

  def queryGsi3HkSk[T](pkString: DynamoKeyValue, skString: DynamoKeyValue)(implicit ev: Item => T): Future[Seq[T]] = {
    dynamo.queryIndexBySpec(dynamo.gsi3Index, new QuerySpec()
      .withKeyConditionExpression("%s=:gsi3_pk_expr and %s=:gsi3_sk_expr".format(GSI_3_HK, GSI_3_SK))
      .withValueMap(new ValueMap()
        .withString(":gsi3_pk_expr", pkString)
        .withString(":gsi3_sk_expr", skString)))(ev)
  }
}

object DynamoRepository {
  type DynamoKeyValue = String

  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

  val True = "true"
  val False = "false"

  implicit def itemToString(item: Item, name: String): String = item.getString(name)
  implicit def itemToBoolean(item: Item, name: String): Boolean = item.getBoolean(name)
  implicit def itemToLong(item: Item, name: String): Long = item.getLong(name)
  implicit def itemToInt(item: Item, name: String): Int = item.getInt(name)
  implicit def itemToBigDecimal(item: Item, name: String): BigDecimal = BigDecimal(item.getString(name))
  implicit def itemToJson(item: Item, name: String): JsValue = if (item.get(name) == null) JsNull else Json.parse{
    // Because if you call item.getJSON() it escapes everything.
    item.getString(name)
  }
  implicit def itemToList[T](item: Item, name: String): List[T] = {
    item.getList[T](name).asScala.toList
  }
  implicit def itemToMap[T](item: Item, name: String): Map[String, T] = {
    item.getMap[T](name).asScala.toMap
  }

  implicit def itemToTimestamp[T](item: Item, name: String): java.sql.Timestamp = stringToTimestamp(item.getString(name))
  implicit def itemToDate[T](item: Item, name: String): java.sql.Date= stringToDate(item.getString(name))

  implicit def stringToTimestamp(str: String): java.sql.Timestamp = {
    new java.sql.Timestamp(
      LocalDateTime.parse(str, dateFormatter).atOffset(ZoneOffset.UTC).toInstant.toEpochMilli
    )
  }

  implicit def stringToDate(str: String): java.sql.Date = {
    new java.sql.Date(
      LocalDateTime.parse(str, dateFormatter).atOffset(ZoneOffset.UTC).toInstant.toEpochMilli
    )
  }


  implicit def dateToString(date: java.sql.Date): String = {
    if (date!= null) {
      dateFormatter.format(Instant.ofEpochMilli(date.getTime).atOffset(ZoneOffset.UTC))
    } else null
  }

  implicit def timestampToString(timestamp: java.sql.Timestamp): String = {
    if (timestamp != null) {
      dateFormatter.format(Instant.ofEpochMilli(timestamp.getTime).atOffset(ZoneOffset.UTC))
    } else null
  }

  implicit def timestampToAny(timestamp: java.sql.Timestamp): Any = {
    if (timestamp != null) {
      timestampToString(timestamp)
    } else null
  }

  implicit val timestampReads = JsPath.read[String].map(x => OffsetDateTime.parse(x, isoDateFormatter)).map(x => new java.sql.Timestamp(x.toInstant.toEpochMilli))
    .map(x => new java.sql.Timestamp(x.toInstant.toEpochMilli))

  implicit val timestampWrites: Writes[java.sql.Timestamp] = (timestamp: java.sql.Timestamp) => if (timestamp != null) {
    JsString(timestampToString(timestamp))
  } else JsNull
}
