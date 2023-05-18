package org.plexq.aws.dynamo

import com.amazonaws.services.dynamodbv2.document.Item
import play.api.libs.json.JsValue

abstract class DynamoReads[A](implicit val value: Item) extends Logging {
  import DynamoRepository._

  def apply(): A
  def PK: String = value.getString(Dynamo.HK)
  def SK: String = value.getString(Dynamo.SK)

  def string(name: String): String = itemToString(value, name)
  def boolean(name: String): Boolean  = itemToBoolean(value, name)
  def long(name: String): Long = itemToLong(value, name)
  def int(name: String): Int = itemToInt(value, name)
  def bigDecimal(name: String): BigDecimal = itemToBigDecimal(value, name)
  def timestamp(name: String): java.sql.Timestamp = itemToTimestamp(value, name)
  def date(name: String): java.sql.Date= itemToDate(value, name)
  def list[T](name: String): List[T]= itemToList[T](value, name)
  def set[T](name: String): Set[T]= {
    val k = itemToList[T](value, name).toSet
    k
  }
  def map[T](name: String): Map[String, T] = itemToMap[T](value, name)

  def json(name: String): JsValue = {
    itemToJson(value, name)
  }

  def optJson(name: String): Option[JsValue] = {
    Option(value.get(name)).map(_ => itemToJson(value, name))
  }

  def opt[T](name: String)(implicit ev: (Item, String) => T): Option[T] = {
    try {
      Option(ev(value, name))
    } catch {
      case e: NumberFormatException => None
      case e: NullPointerException => None
    }
  }
}
