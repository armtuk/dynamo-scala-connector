package org.plexq.aws.dynamo

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, QuerySpec}
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.model._
import com.typesafe.config.Config
import org.plexq.aws.dynamo.Dynamo._
import org.plexq.aws.dynamo.DynamoRepository.DynamoKeyValue
import play.api.libs.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsResultException, JsString, JsValue}

import java.time.format.DateTimeFormatter
import java.util
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters.{IterableHasAsJava, IteratorHasAsScala, MapHasAsScala}
import scala.language.implicitConversions
import scala.util.control.NonFatal

class Dynamo(config: Config, var testMode: Boolean = false,
                       implicit val ec: ExecutionContext) extends Logging {

  val client = if (config.hasPath(ConfigKeys.AWS.Dynamo.endpoint)) {
    AmazonDynamoDBClientBuilder.standard()
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.getString(ConfigKeys.AWS.Dynamo.endpoint), config.getString(ConfigKeys.AWS.region)))
      .build();
  }
  else {
    AmazonDynamoDBClientBuilder.standard().build();
  }

  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // Quoted "Z" to indicate UTC, no timezone offset

  val dynamoDB = new DynamoDB(client);
  val tableName: String = config.getString(ConfigKeys.AWS.Dynamo.tableName)
  val table: Table = dynamoDB.getTable(tableName)
  val pkIndex: Index = table.getIndex(HK)
  val skIndex: Index = table.getIndex(SK)
  val gsi1Index: Index = table.getIndex(GSI_1_IndexName)
  val gsi2Index: Index = table.getIndex(GSI_2_IndexName)
  val gsi3Index: Index = table.getIndex(GSI_3_IndexName)
  val gsi4Index: Index = table.getIndex(GSI_4_IndexName)

  ensureTable()

  def ensureTable(): Unit = {
    try {
      client.describeTable(tableName).getTable
    }
    catch {
      case e: ResourceNotFoundException => createTable()
      case e: Exception => {
          println(e)
          throw e
      }
    }
  }

  def createTable(): Unit = {
    logger.info("Creating missing table " + tableName)

    val x = new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(
        util.Arrays.asList(
          new KeySchemaElement(HK, KeyType.HASH),
          new KeySchemaElement(SK, KeyType.RANGE),
        )
      )
      .withAttributeDefinitions(
        util.Arrays.asList(
          new AttributeDefinition(HK, "S"),
          new AttributeDefinition(SK, "S"),
          new AttributeDefinition(GSI_1_HK, "S"),
          new AttributeDefinition(GSI_1_SK, "S"),
          new AttributeDefinition(GSI_2_HK, "S"),
          new AttributeDefinition(GSI_2_SK, "S"),
          new AttributeDefinition(GSI_3_HK, "S"),
          new AttributeDefinition(GSI_3_SK, "S"),
          new AttributeDefinition(GSI_4_HK, "S"),
          new AttributeDefinition(GSI_4_SK, "S"),
        )
      )
      .withGlobalSecondaryIndexes(
        new GlobalSecondaryIndex().withIndexName(GSI_1_IndexName)
          .withKeySchema(util.Arrays.asList(
            new KeySchemaElement(GSI_1_HK, KeyType.HASH),
            new KeySchemaElement(GSI_1_SK, KeyType.RANGE)
          ))
          .withProjection(new Projection().withProjectionType("ALL"))
      )
      .withGlobalSecondaryIndexes(
        new GlobalSecondaryIndex().withIndexName(GSI_2_IndexName)
          .withKeySchema(util.Arrays.asList(
            new KeySchemaElement(GSI_2_HK, KeyType.HASH),
            new KeySchemaElement(GSI_2_SK, KeyType.RANGE)
          ))
          .withProjection(new Projection().withProjectionType("ALL"))
      )
      .withGlobalSecondaryIndexes(
        new GlobalSecondaryIndex().withIndexName(GSI_3_IndexName)
          .withKeySchema(util.Arrays.asList(
            new KeySchemaElement(GSI_3_HK, KeyType.HASH),
            new KeySchemaElement(GSI_3_SK, KeyType.RANGE)
          ))
          .withProjection(new Projection().withProjectionType("ALL"))
      )
      .withGlobalSecondaryIndexes(
        new GlobalSecondaryIndex().withIndexName(GSI_4_IndexName)
          .withKeySchema(util.Arrays.asList(
            new KeySchemaElement(GSI_4_HK, KeyType.HASH),
            new KeySchemaElement(GSI_4_SK, KeyType.RANGE)
          ))
          .withProjection(new Projection().withProjectionType("ALL"))
      )
      .withBillingMode(BillingMode.PAY_PER_REQUEST)

    dynamoDB.createTable(x)
  }

  def putItem(item: Item): Future[PutItemOutcome] = {
    Future { blocking {
      try {
        Future.successful(dynamoDB.getTable(tableName).putItem(item))
      }
      catch {
        case NonFatal(e) => {
            Future.failed(e)
          }
      }
    }}.flatten
  }

  // TODO Consider making this an adapter type thingie so we could pass either play-json or gson
  def putItem(json: JsObject): Future[PutItemOutcome] = {
    putItem(json2Item(json))
  }

  def getItem(spec: GetItemSpec) = {
    dynamoDB.getTable(tableName)
      .getItem(spec.withConsistentRead(testMode))
  }

  def delete(pk: String, sk: String) = {
    client.deleteItem(
      new DeleteItemRequest(tableName, java.util.Map.of(HK, new AttributeValue(pk), SK, new AttributeValue(sk)))
    )
  }

  def putItem[T](t: T)(implicit ev: T => Item): Future[T] = {
    try {
      //Thread.currentThread().getStackTrace.foreach(println)
      //logger.info("Dynamo putting " + ev(t))
      putItem(ev(t)).map(_ => t)
    }
    catch {
      case NonFatal(e) => {
        Future.failed(e)
      }
    }
  }

  def deleteItem[T](hkey: DynamoKeyValue, skey: DynamoKeyValue)(implicit ev: Item => T): Future[DeleteItemResult] = {
    Future { blocking {
      delete(hkey, skey)
    }}
  }

  def updateItem(pk: String, sk: String)(f: Item => Item): Future[Option[Item]] = {
    Future { blocking {
        Option(getItem(new GetItemSpec()
          .withPrimaryKey(new PrimaryKey(HK, pk, SK, sk))
          .withConsistentRead(testMode)
        ))
      }}.flatMap {
        case Some(x) => {
          val newValue: Item = f(x)
          putItem(newValue).map(x => Some(newValue))
        }
        case _ => {
          Future.successful(None)
        }
      }
  }

  def readByHashKey[T](rawPKey: String): Future[Seq[Item]] = {
    Future { blocking {
      val q = new QuerySpec().withKeyConditionExpression("%s = :pkValue".format(Dynamo.HK))
        .withValueMap(new ValueMap().withString(":pkValue", rawPKey))
        .withConsistentRead(testMode)

      table.query(q).pages().iterator().asScala.flatMap(_.iterator().asScala).toSeq
    }}
  }

  def readByKey[T](hashKey: String, sortKey: String)(ev: Item => T) = {
    Future { blocking {
      Option(table.getItem(Dynamo.HK, hashKey, Dynamo.SK, sortKey)).map(ev)
    }}
  }

  def queryIndexBySpec[T](index: Index, spec: QuerySpec)(implicit ev: Item => T): Future[Seq[T]] = {
    logger.info("Querying index by " + spec.getValueMap.asScala)
    Future { blocking {
      val k = index.query(spec).pages().iterator().asScala.toList

      k.flatMap(it => it.iterator().asScala.map(x => {
        try {
          ev(x)
        }
        catch {
          case e: JsResultException => {
            logger.error(s"Failed to parse record ${x}")
            throw e
          }
        }
      })).toSeq
    }}
  }
}

object Dynamo {
  val HK = "HashKey"
  val SK = "SortKey"

  val GSI_1_IndexName = "GSI1"
  val GSI_1_HK = "GSI_1_HK"
  val GSI_1_SK = "GSI_1_SK"

  val GSI_2_IndexName = "GSI2"
  val GSI_2_HK = "GSI_2_HK"
  val GSI_2_SK = "GSI_2_SK"

  val GSI_3_IndexName = "GSI3"
  val GSI_3_HK = "GSI_3_HK"
  val GSI_3_SK = "GSI_3_SK"

  val GSI_4_IndexName = "GSI4"
  val GSI_4_HK = "GSI_4_HK"
  val GSI_4_SK = "GSI_4_SK"

  val isoDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

  def json2Item(json: JsObject): Item = json.value.map {
    case (k: String, v: JsValue ) if jsvalue2Value.isDefinedAt(v) => k -> jsvalue2Value(v)
    case (k: String, v: JsArray) => v.value.headOption match {
      case Some(x) => {
        val g: (String,  Any) = x match {
          case _: JsString => (k, v.value.map(_.as[String]).toSeq.asJavaList)
          case JsNumber(n) if (n.rounded ==  n) => k -> (v.value.map(_.as[Long]).toSeq.asJavaList)
          case _: JsNumber => k -> (v.value.map(_.as[Double]).toSeq.asJavaList)
          case _: JsBoolean => k -> (v.value.map(_.as[Boolean]).toSeq.asJavaList)
          case JsNull => (k -> null)
        }
        g
      }
      case None => k -> java.util.Collections.emptyList()
    }
    case (k: String, JsObject(o)) => {
      println("Found object " + o)
      val r = o.map(e => e._1 -> jsvalue2Value(e._2)).asJavaMap
      println("Converting to " + r)
      k -> r
    }
  }.foldLeft(new Item())((a, b) => {
    try {
      a.`with`(b._1, b._2)
    }
    catch {
      case NonFatal(e) => {
        println(s"Failed to add ${b._1} -> ${b._2} to item ${a}")
        throw e
      }
    }
  })

  def jsArray2ItemElement(json: JsArray) = {
    json.value.headOption match {
      case Some(v) => v match {
        case _: JsString => json.value.map(_.as[String]).toSeq.asJavaList
      }
      case _ => null
    }
  }

  class JavaListWrapper[T](o: Seq[T]) {
    def asJavaList: util.List[T] = util.Arrays.asList(o:_*)
  }

  implicit def toJavaListWrapper[T](v: Seq[T]): JavaListWrapper[T] = new JavaListWrapper[T](v)

  class JavaMapWrapper[K, V](o: Iterable[(K, V)]) {
    def asJavaMap: util.Map[K, V] = util.Map.ofEntries(
      o.map(e => java.util.Map.entry(e._1, e._2)).toSeq:_*
    )
  }

  implicit def toJavaMapWrapper[K, V](v: Iterable[(K, V)]): JavaMapWrapper[K, V] = new JavaMapWrapper[K, V](v)

  val jsvalue2Value: PartialFunction[JsValue, Any] = {
    case JsString(s) => s
    case JsNumber(n) if (n.rounded == n) => n.longValue
    case JsNumber(n) => n.doubleValue
    case JsBoolean(b) => b
    case JsNull => null
  }

  def id = UUID.randomUUID().toString
}