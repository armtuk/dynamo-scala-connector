package org.plexq.aws.dynamo

import com.amazonaws.services.dynamodbv2.document.Item
import play.api.libs.json.{JsArray, JsNumber, Json}

import scala.concurrent.Await
import scala.concurrent.duration._

class DynamoSpec extends PlexqSpec {
  val dynamo = inject[Dynamo]

  case class SimpleTestValue(hashKey: String, sortKey: String)

  "Dynamo" should {
    "put a json to an item" in {
      val hkey = "DynamoSpec:"+System.currentTimeMillis()
      val t = Json.obj(Dynamo.HK -> hkey,
        "SortKey" -> "xyz",
        "a" -> "b")

      Await.ready(dynamo.putItem(t), 30.seconds)

      var item = Await.result(dynamo.readByHashKey(hkey), 30.seconds).head

      item.get(Dynamo.HK) must be (hkey)
      item.get(Dynamo.SK) must be ("xyz")
      item.get("a") must be ("b")
    }

    "put a json with an array to an item" in {
      val hkey = "DynamoSpec:" + System.currentTimeMillis()
      val t = Json.obj(Dynamo.HK -> hkey,
        "SortKey" -> "xyz",
        "a" -> Seq(1, 2, 3))

      Await.ready(dynamo.putItem(t), 30.seconds)

      var item = Await.result(dynamo.readByHashKey(hkey), 30.seconds).head

      val first = item.get("a").asInstanceOf[java.util.List[java.math.BigDecimal]].get(0)

      BigDecimal(first).intValue must be (1)
    }

    "put a json with an empty array to an item" in {
      val hkey = "DynamoSpec:" + System.currentTimeMillis()
      val t = Json.obj(Dynamo.HK -> hkey,
        "SortKey" -> "xyz",
        "a" -> Seq[Long]())

      Await.ready(dynamo.putItem(t), 30.seconds)

      var item = Await.result(dynamo.readByHashKey(hkey), 30.seconds).head

      item.get("a").asInstanceOf[java.util.List[java.math.BigDecimal]].size() must be (0)
    }

    "put a json with a map to an item" in {
      val hkey = "DynamoSpec:" + System.currentTimeMillis()
      val t = Json.obj(Dynamo.HK -> hkey,
        "SortKey" -> "xyz",
        "a" -> Map("x" -> "alpha", "y" -> "beta"))

      Await.ready(dynamo.putItem(t), 30.seconds)

      var item = Await.result(dynamo.readByHashKey(hkey), 30.seconds).head

      val r = item.getMap[String]("a").get("x")

      r must be ("alpha")
    }

    "put an item and read it back with a hash and sort key" in {
      val hkey = "DynamoSpec:" + System.currentTimeMillis()
      val skey = "xyz"
      val t = Json.obj(Dynamo.HK -> hkey, Dynamo.SK -> skey)

      Await.result(dynamo.putItem(t), 30.seconds)

      implicit val reader = (i: Item) => SimpleTestValue(i.getString(Dynamo.HK), i.getString(Dynamo.SK))

      val r = Await.result(dynamo.readByKey(hkey, skey)(reader), 30.seconds).get

      r.hashKey must be (hkey)
      r.sortKey must be (skey)
    }

    "try to read a missing item and get back a None" in {
      implicit val reader = (i: Item) => SimpleTestValue(i.getString(Dynamo.HK), i.getString(Dynamo.SK))

      val hkey = "DynamoSpec:" + System.currentTimeMillis()
      val skey = "xyz"

      val r = Await.result(dynamo.readByKey(hkey, skey)(reader), 30.seconds)

      r must be(None)
    }

  }
}