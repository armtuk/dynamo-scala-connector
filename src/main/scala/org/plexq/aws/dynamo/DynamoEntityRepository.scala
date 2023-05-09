package org.plexq.aws.dynamo

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult

import scala.concurrent.Future

trait DynamoEntity {
  val hashKey: String
  val sortKey: String
}

trait DynamoEntityRepository[T <: DynamoEntity] extends DynamoRepository {

  def put(x: T)(implicit ev: T => Item): Future[T] = dynamo.putItem(x)

  def fetch(id: String)(implicit ev: Item => T): Future[Option[T]] = readByHashKey(id, ev).map(_.headOption)

  def delete(id: String)(implicit ev: Item => T): Future[Option[DeleteItemResult]] = {
    fetch(id).flatMap {
      case Some(e) => dynamo.deleteItem(e.hashKey, e.sortKey)(ev).map(Option.apply)
      case None => Future.successful(None)
    }
  }
}


