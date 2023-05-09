package org.plexq.aws.dynamo.migrations

import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.google.inject.Injector
import org.plexq.aws.dynamo.{Done, Dynamo}
import play.api.libs.json.{JsError, JsObject, JsResult, JsSuccess, Json}

import scala.concurrent.{ExecutionContext, Future}
import javax.inject.{Inject, Singleton}
import scala.jdk.CollectionConverters.IteratorHasAsScala

@Singleton
class DynamoMigrationsExecutor @Inject()(dynamo: Dynamo,
                                         injector: Injector,
                                         migrations: Seq[DynamoMigration],
                                         implicit val ec: ExecutionContext) {

  def apply(): Future[Iterator[Done.type]] =
    Future.sequence (
      dynamo.table.scan(new ScanSpec())
        .pages().iterator().asScala.flatMap(_.iterator().asScala)
        .map(item => Json.parse(item.toJSON).as[JsObject])
        .flatMap(x => migrate(x) map {
          case JsSuccess(value, _) => dynamo.putItem(value).map(_ => Done)
          case e: JsError => Future.successful(Done)
        })
    )

  def migrate(item: JsObject): Seq[JsResult[JsObject]] =
    migrations.map { migration =>
      item.transform(migration.transform()).map(_.as[JsObject])
    }
}