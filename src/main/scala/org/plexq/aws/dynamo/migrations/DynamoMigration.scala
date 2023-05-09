package org.plexq.aws.dynamo.migrations

import org.plexq.aws.dynamo.Dynamo
import play.api.libs.json.{JsObject, Reads}

import javax.inject.Inject
import scala.concurrent.ExecutionContext

trait DynamoMigration {
  def transform(): Reads[JsObject]
}

abstract class StandardDynamoMigration @Inject()(dynamo: Dynamo,
                                                implicit val executionContext: ExecutionContext
                                                ) extends DynamoMigration {

}
