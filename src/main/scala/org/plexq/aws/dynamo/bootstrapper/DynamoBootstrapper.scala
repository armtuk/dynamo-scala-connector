package org.plexq.aws.dynamo.bootstrapper

import com.typesafe.config.Config
import org.plexq.aws.dynamo.Dynamo

import javax.inject._

@Singleton
class DynamoBootstrapper @Inject()(config: Config, dynamo: Dynamo) {
  /*
  val bootstrap = List.from(Paths.get(config.get[String](ConfigKeys.AWS.Dynamo.bootstrapPath)).toFile.listFiles())
    .filter(x => x.getName.endsWith(".json"))
    .map(_.toPath)
    .map(Files.readString)
    .map(Json.parse(_).as[JsObject])
    .foreach(x =>
      telemetryService.withInfo("DynamoBootstrapper/init", "putItem", "Bootstrapping element: %s".format((x \ Dynamo.PK).as[String])){
        dynamo.putItem(x)
      })

   */
}
