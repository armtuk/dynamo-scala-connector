package org.plexq.aws.dynamo

import com.google.inject.AbstractModule
import com.typesafe.config.{Config, ConfigFactory}
import net.codingwell.scalaguice.ScalaModule

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global


class DynamoConnectorModule extends AbstractModule with ScalaModule {
  val config = ConfigFactory.load()

  override def configure(): Unit = {
    bind[Config].toInstance(config)
    bind[ExecutionContext].toInstance(global)
    bind[Dynamo].toInstance(new Dynamo(config, true, global))
  }
}
