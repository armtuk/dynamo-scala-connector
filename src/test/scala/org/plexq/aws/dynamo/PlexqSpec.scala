package org.plexq.aws.dynamo

import com.google.inject.{Guice, Injector}
import org.scalatest.{MustMatchers, OptionValues, WordSpec}

class PlexqSpec extends WordSpec with Injecting with MustMatchers with OptionValues {
  var injector: Injector = Guice.createInjector(new DynamoConnectorModule)
}
