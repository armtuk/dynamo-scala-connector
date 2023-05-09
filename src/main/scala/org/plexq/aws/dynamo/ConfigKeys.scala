package org.plexq.aws.dynamo

object ConfigKeys {

  object AWS {
    val prefix = "aws"
    val region: String = prefix + ".region";

    val env: String = prefix + ".env";

    object Dynamo {
      val prefix: String = ConfigKeys.AWS.prefix + ".dynamo"
      val endpoint: String = prefix + ".endpoint";
      val listTableLimit: String = prefix + ".listTablesLimit"
      val tableName: String = prefix + ".tableName"
      val bootstrapPath: String = prefix + ".bootstrapPath"
    }
  }
}
