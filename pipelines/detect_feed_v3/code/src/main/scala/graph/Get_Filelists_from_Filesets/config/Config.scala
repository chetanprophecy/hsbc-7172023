package graph.Get_Filelists_from_Filesets.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  FEED_ARRIVAL_PUB_DEFAULT_FILESET_VERSION: String = "v0010",
  AI_PHASE:                                 String = "penv",
  FEED_ARRIVAL_PUB_RAW_FEED_IDENTIFIER:     String = "scp_daily",
  FEED_ARRIVAL_PUB_SOURCE:                  String = "worldcheck",
  FEED_ARRIVAL_PUB_SOURCE_PUBLIC_PROJECT: String =
    "/Users/ashish/hsbc/abinitio-demo/AA_WORLDCHECK_HSBC_PUB"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
