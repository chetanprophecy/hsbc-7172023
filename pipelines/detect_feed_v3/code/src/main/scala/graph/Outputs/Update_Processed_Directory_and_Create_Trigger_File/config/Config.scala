package graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File.config

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
  GRAPH_CLASSIFICATION:                     String = " ",
  FEED_ARRIVAL_PUB_SERIAL_TEMP: String =
    "/Users/ashish/hsbc/abinitio-demo/fcr/cdf/FEED_ARRIVAL_PUB/temp",
  FEED_ARRIVAL_PUB_LANDING_TRIGGER_FILE: String = "PARAMETER_NOT_SET ",
  FEED_ARRIVAL_PUB_RAW_FEED_IDENTIFIER:  String = "scp_daily",
  FEED_ARRIVAL_PUB_COUNTRY:              String = "aa",
  FEED_ARRIVAL_PUB_SCHEDULE:             String = "daily",
  FEED_ARRIVAL_PUB_LEGAL_ENTITY:         String = "hsbc",
  FEED_ARRIVAL_PUB_SOURCE:               String = "worldcheck",
  FEED_ARRIVAL_PUB_SOURCE_PUBLIC_PROJECT: String =
    "/Users/ashish/hsbc/abinitio-demo/AA_WORLDCHECK_HSBC_PUB",
  GRAPH_SECURITY_TYPE:      String = " ",
  FEED_ARRIVAL_PUB_EXTRACT: String = "scp",
  GRAPH_FILE_FORMAT:        String = " "
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
