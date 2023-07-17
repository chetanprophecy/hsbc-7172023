package graph.Extract_New_Feed_Details.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import graph.Extract_New_Feed_Details.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds.config.{
  Config => Strip_Out_Feeds_Later_Than_Any_Empty_Feeds_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  COMPONENT_SOURCE_DIRECTORY: String = "AA_WORLDCHECK_HSBC_PUB/dml/raw",
  FEED_ARRIVAL_PUB_SOURCE_LANDING_ROOT: String =
    "/Users/ashish/hsbc/abinitio-demo",
  FEED_ARRIVAL_PUB_DETECTION_METHOD: String = " ",
  Strip_Out_Feeds_Later_Than_Any_Empty_Feeds: Strip_Out_Feeds_Later_Than_Any_Empty_Feeds_Config =
    Strip_Out_Feeds_Later_Than_Any_Empty_Feeds_Config()
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
