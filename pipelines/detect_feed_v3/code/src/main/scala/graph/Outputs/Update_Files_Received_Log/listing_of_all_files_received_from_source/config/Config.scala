package graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  FEED_ARRIVAL_PUB_COUNTRY:      String = "aa",
  FEED_ARRIVAL_PUB_SCHEDULE:     String = "daily",
  FEED_ARRIVAL_PUB_LEGAL_ENTITY: String = "hsbc",
  FEED_ARRIVAL_PUB_SOURCE_LANDING_ROOT: String =
    "/Users/ashish/hsbc/abinitio-demo",
  FEED_ARRIVAL_PUB_SOURCE: String = "worldcheck",
  COMPONENT_SOURCE_DIRECTORY: String =
    "AA_WORLDCHECK_HSBC_PUB/dml/raw/trwc_emea_prod_20181027.db",
  FEED_ARRIVAL_PUB_EXTRACT: String = "scp"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
