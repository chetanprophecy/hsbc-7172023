package graph.Obtain_List_Of_Unprocessed_Directories.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  GRAPH_SOURCE_EXTRACT_DIRECTORY_PATTERN: String = "[!.]*",
  GRAPH_SOURCE_DIRECTORY:                 String = "AA_WORLDCHECK_HSBC_PUB/dml/raw",
  FEED_ARRIVAL_PUB_SOURCE_LANDING_ROOT: String =
    "/Users/ashish/hsbc/abinitio-demo"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
