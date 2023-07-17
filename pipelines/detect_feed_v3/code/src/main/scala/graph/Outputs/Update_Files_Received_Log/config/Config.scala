package graph.Outputs.Update_Files_Received_Log.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source.config.{
  Config => listing_of_all_files_received_from_source_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  listing_of_all_files_received_from_source: listing_of_all_files_received_from_source_Config =
    listing_of_all_files_received_from_source_Config()
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
