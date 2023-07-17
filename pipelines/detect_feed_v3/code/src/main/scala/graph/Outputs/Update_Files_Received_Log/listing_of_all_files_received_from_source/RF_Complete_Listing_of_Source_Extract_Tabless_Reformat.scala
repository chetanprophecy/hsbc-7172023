package graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object RF_Complete_Listing_of_Source_Extract_Tabless_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      directory_listing(
        concat(
          lit(""),
          lit(Config.FEED_ARRIVAL_PUB_SOURCE_LANDING_ROOT),
          lit("/"),
          lit(Config.COMPONENT_SOURCE_DIRECTORY),
          lit("/"),
          col("source_extract_instance_directory")
        ),
        lit("[!.]*")
      ).as("vec_source_extract_tables"),
      col("feed_date"),
      col("file_date")
    )
  }

}
