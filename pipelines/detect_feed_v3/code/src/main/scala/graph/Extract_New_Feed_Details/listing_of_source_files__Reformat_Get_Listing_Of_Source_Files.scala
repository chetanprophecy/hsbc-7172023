package graph.Extract_New_Feed_Details

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Extract_New_Feed_Details.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object listing_of_source_files__Reformat_Get_Listing_Of_Source_Files {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      col("file_date"),
      filter(
        transform(
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
          ),
          ii => ii
        ),
        xx => !xx.isNull
      ).as("vec_source_extract_file_names"),
      filter(
        transform(
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
          ),
          ii =>
            concat(
              lit(""),
              lit(Config.FEED_ARRIVAL_PUB_SOURCE_LANDING_ROOT),
              lit("/"),
              lit(Config.COMPONENT_SOURCE_DIRECTORY),
              lit("/"),
              col("source_extract_instance_directory"),
              lit("/"),
              ii
            )
        ),
        xx => !xx.isNull
      ).as("vec_source_extract_file_directories")
    )
  }

}
