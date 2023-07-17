package graph.Obtain_List_Of_Unprocessed_Directories

import io.prophecy.libs._
import graph.Obtain_List_Of_Unprocessed_Directories.config.Context
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Create_Data {
  def apply(context: Context): DataFrame = {
    val spark = context.spark
    val Config = context.config
      lazy val arrayDF = generateDataFrameWithSequenceColumn(1, 1, "index", spark)
    
      lazy val out = arrayDF.select(
        directory_listing(
          concat(lit(""),
                 lit(Config.FEED_ARRIVAL_PUB_SOURCE_LANDING_ROOT),
                 lit("/"),
                 lit(Config.GRAPH_SOURCE_DIRECTORY)
          ),
          lit(Config.GRAPH_SOURCE_EXTRACT_DIRECTORY_PATTERN)
        ).as("vec_source_extract_instance_directory")
      )
    out
  }

}
