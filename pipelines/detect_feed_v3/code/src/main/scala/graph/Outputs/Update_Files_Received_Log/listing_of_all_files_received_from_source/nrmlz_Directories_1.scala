package graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source

import io.prophecy.libs._
import graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source.config.Context
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object nrmlz_Directories_1 {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out_outputs__update_files_received_log__listing_of_all_files_received_from_source__nrmlz_directories =
        in.normalize(
          lengthExpression = Some(size(col("vec_source_extract_tables"))),
          finishedExpression = None,
          finishedCondition = None,
          alias = "index",
          colsToSelect = List((element_at(col("vec_source_extract_tables"), col("index") + lit(1))).as("file_name")),
          lengthRelatedGlobalExpressions = Map(),
          tempWindowExpr = Map()
        )
    
      val simpleSelect_in_DF =
        out_outputs__update_files_received_log__listing_of_all_files_received_from_source__nrmlz_directories.select(
          (col("file_name")).as("file_name"),
          (col("feed_date")).as("feed_date"),
          (lit(Config.FEED_ARRIVAL_PUB_COUNTRY)).as("country"),
          (lit(Config.FEED_ARRIVAL_PUB_LEGAL_ENTITY)).as("legal_entity"),
          (lit(Config.FEED_ARRIVAL_PUB_EXTRACT)).as("extract"),
          (lit(Config.FEED_ARRIVAL_PUB_SCHEDULE)).as("schedule"),
          (lit(Config.FEED_ARRIVAL_PUB_SOURCE)).as("source"),
          (col("file_date")).as("file_date")
        )
    
      val out = simpleSelect_in_DF
    out
  }

}
