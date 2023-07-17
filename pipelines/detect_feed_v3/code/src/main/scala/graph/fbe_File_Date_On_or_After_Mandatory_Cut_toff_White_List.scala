package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object fbe_File_Date_On_or_After_Mandatory_Cut_toff_White_List {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) = {
    val Config = context.config
    (in.filter(
       (col("file_date") >= lit(Config.GRAPH_MANDATORY_FILESET_FROM_DATE)).and(
         !lookup_match(
           "Incomplete_Feed_White_List",
           lit(Config.FEED_ARRIVAL_PUB_COUNTRY),
           lit(Config.FEED_ARRIVAL_PUB_LEGAL_ENTITY),
           lit(Config.FEED_ARRIVAL_PUB_SOURCE),
           lit(Config.FEED_ARRIVAL_PUB_EXTRACT),
           lit(Config.FEED_ARRIVAL_PUB_SCHEDULE),
           col("file_date")
         ).cast(BooleanType)
       )
     ),
     in.filter(
       !(col("file_date") >= lit(Config.GRAPH_MANDATORY_FILESET_FROM_DATE)).and(
         !lookup_match(
           "Incomplete_Feed_White_List",
           lit(Config.FEED_ARRIVAL_PUB_COUNTRY),
           lit(Config.FEED_ARRIVAL_PUB_LEGAL_ENTITY),
           lit(Config.FEED_ARRIVAL_PUB_SOURCE),
           lit(Config.FEED_ARRIVAL_PUB_EXTRACT),
           lit(Config.FEED_ARRIVAL_PUB_SCHEDULE),
           col("file_date")
         ).cast(BooleanType)
       )
     )
    )
  }

}
