package graph.Extract_New_Feed_Details.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Extract_New_Feed_Details.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Format_Log_Record_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lit("+++++++++++++ Begin Feed Deselected Due To Empty Feed +++++++")
        .as("header"),
      string_representation(
        struct(col("file_date"), col("file_date_of_empty_feed"))
      ).as("deselected_feeds"),
      lit("+++++++++++++ End Feed Deselected Due To Empty Feed +++++++").as(
        "trailer"
      )
    )

}
