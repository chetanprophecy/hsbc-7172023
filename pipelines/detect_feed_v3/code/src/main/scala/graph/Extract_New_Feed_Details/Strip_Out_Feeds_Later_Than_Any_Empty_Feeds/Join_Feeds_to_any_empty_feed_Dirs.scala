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

object Join_Feeds_to_any_empty_feed_Dirs {

  def apply(context: Context, right: DataFrame, left: DataFrame): DataFrame =
    right
      .as("right")
      .join(
        left.as("left"),
        (col("left.file_date") === col("right.file_date"))
          .and(
            col("left.vec_source_extract_file_names") === col(
              "right.vec_source_extract_file_names"
            )
          )
          .and(
            col("left.vec_source_extract_file_directories") === col(
              "right.vec_source_extract_file_directories"
            )
          ),
        "right_outer"
      )
      .select(
        col("left.file_date").as("file_date"),
        coalesce(col("right.file_date"), lit("20990101"))
          .as("file_date_of_empty_feed"),
        col("left.vec_source_extract_file_names")
          .as("vec_source_extract_file_names"),
        col("left.vec_source_extract_file_directories")
          .as("vec_source_extract_file_directories")
      )

}
