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

object Join_Establish_If_all_Expected_Files_Are_in_Place_Reformat_0 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("file_date"),
      col("file_date").as("file_date_0"),
      col("source_extract_file_directory"),
      col("source_extract_file_directory")
        .as("source_extract_file_directory_0"),
      col("source_extract_file_name"),
      col("source_extract_file_name").as("source_extract_file_name_0"),
      col("size").cast(LongType).as("size"),
      col("size").cast(LongType).as("size_0"),
      col("empty_file_allowed"),
      col("empty_file_allowed").as("empty_file_allowed_0"),
      col("source_db_name"),
      col("source_db_name").as("source_db_name_0"),
      col("seconds_since_directory_updated"),
      col("seconds_since_directory_updated")
        .cast(StringType)
        .as("seconds_since_directory_updated_0")
    )

}
