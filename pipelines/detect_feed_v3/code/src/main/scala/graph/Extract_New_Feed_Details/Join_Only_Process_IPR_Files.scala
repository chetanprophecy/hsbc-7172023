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

object Join_Only_Process_IPR_Files {

  def apply(context: Context, right: DataFrame, left: DataFrame): DataFrame =
    right
      .as("right")
      .join(
        left.as("left"),
        (col("left.file_date") === col("right.file_date")).and(
          col("left.source_extract_file_name") === col(
            "right.source_fileset_entry_nm"
          )
        ),
        "inner"
      )
      .select(
        coalesce(col("right.file_date"), col("left.file_date")).as("file_date"),
        col("left.source_extract_file_directory")
          .as("source_extract_file_directory"),
        col("left.source_extract_file_name").as("source_extract_file_name"),
        lit(0).cast(LongType).as("size"),
        col("right.empty_file_allowed").as("empty_file_allowed"),
        col("left.source_db_name").as("source_db_name"),
        abs(
          unix_timestamp(
            date_format(
              to_timestamp(datetime_from_unixtime(
                             file_information(
                               col("left.source_extract_file_directory")
                             ).getField("modified")
                           ),
                           "yyyyMMddHHmmssSSSSSS"
              ),
              "yyyy-MM-dd HH:mm:ss"
            )
          ) - unix_timestamp()
        ).cast(StringType).as("seconds_since_directory_updated")
      )

}
