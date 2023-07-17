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

object Check_for_Data_in_Hadoop_Files_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("file_date"),
      col("source_extract_file_directory"),
      col("source_extract_file_name"),
      coalesce(
        aggregate(
          directory_listing(col("source_extract_file_directory"), lit("[!.]*")),
          lit(0).cast(LongType),
          (acc, ii) =>
            acc + file_information(
              concat(col("source_extract_file_directory"), lit("/"), ii)
            ).getField("size")
        ).cast(LongType),
        col("size").cast(LongType)
      ).as("size"),
      col("empty_file_allowed"),
      col("source_db_name"),
      col("seconds_since_directory_updated")
    )

}
