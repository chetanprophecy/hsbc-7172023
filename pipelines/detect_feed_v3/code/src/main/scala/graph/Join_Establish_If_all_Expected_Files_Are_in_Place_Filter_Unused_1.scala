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

object Join_Establish_If_all_Expected_Files_Are_in_Place_Filter_Unused_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      col("file_date_0").isNull
        .and(col("source_extract_file_directory_0").isNull)
        .and(col("source_extract_file_name_0").isNull)
        .and(col("size_0").isNull)
        .and(col("empty_file_allowed_0").isNull)
        .and(col("source_db_name_0").isNull)
        .and(col("seconds_since_directory_updated_0").isNull)
    )

}
