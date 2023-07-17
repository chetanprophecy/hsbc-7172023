package graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_One_directory_per_feed {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("source_extract_instance_directory"))
      .agg(max(col("source_extract_instance_directory")).as("directory_entry"))

}
