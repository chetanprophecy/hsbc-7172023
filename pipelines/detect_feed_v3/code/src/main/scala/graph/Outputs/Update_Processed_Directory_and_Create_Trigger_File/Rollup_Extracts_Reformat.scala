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

object Rollup_Extracts_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("country"),
      col("legal_entity"),
      col("source"),
      col("extract"),
      col("schedule"),
      col("feed_date"),
      col("file_date"),
      col("fileset_name"),
      col("source_extract_instance_directory"),
      col("landing_trigger_file")
    )

}
