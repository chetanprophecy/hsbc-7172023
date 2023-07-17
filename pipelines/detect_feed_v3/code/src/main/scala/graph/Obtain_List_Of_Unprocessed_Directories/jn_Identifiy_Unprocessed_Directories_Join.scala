package graph.Obtain_List_Of_Unprocessed_Directories

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Obtain_List_Of_Unprocessed_Directories.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object jn_Identifiy_Unprocessed_Directories_Join {

  def apply(context: Context, right: DataFrame, left: DataFrame): DataFrame =
    right
      .as("right")
      .join(left.as("left"),
            col("left.source_extract_instance_directory") === col(
              "right.directory_entry"
            ),
            "outer"
      )
      .select(col("source_extract_instance_directory"),
              col("directory_entry_1"),
              col("source_extract_instance_directory_0")
      )

}
