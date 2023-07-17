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

object jn_Identifiy_Unprocessed_Directories_Filter {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      !col("source_extract_instance_directory_0").isNull
        .and(!col("directory_entry_1").isNull)
    )

}
