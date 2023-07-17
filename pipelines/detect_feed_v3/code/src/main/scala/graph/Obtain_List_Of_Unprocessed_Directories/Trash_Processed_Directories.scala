package graph.Obtain_List_Of_Unprocessed_Directories

import io.prophecy.libs._
import graph.Obtain_List_Of_Unprocessed_Directories.config.Context
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Trash_Processed_Directories {
  def apply(context: Context, in: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    in.count()
  }

}
