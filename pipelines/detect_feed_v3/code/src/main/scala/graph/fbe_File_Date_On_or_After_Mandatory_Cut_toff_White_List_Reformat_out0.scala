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

object fbe_File_Date_On_or_After_Mandatory_Cut_toff_White_List_Reformat_out0 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("file_date"))

}
