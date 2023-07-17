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

object Join_Strip_Out_Incomplete_or_Unstable_Feeds_Reformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("file_date"), col("file_date").as("file_date_1"))

}
