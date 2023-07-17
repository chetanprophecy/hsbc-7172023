package graph.Outputs.Update_Files_Received_Log

import io.prophecy.libs._
import graph.Outputs.Update_Files_Received_Log.config.Context
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Multipublish_Files_Received_Queue {
  def apply(context: Context, in: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    in.count()
  }

}
