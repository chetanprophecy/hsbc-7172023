package graph.Check_Predecessor_Complete

import io.prophecy.libs._
import graph.Check_Predecessor_Complete.config.Context
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Trash_Source_Directories_that_are_complete_but_have_an_incomplete_predecessor {
  def apply(context: Context, in: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    in.count()
  }

}
