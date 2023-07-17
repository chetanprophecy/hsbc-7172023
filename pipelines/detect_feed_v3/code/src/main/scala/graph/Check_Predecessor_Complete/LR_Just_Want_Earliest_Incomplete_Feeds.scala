package graph.Check_Predecessor_Complete

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Check_Predecessor_Complete.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object LR_Just_Want_Earliest_Incomplete_Feeds {
  def apply(context: Context, in: DataFrame): DataFrame = in.limit(1)
}
