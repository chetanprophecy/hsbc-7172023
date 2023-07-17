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

object Sort {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(to_timestamp(col("feed_date"), "yyyyMMddHHmmss").asc)

}
