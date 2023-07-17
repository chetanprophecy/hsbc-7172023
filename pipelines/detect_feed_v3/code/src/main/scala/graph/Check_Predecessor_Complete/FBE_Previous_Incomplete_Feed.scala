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

object FBE_Previous_Incomplete_Feed {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(col("feed_date") < col("earliest_incomplete_feed_date")),
     in.filter(!(col("feed_date") < col("earliest_incomplete_feed_date")))
    )

}
