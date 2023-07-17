package graph.Extract_New_Feed_Details.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Extract_New_Feed_Details.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object FBE_Strip_Out_Feeds_Later_Than_Any_Empty_Feed_ss {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(col("file_date") < col("file_date_of_empty_feed")),
     in.filter(!(col("file_date") < col("file_date_of_empty_feed")))
    )

}
