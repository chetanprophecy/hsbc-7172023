package graph.Extract_New_Feed_Details

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Extract_New_Feed_Details.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object FBE_Only_Want_Stable_Files_and_Files_With_Data {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) = {
    val Config = context.config
    (in.filter(
       (col("seconds_since_directory_updated").cast(LongType) > lit("900"))
         .or(
           coalesce(lit(Config.FEED_ARRIVAL_PUB_DETECTION_METHOD),
                    lit("")
           ) === lit("HANDSHAKE")
         )
         .and(
           (col("size").cast(LongType) =!= lit(0))
             .or(col("empty_file_allowed") =!= lit("N"))
         )
     ),
     in.filter(
       !(col("seconds_since_directory_updated").cast(LongType) > lit("900"))
         .or(
           coalesce(lit(Config.FEED_ARRIVAL_PUB_DETECTION_METHOD),
                    lit("")
           ) === lit("HANDSHAKE")
         )
         .and(
           (col("size").cast(LongType) =!= lit(0))
             .or(col("empty_file_allowed") =!= lit("N"))
         )
     )
    )
  }

}
