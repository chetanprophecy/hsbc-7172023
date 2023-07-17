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

object Rollup_Incomplete_Source_Extracts_File_Datess {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("file_date")).agg(lit(1).as("1"))

}
