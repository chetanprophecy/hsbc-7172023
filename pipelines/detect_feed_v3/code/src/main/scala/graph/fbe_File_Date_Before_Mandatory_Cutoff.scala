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

object fbe_File_Date_Before_Mandatory_Cutoff {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      col("file_date") < lit(context.config.GRAPH_MANDATORY_FILESET_FROM_DATE)
    )

}
