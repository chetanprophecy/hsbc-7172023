package graph.Get_Filelists_from_Filesets

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Get_Filelists_from_Filesets.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object FB_Only_Keep_Files_Going_To_IPR {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(col("copy_to_ipr") === lit("Y"))

}
