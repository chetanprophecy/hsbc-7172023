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

object Dedup_Sorted_File_Date {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.withColumn(
        "row_number",
        row_number().over(Window.partitionBy("file_date").orderBy(lit(1)))
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
