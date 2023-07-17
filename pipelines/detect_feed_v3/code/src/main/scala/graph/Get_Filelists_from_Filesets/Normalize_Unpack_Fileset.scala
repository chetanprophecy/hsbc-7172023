package graph.Get_Filelists_from_Filesets

import io.prophecy.libs._
import graph.Get_Filelists_from_Filesets.config.Context
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Normalize_Unpack_Fileset {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out_get_filelists_from_filesets__normalize_unpack_fileset = in.normalize(
        lengthExpression = Some(size(col("fileset_entry"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List((element_at(col("fileset_entry"), col("index") + lit(1))).as("fileset_entry")),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = out_get_filelists_from_filesets__normalize_unpack_fileset.select(
        (col("fileset_entry")).as("fileset_entry"),
        (col("file_date")).as("file_date"),
        (col("source_extract_instance_directory")).as("source_extract_instance_directory"),
        (col("feed_date")).as("feed_date"),
        (array().cast(ArrayType(StringType, true))).as("vec_source_partition_files")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
