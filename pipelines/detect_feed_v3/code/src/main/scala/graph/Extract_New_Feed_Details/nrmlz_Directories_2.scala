package graph.Extract_New_Feed_Details

import io.prophecy.libs._
import graph.Extract_New_Feed_Details.config.Context
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object nrmlz_Directories_2 {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out_extract_new_feed_details__nrmlz_directories = in.normalize(
        lengthExpression = Some(size(col("vec_source_extract_file_directories"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (element_at(col("vec_source_extract_file_directories"), col("index") + lit(1)))
            .as("source_extract_file_directory"),
          (element_at(col("vec_source_extract_file_names"), col("index") + lit(1))).as("source_extract_file_name"),
          (regexp_replace(
            element_at(
              slice(split(element_at(col("vec_source_extract_file_directories"), col("index") + lit(1)), "/"), -2, 1),
              lit(1)
            ),
            "\\.db",
            ""
          )).as("source_db_name")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = out_extract_new_feed_details__nrmlz_directories.select(
        (col("file_date")).as("file_date"),
        (col("source_extract_file_directory")).as("source_extract_file_directory"),
        string_substring(col("source_extract_file_name"),lit(1), string_rindex(col("source_extract_file_name"),lit("."))-lit(1)).as("source_extract_file_name"),
        (col("source_db_name")).as("source_db_name")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
