package graph.Obtain_List_Of_Unprocessed_Directories

import io.prophecy.libs._
import graph.Obtain_List_Of_Unprocessed_Directories.config.Context
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object nrmlz_Directories {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out_obtain_list_of_unprocessed_directories__nrmlz_directories = in.normalize(
        lengthExpression = Some(size(col("vec_source_extract_instance_directory"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (element_at(col("vec_source_extract_instance_directory"), col("index") + lit(1)))
            .as("source_extract_instance_directory")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = out_obtain_list_of_unprocessed_directories__nrmlz_directories.select(
        (col("source_extract_instance_directory")).as("source_extract_instance_directory")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
