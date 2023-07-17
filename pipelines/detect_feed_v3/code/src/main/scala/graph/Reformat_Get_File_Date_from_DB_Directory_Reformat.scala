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

object Reformat_Get_File_Date_from_DB_Directory_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      get_db_file_date(col("source_extract_instance_directory"),
                       lit(Config.GRAPH_FILE_DATE_FORMAT),
                       lit(Config.GRAPH_LANDING_STRUCTURE)
      ).as("file_date"),
      col("source_extract_instance_directory")
    )
  }

}
