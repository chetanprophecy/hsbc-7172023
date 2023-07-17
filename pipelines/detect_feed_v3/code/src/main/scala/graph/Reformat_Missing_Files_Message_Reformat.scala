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

object Reformat_Missing_Files_Message_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(
        when(
          col("file_date") < date_format(
            to_date(lit(context.config.GRAPH_MANDATORY_FILESET_FROM_DATE),
                    "yyyyMMdd"
            ),
            "yyyyMMdd"
          ),
          concat(lit("Skipping source file: "),
                 col("source_fileset_entry_nm"),
                 lit(" ("),
                 col("fileset_entry_nm"),
                 lit(") for "),
                 col("file_date")
          )
        ).otherwise(
          concat(lit("Waiting on source file: "),
                 col("source_fileset_entry_nm"),
                 lit(" ("),
                 col("fileset_entry_nm"),
                 lit(") for "),
                 col("file_date")
          )
        ),
        lit(null).cast(StringType)
      ).as("missing_file")
    )

}
