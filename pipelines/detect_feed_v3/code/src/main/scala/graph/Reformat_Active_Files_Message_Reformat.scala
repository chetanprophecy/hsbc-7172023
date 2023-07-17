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

object Reformat_Active_Files_Message_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(
        when(
          col("seconds_since_directory_updated") < lit("900"),
          concat(lit("Source File is not stable (Possibly Still Extracting): "),
                 col("source_extract_file_name"),
                 lit(" for "),
                 col("file_date")
          )
        ).otherwise(
          when(
            col("size").cast(LongType) === lit(0),
            concat(lit("Source File has no data (Possibly Still Extracting): "),
                   col("source_extract_file_name"),
                   lit(" for "),
                   col("file_date")
            )
          )
        ),
        lit(null).cast(StringType)
      ).as("missing_file")
    )

}
