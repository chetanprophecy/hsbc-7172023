package graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File

import io.prophecy.libs._
import graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File.config.Context
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Multiple_Trigger_Files {
  def apply(context: Context, in: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    
      val withFileDF = in.withColumn(
        "fileName",
        concat(
          lit(Config.FEED_ARRIVAL_PUB_SERIAL_TEMP),
          lit("/ipr_dispatch/"),
          lit("ingest_"),
          lit(Config.FEED_ARRIVAL_PUB_COUNTRY),
          lit("_"),
          lit(Config.FEED_ARRIVAL_PUB_LEGAL_ENTITY),
          lit("_"),
          lit(Config.FEED_ARRIVAL_PUB_COUNTRY),
          lit("_"),
          lit(Config.FEED_ARRIVAL_PUB_SOURCE),
          lit("_"),
          lit(Config.FEED_ARRIVAL_PUB_LEGAL_ENTITY),
          lit("_"),
          lit(Config.FEED_ARRIVAL_PUB_EXTRACT),
          lit("_"),
          lit(Config.FEED_ARRIVAL_PUB_SCHEDULE),
          lit("_"),
          element_at(split(col("feed_date"), ":"), lit(2)),
          lit(".trigger")
        )
      )
    
      withFileDF.breakAndWriteDataFrameForOutputFile(
        List(
          "country",
          "legal_entity",
          "source",
          "extract",
          "schedule",
          "feed_date",
          "file_date",
          "fileset_name",
          "source_extract_instance_directory",
          "landing_trigger_file"
        ),
        "fileName",
        "csv",
        Some(",")
      )
  }

}
