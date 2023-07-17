package graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_Extracts {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.groupBy(
        concat(lit("source_extract_instance_directory:"),
               col("source_extract_instance_directory")
        ).as("source_extract_instance_directory")
      )
      .agg(
        max(concat(lit("country:"), lit(Config.FEED_ARRIVAL_PUB_COUNTRY)))
          .as("country"),
        max(
          concat(lit("legal_entity:"),
                 lit(Config.FEED_ARRIVAL_PUB_LEGAL_ENTITY)
          )
        ).as("legal_entity"),
        max(concat(lit("source:"), lit(Config.FEED_ARRIVAL_PUB_SOURCE)))
          .as("source"),
        max(concat(lit("extract:"), lit(Config.FEED_ARRIVAL_PUB_EXTRACT)))
          .as("extract"),
        max(concat(lit("schedule:"), lit(Config.FEED_ARRIVAL_PUB_SCHEDULE)))
          .as("schedule"),
        max(concat(lit("feed_date:"), col("feed_date"))).as("feed_date"),
        max(concat(lit("file_date:"), col("file_date"))).as("file_date"),
        max(
          concat(
            lit("fileset_name:"),
            lit(Config.FEED_ARRIVAL_PUB_SOURCE_PUBLIC_PROJECT),
            lit("/fileset/"),
            lit(Config.FEED_ARRIVAL_PUB_SOURCE),
            lit("."),
            lit(Config.FEED_ARRIVAL_PUB_RAW_FEED_IDENTIFIER),
            lit(".fileset."),
            get_fileset_version_inner(
              col("file_date"),
              concat(lit(Config.FEED_ARRIVAL_PUB_SOURCE_PUBLIC_PROJECT),
                     lit("/configs/fileset_version_rules."),
                     lit(Config.AI_PHASE),
                     lit(".txt")
              ),
              lit(Config.FEED_ARRIVAL_PUB_DEFAULT_FILESET_VERSION),
              lit(Config.FEED_ARRIVAL_PUB_RAW_FEED_IDENTIFIER)
            ),
            lit(".txt")
          )
        ).as("fileset_name"),
        max(
          concat(lit("landing_trigger_file:"),
                 lit(Config.FEED_ARRIVAL_PUB_LANDING_TRIGGER_FILE)
          )
        ).as("landing_trigger_file")
      )
  }

}
