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

object Reformat_Read_Filesets_Assign_Feed_Date_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      string_split_no_empty(
        read_file(
          concat(
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
        ).cast(StringType),
        lit("""
""")
      ).as("fileset_entry"),
      col("file_date"),
      col("source_extract_instance_directory"),
      date_format(
        to_timestamp(
          datetime_add(
            date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSS")
              .cast(StringType),
            lit(0).cast(LongType),
            lit(0).cast(LongType),
            lit(0).cast(LongType),
            sum(lit(1)).over(windowSpec()).cast(LongType)
          ),
          "yyyyMMddHHmmssSSSSSS"
        ),
        "yyyyMMddHHmmss"
      ).as("feed_date")
    )
  }

}
