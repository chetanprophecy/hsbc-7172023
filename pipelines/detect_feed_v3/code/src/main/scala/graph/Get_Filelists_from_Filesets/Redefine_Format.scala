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

object Redefine_Format {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("file_date"),
      col("source_extract_instance_directory"),
      col("feed_date"),
      col("vec_source_partition_files"),
      element_at(split(col("fileset_entry"), ","), 1).as("fileset_entry_nm"),
      element_at(split(col("fileset_entry"), ","), 2).as("cleanse_condition"),
      element_at(split(col("fileset_entry"), ","), 3).as("copy_to_ipr"),
      element_at(split(col("fileset_entry"), ","), 4).as("reject_ramp"),
      element_at(split(col("fileset_entry"), ","), 5).as("classification"),
      element_at(split(col("fileset_entry"), ","), 6).as("security_type"),
      element_at(split(col("fileset_entry"), ","), 7).as("table_type"),
      element_at(split(col("fileset_entry"), ","), 8).as("partition_key"),
      element_at(split(col("fileset_entry"), ","), 9).as("table_key"),
      element_at(split(col("fileset_entry"), ","), 10).as("sort_key"),
      element_at(split(col("fileset_entry"), ","), 11)
        .as("merge_with_previous"),
      element_at(split(col("fileset_entry"), ","), 12)
        .as("merge_with_previous_enrich"),
      element_at(split(col("fileset_entry"), ","), 13).as("partition_dedup"),
      element_at(split(col("fileset_entry"), ","), 14).as("dedup_internal"),
      element_at(split(col("fileset_entry"), ","), 15).as("jurisdiction_group"),
      element_at(split(col("fileset_entry"), ","), 16)
        .as("masked_classification"),
      element_at(split(col("fileset_entry"), ","), 17)
        .as("masked_security_type"),
      element_at(split(col("fileset_entry"), ","), 18)
        .as("large_jurisdiction_list"),
      element_at(split(col("fileset_entry"), ","), 19)
        .as("size_classification"),
      element_at(split(col("fileset_entry"), ","), 20)
        .as("source_fileset_entry_nm"),
      element_at(split(col("fileset_entry"), ","), 21).as("check_volume"),
      element_at(split(col("fileset_entry"), ","), 22).as("reject_limit"),
      element_at(split(col("fileset_entry"), ","), 23).as("empty_file_allowed"),
      element_at(split(col("fileset_entry"), ","), 24)
        .as("exclude_from_global_views"),
      trim(element_at(split(col("fileset_entry"), ","), 25)).as("spare1")
    )

}
