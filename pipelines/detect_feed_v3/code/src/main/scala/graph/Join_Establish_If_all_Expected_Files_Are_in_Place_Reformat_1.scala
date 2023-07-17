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

object Join_Establish_If_all_Expected_Files_Are_in_Place_Reformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("fileset_entry_nm"),
      col("fileset_entry_nm").as("fileset_entry_nm_1"),
      col("cleanse_condition"),
      col("cleanse_condition").as("cleanse_condition_1"),
      col("copy_to_ipr"),
      col("copy_to_ipr").as("copy_to_ipr_1"),
      col("reject_ramp"),
      col("reject_ramp").as("reject_ramp_1"),
      col("classification"),
      col("classification").as("classification_1"),
      col("security_type"),
      col("security_type").as("security_type_1"),
      col("table_type"),
      col("table_type").as("table_type_1"),
      col("partition_key"),
      col("partition_key").as("partition_key_1"),
      col("table_key"),
      col("table_key").as("table_key_1"),
      col("sort_key"),
      col("sort_key").as("sort_key_1"),
      col("merge_with_previous"),
      col("merge_with_previous").as("merge_with_previous_1"),
      col("merge_with_previous_enrich"),
      col("merge_with_previous_enrich").as("merge_with_previous_enrich_1"),
      col("partition_dedup"),
      col("partition_dedup").as("partition_dedup_1"),
      col("dedup_internal"),
      col("dedup_internal").as("dedup_internal_1"),
      col("jurisdiction_group"),
      col("jurisdiction_group").as("jurisdiction_group_1"),
      col("masked_classification"),
      col("masked_classification").as("masked_classification_1"),
      col("masked_security_type"),
      col("masked_security_type").as("masked_security_type_1"),
      col("large_jurisdiction_list"),
      col("large_jurisdiction_list").as("large_jurisdiction_list_1"),
      col("size_classification"),
      col("size_classification").as("size_classification_1"),
      col("source_fileset_entry_nm"),
      col("source_fileset_entry_nm").as("source_fileset_entry_nm_1"),
      col("check_volume"),
      col("check_volume").as("check_volume_1"),
      col("reject_limit"),
      col("reject_limit").as("reject_limit_1"),
      col("empty_file_allowed"),
      col("empty_file_allowed").as("empty_file_allowed_1"),
      col("exclude_from_global_views"),
      col("exclude_from_global_views").as("exclude_from_global_views_1"),
      col("spare1"),
      col("spare1").as("spare1_1"),
      col("file_date"),
      col("file_date").as("file_date_1"),
      col("source_extract_instance_directory"),
      col("source_extract_instance_directory")
        .as("source_extract_instance_directory_1"),
      col("feed_date"),
      col("feed_date").as("feed_date_1"),
      col("vec_source_partition_files"),
      col("vec_source_partition_files").as("vec_source_partition_files_1")
    )

}
