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

object Join_Strip_Out_Incomplete_or_Unstable_Feeds_Join {

  def apply(context: Context, right: DataFrame, left: DataFrame): DataFrame =
    right
      .as("right")
      .join(left.as("left"),
            col("left.file_date") === col("right.file_date"),
            "outer"
      )
      .select(
        col("left.merge_with_previous_enrich_0")
          .as("merge_with_previous_enrich_0"),
        col("left.masked_classification_0").as("masked_classification_0"),
        col("left.source_fileset_entry_nm_0").as("source_fileset_entry_nm_0"),
        col("left.check_volume_0").as("check_volume_0"),
        col("left.classification").as("classification"),
        col("left.exclude_from_global_views").as("exclude_from_global_views"),
        col("left.partition_key").as("partition_key"),
        col("left.masked_classification").as("masked_classification"),
        col("left.large_jurisdiction_list_0").as("large_jurisdiction_list_0"),
        col("left.source_extract_instance_directory")
          .as("source_extract_instance_directory"),
        col("left.reject_limit_0").as("reject_limit_0"),
        col("left.file_date_0").as("file_date_0"),
        col("left.security_type_0").as("security_type_0"),
        col("left.partition_key_0").as("partition_key_0"),
        col("left.check_volume").as("check_volume"),
        col("left.reject_ramp").as("reject_ramp"),
        col("left.cleanse_condition").as("cleanse_condition"),
        col("left.copy_to_ipr").as("copy_to_ipr"),
        col("left.jurisdiction_group").as("jurisdiction_group"),
        col("left.merge_with_previous_0").as("merge_with_previous_0"),
        col("left.dedup_internal_0").as("dedup_internal_0"),
        col("left.size_classification_0").as("size_classification_0"),
        col("left.fileset_entry_nm").as("fileset_entry_nm"),
        col("left.masked_security_type").as("masked_security_type"),
        col("left.exclude_from_global_views_0")
          .as("exclude_from_global_views_0"),
        col("left.sort_key_0").as("sort_key_0"),
        col("left.source_fileset_entry_nm").as("source_fileset_entry_nm"),
        col("left.spare1_0").as("spare1_0"),
        col("left.vec_source_partition_files").as("vec_source_partition_files"),
        col("left.classification_0").as("classification_0"),
        col("left.table_key_0").as("table_key_0"),
        col("left.vec_source_partition_files_0")
          .as("vec_source_partition_files_0"),
        col("left.empty_file_allowed").as("empty_file_allowed"),
        col("left.feed_date_0").as("feed_date_0"),
        col("left.sort_key").as("sort_key"),
        col("left.table_key").as("table_key"),
        col("left.file_date").as("file_date"),
        col("left.dedup_internal").as("dedup_internal"),
        col("left.size_classification").as("size_classification"),
        col("left.empty_file_allowed_0").as("empty_file_allowed_0"),
        col("left.source_extract_instance_directory_0")
          .as("source_extract_instance_directory_0"),
        col("left.table_type").as("table_type"),
        col("left.reject_ramp_0").as("reject_ramp_0"),
        col("left.spare1").as("spare1"),
        col("left.merge_with_previous_enrich").as("merge_with_previous_enrich"),
        col("left.partition_dedup_0").as("partition_dedup_0"),
        col("left.fileset_entry_nm_0").as("fileset_entry_nm_0"),
        col("left.merge_with_previous").as("merge_with_previous"),
        col("right.file_date_1").as("file_date_1"),
        col("left.jurisdiction_group_0").as("jurisdiction_group_0"),
        col("left.partition_dedup").as("partition_dedup"),
        col("left.masked_security_type_0").as("masked_security_type_0"),
        col("left.cleanse_condition_0").as("cleanse_condition_0"),
        col("left.reject_limit").as("reject_limit"),
        col("left.large_jurisdiction_list").as("large_jurisdiction_list"),
        col("left.security_type").as("security_type"),
        col("left.copy_to_ipr_0").as("copy_to_ipr_0"),
        col("left.table_type_0").as("table_type_0"),
        col("left.feed_date").as("feed_date")
      )

}
