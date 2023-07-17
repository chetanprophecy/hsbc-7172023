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

object Join_Establish_If_all_Expected_Files_Are_in_Place_Join {

  def apply(context: Context, right: DataFrame, left: DataFrame): DataFrame =
    right
      .as("right")
      .join(
        left.as("left"),
        (col("left.file_date") === col("right.file_date")).and(
          col("left.source_extract_file_name") === col(
            "right.source_fileset_entry_nm"
          )
        ),
        "outer"
      )
      .select(
        col("right.exclude_from_global_views_1")
          .as("exclude_from_global_views_1"),
        col("right.classification").as("classification"),
        col("right.sort_key_1").as("sort_key_1"),
        col("right.exclude_from_global_views").as("exclude_from_global_views"),
        col("right.partition_key").as("partition_key"),
        col("right.spare1_1").as("spare1_1"),
        col("right.classification_1").as("classification_1"),
        col("right.table_key_1").as("table_key_1"),
        col("right.vec_source_partition_files_1")
          .as("vec_source_partition_files_1"),
        col("right.masked_classification").as("masked_classification"),
        col("right.source_extract_instance_directory")
          .as("source_extract_instance_directory"),
        col("left.file_date_0").as("file_date_0"),
        col("right.feed_date_1").as("feed_date_1"),
        col("right.check_volume").as("check_volume"),
        col("right.empty_file_allowed_1").as("empty_file_allowed_1"),
        col("right.reject_ramp").as("reject_ramp"),
        col("right.source_extract_instance_directory_1")
          .as("source_extract_instance_directory_1"),
        col("right.copy_to_ipr_1").as("copy_to_ipr_1"),
        col("right.reject_ramp_1").as("reject_ramp_1"),
        col("right.cleanse_condition").as("cleanse_condition"),
        col("right.copy_to_ipr").as("copy_to_ipr"),
        col("right.partition_dedup_1").as("partition_dedup_1"),
        col("right.jurisdiction_group").as("jurisdiction_group"),
        col("right.fileset_entry_nm_1").as("fileset_entry_nm_1"),
        col("right.jurisdiction_group_1").as("jurisdiction_group_1"),
        col("left.source_extract_file_directory_0")
          .as("source_extract_file_directory_0"),
        col("right.fileset_entry_nm").as("fileset_entry_nm"),
        col("right.masked_security_type_1").as("masked_security_type_1"),
        col("right.cleanse_condition_1").as("cleanse_condition_1"),
        col("right.masked_security_type").as("masked_security_type"),
        col("right.check_volume_1").as("check_volume_1"),
        col("right.table_type_1").as("table_type_1"),
        col("right.source_fileset_entry_nm").as("source_fileset_entry_nm"),
        array()
          .cast(ArrayType(StringType, true))
          .as("vec_source_partition_files"),
        col("right.empty_file_allowed").as("empty_file_allowed"),
        col("right.sort_key").as("sort_key"),
        col("right.partition_key_1").as("partition_key_1"),
        col("right.table_key").as("table_key"),
        col("left.source_extract_file_name_0").as("source_extract_file_name_0"),
        col("right.merge_with_previous_enrich_1")
          .as("merge_with_previous_enrich_1"),
        col("right.masked_classification_1").as("masked_classification_1"),
        col("right.dedup_internal").as("dedup_internal"),
        col("right.size_classification").as("size_classification"),
        col("right.source_fileset_entry_nm_1").as("source_fileset_entry_nm_1"),
        col("right.file_date").as("file_date"),
        col("left.empty_file_allowed_0").as("empty_file_allowed_0"),
        col("left.size_0").as("size_0"),
        col("right.table_type").as("table_type"),
        col("right.spare1").as("spare1"),
        col("left.seconds_since_directory_updated_0")
          .as("seconds_since_directory_updated_0"),
        col("right.merge_with_previous_enrich")
          .as("merge_with_previous_enrich"),
        col("right.large_jurisdiction_list_1").as("large_jurisdiction_list_1"),
        col("right.merge_with_previous").as("merge_with_previous"),
        col("right.reject_limit_1").as("reject_limit_1"),
        col("right.file_date_1").as("file_date_1"),
        col("right.security_type_1").as("security_type_1"),
        col("right.partition_dedup").as("partition_dedup"),
        col("right.reject_limit").as("reject_limit"),
        col("right.large_jurisdiction_list").as("large_jurisdiction_list"),
        col("right.security_type").as("security_type"),
        col("left.source_db_name_0").as("source_db_name_0"),
        col("right.feed_date").as("feed_date"),
        col("right.merge_with_previous_1").as("merge_with_previous_1"),
        col("right.dedup_internal_1").as("dedup_internal_1"),
        col("right.size_classification_1").as("size_classification_1")
      )

}
