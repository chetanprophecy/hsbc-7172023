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

object Join_Strip_Out_Incomplete_or_Unstable_Feeds_Reformat_0 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("fileset_entry_nm"),
      col("fileset_entry_nm").as("fileset_entry_nm_0"),
      col("cleanse_condition"),
      col("cleanse_condition").as("cleanse_condition_0"),
      col("copy_to_ipr"),
      col("copy_to_ipr").as("copy_to_ipr_0"),
      col("reject_ramp"),
      col("reject_ramp").as("reject_ramp_0"),
      col("classification"),
      col("classification").as("classification_0"),
      col("security_type"),
      col("security_type").as("security_type_0"),
      col("table_type"),
      col("table_type").as("table_type_0"),
      col("partition_key"),
      col("partition_key").as("partition_key_0"),
      col("table_key"),
      col("table_key").as("table_key_0"),
      col("sort_key"),
      col("sort_key").as("sort_key_0"),
      col("merge_with_previous"),
      col("merge_with_previous").as("merge_with_previous_0"),
      col("merge_with_previous_enrich"),
      col("merge_with_previous_enrich").as("merge_with_previous_enrich_0"),
      col("partition_dedup"),
      col("partition_dedup").as("partition_dedup_0"),
      col("dedup_internal"),
      col("dedup_internal").as("dedup_internal_0"),
      col("jurisdiction_group"),
      col("jurisdiction_group").as("jurisdiction_group_0"),
      col("masked_classification"),
      col("masked_classification").as("masked_classification_0"),
      col("masked_security_type"),
      col("masked_security_type").as("masked_security_type_0"),
      col("large_jurisdiction_list"),
      col("large_jurisdiction_list").as("large_jurisdiction_list_0"),
      col("size_classification"),
      col("size_classification").as("size_classification_0"),
      col("source_fileset_entry_nm"),
      col("source_fileset_entry_nm").as("source_fileset_entry_nm_0"),
      col("check_volume"),
      col("check_volume").as("check_volume_0"),
      col("reject_limit"),
      col("reject_limit").as("reject_limit_0"),
      col("empty_file_allowed"),
      col("empty_file_allowed").as("empty_file_allowed_0"),
      col("exclude_from_global_views"),
      col("exclude_from_global_views").as("exclude_from_global_views_0"),
      col("spare1"),
      col("spare1").as("spare1_0"),
      col("file_date"),
      col("file_date").as("file_date_0"),
      col("source_extract_instance_directory"),
      col("source_extract_instance_directory")
        .as("source_extract_instance_directory_0"),
      col("feed_date"),
      col("feed_date").as("feed_date_0"),
      col("vec_source_partition_files"),
      col("vec_source_partition_files").as("vec_source_partition_files_0")
    )

}
