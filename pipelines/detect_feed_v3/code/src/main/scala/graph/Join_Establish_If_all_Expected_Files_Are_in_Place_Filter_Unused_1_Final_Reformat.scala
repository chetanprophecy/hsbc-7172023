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

object Join_Establish_If_all_Expected_Files_Are_in_Place_Filter_Unused_1_Final_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("fileset_entry_nm_1").as("fileset_entry_nm"),
      col("cleanse_condition_1").as("cleanse_condition"),
      col("copy_to_ipr_1").as("copy_to_ipr"),
      col("reject_ramp_1").as("reject_ramp"),
      col("classification_1").as("classification"),
      col("security_type_1").as("security_type"),
      col("table_type_1").as("table_type"),
      col("partition_key_1").as("partition_key"),
      col("table_key_1").as("table_key"),
      col("sort_key_1").as("sort_key"),
      col("merge_with_previous_1").as("merge_with_previous"),
      col("merge_with_previous_enrich_1").as("merge_with_previous_enrich"),
      col("partition_dedup_1").as("partition_dedup"),
      col("dedup_internal_1").as("dedup_internal"),
      col("jurisdiction_group_1").as("jurisdiction_group"),
      col("masked_classification_1").as("masked_classification"),
      col("masked_security_type_1").as("masked_security_type"),
      col("large_jurisdiction_list_1").as("large_jurisdiction_list"),
      col("size_classification_1").as("size_classification"),
      col("source_fileset_entry_nm_1").as("source_fileset_entry_nm"),
      col("check_volume_1").as("check_volume"),
      col("reject_limit_1").as("reject_limit"),
      col("empty_file_allowed_1").as("empty_file_allowed"),
      col("exclude_from_global_views_1").as("exclude_from_global_views"),
      col("spare1_1").as("spare1"),
      col("file_date_1").as("file_date"),
      col("source_extract_instance_directory_1")
        .as("source_extract_instance_directory"),
      col("feed_date_1").as("feed_date"),
      col("vec_source_partition_files_1").as("vec_source_partition_files")
    )

}
