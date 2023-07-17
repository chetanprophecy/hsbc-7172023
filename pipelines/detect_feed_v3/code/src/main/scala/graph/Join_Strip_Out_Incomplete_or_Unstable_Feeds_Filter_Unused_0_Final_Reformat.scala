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

object Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter_Unused_0_Final_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("fileset_entry_nm_0").as("fileset_entry_nm"),
      col("cleanse_condition_0").as("cleanse_condition"),
      col("copy_to_ipr_0").as("copy_to_ipr"),
      col("reject_ramp_0").as("reject_ramp"),
      col("classification_0").as("classification"),
      col("security_type_0").as("security_type"),
      col("table_type_0").as("table_type"),
      col("partition_key_0").as("partition_key"),
      col("table_key_0").as("table_key"),
      col("sort_key_0").as("sort_key"),
      col("merge_with_previous_0").as("merge_with_previous"),
      col("merge_with_previous_enrich_0").as("merge_with_previous_enrich"),
      col("partition_dedup_0").as("partition_dedup"),
      col("dedup_internal_0").as("dedup_internal"),
      col("jurisdiction_group_0").as("jurisdiction_group"),
      col("masked_classification_0").as("masked_classification"),
      col("masked_security_type_0").as("masked_security_type"),
      col("large_jurisdiction_list_0").as("large_jurisdiction_list"),
      col("size_classification_0").as("size_classification"),
      col("source_fileset_entry_nm_0").as("source_fileset_entry_nm"),
      col("check_volume_0").as("check_volume"),
      col("reject_limit_0").as("reject_limit"),
      col("empty_file_allowed_0").as("empty_file_allowed"),
      col("exclude_from_global_views_0").as("exclude_from_global_views"),
      col("spare1_0").as("spare1"),
      col("file_date_0").as("file_date"),
      col("source_extract_instance_directory_0")
        .as("source_extract_instance_directory"),
      col("feed_date_0").as("feed_date"),
      col("vec_source_partition_files_0").as("vec_source_partition_files")
    )

}
