package graph.Check_Predecessor_Complete

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Check_Predecessor_Complete.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join {

  def apply(context: Context, right: DataFrame, left: DataFrame): DataFrame =
    right
      .as("right")
      .join(
        left.as("left"),
        (col("left.fileset_entry_nm") === col("right.fileset_entry_nm"))
          .and(col("left.cleanse_condition") === col("right.cleanse_condition"))
          .and(col("left.copy_to_ipr") === col("right.copy_to_ipr"))
          .and(col("left.reject_ramp") === col("right.reject_ramp"))
          .and(col("left.classification") === col("right.classification"))
          .and(col("left.security_type") === col("right.security_type"))
          .and(col("left.table_type") === col("right.table_type"))
          .and(col("left.partition_key") === col("right.partition_key"))
          .and(col("left.table_key") === col("right.table_key"))
          .and(col("left.sort_key") === col("right.sort_key"))
          .and(
            col("left.merge_with_previous") === col("right.merge_with_previous")
          )
          .and(
            col("left.merge_with_previous_enrich") === col(
              "right.merge_with_previous_enrich"
            )
          )
          .and(col("left.partition_dedup") === col("right.partition_dedup"))
          .and(col("left.dedup_internal") === col("right.dedup_internal"))
          .and(
            col("left.jurisdiction_group") === col("right.jurisdiction_group")
          )
          .and(
            col("left.masked_classification") === col(
              "right.masked_classification"
            )
          )
          .and(
            col("left.masked_security_type") === col(
              "right.masked_security_type"
            )
          )
          .and(
            col("left.large_jurisdiction_list") === col(
              "right.large_jurisdiction_list"
            )
          )
          .and(
            col("left.size_classification") === col("right.size_classification")
          )
          .and(
            col("left.source_fileset_entry_nm") === col(
              "right.source_fileset_entry_nm"
            )
          )
          .and(col("left.check_volume") === col("right.check_volume"))
          .and(col("left.reject_limit") === col("right.reject_limit"))
          .and(
            col("left.empty_file_allowed") === col("right.empty_file_allowed")
          )
          .and(
            col("left.exclude_from_global_views") === col(
              "right.exclude_from_global_views"
            )
          )
          .and(col("left.spare1") === col("right.spare1"))
          .and(col("left.file_date") === col("right.file_date"))
          .and(
            col("left.source_extract_instance_directory") === col(
              "right.source_extract_instance_directory"
            )
          )
          .and(col("left.feed_date") === col("right.feed_date"))
          .and(
            col("left.vec_source_partition_files") === col(
              "right.vec_source_partition_files"
            )
          ),
        "outer"
      )
      .select(
        col("right.fileset_entry_nm").as("fileset_entry_nm"),
        col("right.cleanse_condition").as("cleanse_condition"),
        col("right.copy_to_ipr").as("copy_to_ipr"),
        col("right.reject_ramp").as("reject_ramp"),
        col("right.classification").as("classification"),
        col("right.security_type").as("security_type"),
        col("right.table_type").as("table_type"),
        col("right.partition_key").as("partition_key"),
        col("right.table_key").as("table_key"),
        col("right.sort_key").as("sort_key"),
        col("right.merge_with_previous").as("merge_with_previous"),
        col("right.merge_with_previous_enrich")
          .as("merge_with_previous_enrich"),
        col("right.partition_dedup").as("partition_dedup"),
        col("right.dedup_internal").as("dedup_internal"),
        col("right.jurisdiction_group").as("jurisdiction_group"),
        col("right.masked_classification").as("masked_classification"),
        col("right.masked_security_type").as("masked_security_type"),
        col("right.large_jurisdiction_list").as("large_jurisdiction_list"),
        col("right.size_classification").as("size_classification"),
        col("right.source_fileset_entry_nm").as("source_fileset_entry_nm"),
        col("right.check_volume").as("check_volume"),
        col("right.reject_limit").as("reject_limit"),
        col("right.empty_file_allowed").as("empty_file_allowed"),
        col("right.exclude_from_global_views").as("exclude_from_global_views"),
        col("right.spare1").as("spare1"),
        col("right.file_date").as("file_date"),
        col("right.source_extract_instance_directory")
          .as("source_extract_instance_directory"),
        col("right.feed_date").as("feed_date"),
        coalesce(col("left.feed_date"), lit("21000101010101"))
          .as("earliest_incomplete_feed_date"),
        col("right.vec_source_partition_files").as("vec_source_partition_files")
      )

}
