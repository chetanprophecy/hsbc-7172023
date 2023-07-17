package graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object rup_Leading_Record_from_each_Source_Directorory {

  def apply(context: Context, in0: DataFrame): DataFrame =
    in0
      .groupBy(col("source_extract_instance_directory"))
      .agg(
        first(col("fileset_entry_nm")).as("fileset_entry_nm"),
        first(col("cleanse_condition")).as("cleanse_condition"),
        first(col("copy_to_ipr")).as("copy_to_ipr"),
        first(col("reject_ramp")).as("reject_ramp"),
        first(col("classification")).as("classification"),
        first(col("security_type")).as("security_type"),
        first(col("table_type")).as("table_type"),
        first(col("partition_key")).as("partition_key"),
        first(col("table_key")).as("table_key"),
        first(col("sort_key")).as("sort_key"),
        first(col("merge_with_previous")).as("merge_with_previous"),
        first(col("merge_with_previous_enrich"))
          .as("merge_with_previous_enrich"),
        first(col("partition_dedup")).as("partition_dedup"),
        first(col("dedup_internal")).as("dedup_internal"),
        first(col("jurisdiction_group")).as("jurisdiction_group"),
        first(col("masked_classification")).as("masked_classification"),
        first(col("masked_security_type")).as("masked_security_type"),
        first(col("large_jurisdiction_list")).as("large_jurisdiction_list"),
        first(col("size_classification")).as("size_classification"),
        first(col("source_fileset_entry_nm")).as("source_fileset_entry_nm"),
        first(col("check_volume")).as("check_volume"),
        first(col("reject_limit")).as("reject_limit"),
        first(col("empty_file_allowed")).as("empty_file_allowed"),
        first(col("exclude_from_global_views")).as("exclude_from_global_views"),
        first(col("spare1")).as("spare1"),
        first(col("file_date")).as("file_date"),
        first(col("feed_date")).as("feed_date"),
        first(col("vec_source_partition_files"))
          .as("vec_source_partition_files")
      )

}
