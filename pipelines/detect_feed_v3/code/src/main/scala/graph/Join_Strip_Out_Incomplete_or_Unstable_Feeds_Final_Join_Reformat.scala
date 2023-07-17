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

object Join_Strip_Out_Incomplete_or_Unstable_Feeds_Final_Join_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("fileset_entry_nm"),
      col("cleanse_condition"),
      col("copy_to_ipr"),
      col("reject_ramp"),
      col("classification"),
      col("security_type"),
      col("table_type"),
      col("partition_key"),
      col("table_key"),
      col("sort_key"),
      col("merge_with_previous"),
      col("merge_with_previous_enrich"),
      col("partition_dedup"),
      col("dedup_internal"),
      col("jurisdiction_group"),
      col("masked_classification"),
      col("masked_security_type"),
      col("large_jurisdiction_list"),
      col("size_classification"),
      col("source_fileset_entry_nm"),
      col("check_volume"),
      col("reject_limit"),
      col("empty_file_allowed"),
      col("exclude_from_global_views"),
      col("spare1"),
      col("file_date"),
      col("source_extract_instance_directory"),
      col("feed_date"),
      col("vec_source_partition_files")
    )

}
