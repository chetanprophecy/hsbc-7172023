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

object Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      col("fileset_entry_nm_0").isNotNull
        .or(!col("cleanse_condition_0").isNull)
        .or(!col("copy_to_ipr_0").isNull)
        .or(!col("reject_ramp_0").isNull)
        .or(!col("classification_0").isNull)
        .or(!col("security_type_0").isNull)
        .or(!col("table_type_0").isNull)
        .or(!col("partition_key_0").isNull)
        .or(!col("table_key_0").isNull)
        .or(!col("sort_key_0").isNull)
        .or(!col("merge_with_previous_0").isNull)
        .or(!col("merge_with_previous_enrich_0").isNull)
        .or(!col("partition_dedup_0").isNull)
        .or(!col("dedup_internal_0").isNull)
        .or(!col("jurisdiction_group_0").isNull)
        .or(!col("masked_classification_0").isNull)
        .or(!col("masked_security_type_0").isNull)
        .or(!col("large_jurisdiction_list_0").isNull)
        .or(!col("size_classification_0").isNull)
        .or(!col("source_fileset_entry_nm_0").isNull)
        .or(!col("check_volume_0").isNull)
        .or(!col("reject_limit_0").isNull)
        .or(!col("empty_file_allowed_0").isNull)
        .or(!col("exclude_from_global_views_0").isNull)
        .or(!col("spare1_0").isNull)
        .or(!col("file_date_0").isNull)
        .or(!col("source_extract_instance_directory_0").isNull)
        .or(!col("feed_date_0").isNull)
        .or(!col("vec_source_partition_files_0").isNull)
        .and(col("file_date_1").isNotNull)
    )

}
