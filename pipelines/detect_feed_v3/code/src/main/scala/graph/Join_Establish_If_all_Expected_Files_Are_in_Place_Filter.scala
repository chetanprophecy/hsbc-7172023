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

object Join_Establish_If_all_Expected_Files_Are_in_Place_Filter {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      col("file_date_0").isNotNull
        .or(!col("source_extract_file_directory_0").isNull)
        .or(!col("source_extract_file_name_0").isNull)
        .or(!col("size_0").cast(LongType).isNull)
        .or(!col("empty_file_allowed_0").isNull)
        .or(!col("source_db_name_0").isNull)
        .or(!col("seconds_since_directory_updated_0").isNull)
        .and(
          col("fileset_entry_nm_1").isNotNull
            .or(!col("cleanse_condition_1").isNull)
            .or(!col("copy_to_ipr_1").isNull)
            .or(!col("reject_ramp_1").isNull)
            .or(!col("classification_1").isNull)
            .or(!col("security_type_1").isNull)
            .or(!col("table_type_1").isNull)
            .or(!col("partition_key_1").isNull)
            .or(!col("table_key_1").isNull)
            .or(!col("sort_key_1").isNull)
            .or(!col("merge_with_previous_1").isNull)
            .or(!col("merge_with_previous_enrich_1").isNull)
            .or(!col("partition_dedup_1").isNull)
            .or(!col("dedup_internal_1").isNull)
            .or(!col("jurisdiction_group_1").isNull)
            .or(!col("masked_classification_1").isNull)
            .or(!col("masked_security_type_1").isNull)
            .or(!col("large_jurisdiction_list_1").isNull)
            .or(!col("size_classification_1").isNull)
            .or(!col("source_fileset_entry_nm_1").isNull)
            .or(!col("check_volume_1").isNull)
            .or(!col("reject_limit_1").isNull)
            .or(!col("empty_file_allowed_1").isNull)
            .or(!col("exclude_from_global_views_1").isNull)
            .or(!col("spare1_1").isNull)
            .or(!col("file_date_1").isNull)
            .or(!col("source_extract_instance_directory_1").isNull)
            .or(!col("feed_date_1").isNull)
            .or(!col("vec_source_partition_files_1").isNull)
        )
    )

}
