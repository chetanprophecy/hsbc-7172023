package graph.Extract_New_Feed_Details

import io.prophecy.libs._
import graph.Extract_New_Feed_Details.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Strip_Out_Feeds_Later_Than_Any_Empty_Feeds {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_FBE_Detect_Empty_Directory = FBE_Detect_Empty_Directory(context, in)
    val df_Sort_1                     = Sort_1(context,                     df_FBE_Detect_Empty_Directory)
    val df_Leading_Records            = Leading_Records(context,            df_Sort_1)
    val df_Join_Feeds_to_any_empty_feed_Dirs =
      Join_Feeds_to_any_empty_feed_Dirs(context, df_Leading_Records, in)
    val (df_FBE_Strip_Out_Feeds_Later_Than_Any_Empty_Feed_ss_out0,
         df_FBE_Strip_Out_Feeds_Later_Than_Any_Empty_Feed_ss_out1
    ) = FBE_Strip_Out_Feeds_Later_Than_Any_Empty_Feed_ss(
      context,
      df_Join_Feeds_to_any_empty_feed_Dirs
    )
    val df_Rf_Reduce_DML_Reformat = Rf_Reduce_DML_Reformat(
      context,
      df_FBE_Strip_Out_Feeds_Later_Than_Any_Empty_Feed_ss_out1
    )
    val df_Format_Log_Record_Reformat =
      Format_Log_Record_Reformat(context,   df_Rf_Reduce_DML_Reformat)
    Write_Deselect_Feeds_to_Stdout(context, df_Format_Log_Record_Reformat)
    df_FBE_Strip_Out_Feeds_Later_Than_Any_Empty_Feed_ss_out0
  }

}
