package graph

import io.prophecy.libs._
import graph.Obtain_List_Of_Unprocessed_Directories.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Obtain_List_Of_Unprocessed_Directories {

  def apply(context: Context): DataFrame = {
    val df_Obtain_List_Of_Unprocessed_Directories_Processed_Directory_File =
      Obtain_List_Of_Unprocessed_Directories_Processed_Directory_File(context)
    val df_Create_Data       = Create_Data(context)
    val df_nrmlz_Directories = nrmlz_Directories(context, df_Create_Data)
    val df_jn_Identifiy_Unprocessed_Directories_Reformat_1 =
      jn_Identifiy_Unprocessed_Directories_Reformat_1(
        context,
        df_Obtain_List_Of_Unprocessed_Directories_Processed_Directory_File
      )
    val df_jn_Identifiy_Unprocessed_Directories_Reformat_0 =
      jn_Identifiy_Unprocessed_Directories_Reformat_0(context,
                                                      df_nrmlz_Directories
      )
    val df_jn_Identifiy_Unprocessed_Directories_Join =
      jn_Identifiy_Unprocessed_Directories_Join(
        context,
        df_jn_Identifiy_Unprocessed_Directories_Reformat_1,
        df_jn_Identifiy_Unprocessed_Directories_Reformat_0
      ).cache()
    val df_jn_Identifiy_Unprocessed_Directories_Filter_Unused_0 =
      jn_Identifiy_Unprocessed_Directories_Filter_Unused_0(
        context,
        df_jn_Identifiy_Unprocessed_Directories_Join
      )
    val df_jn_Identifiy_Unprocessed_Directories_Filter_Unused_0_Final_Reformat =
      jn_Identifiy_Unprocessed_Directories_Filter_Unused_0_Final_Reformat(
        context,
        df_jn_Identifiy_Unprocessed_Directories_Filter_Unused_0
      )
    val df_jn_Identifiy_Unprocessed_Directories_Filter =
      jn_Identifiy_Unprocessed_Directories_Filter(
        context,
        df_jn_Identifiy_Unprocessed_Directories_Join
      )
    val df_jn_Identifiy_Unprocessed_Directories_Final_Join_Reformat =
      jn_Identifiy_Unprocessed_Directories_Final_Join_Reformat(
        context,
        df_jn_Identifiy_Unprocessed_Directories_Filter
      )
    Trash_Processed_Directories(
      context,
      df_jn_Identifiy_Unprocessed_Directories_Final_Join_Reformat
    )
    df_jn_Identifiy_Unprocessed_Directories_Filter_Unused_0_Final_Reformat
  }

}
