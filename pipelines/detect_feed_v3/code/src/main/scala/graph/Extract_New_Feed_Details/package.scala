package graph

import io.prophecy.libs._
import graph.Extract_New_Feed_Details.config._
import graph.Extract_New_Feed_Details.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds
import graph.Extract_New_Feed_Details.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds.config.{
  Context => Strip_Out_Feeds_Later_Than_Any_Empty_Feeds_Context
}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Extract_New_Feed_Details {

  def apply(context: Context, in1: DataFrame, in: DataFrame): Subgraph2 = {
    val df_listing_of_source_files__Reformat_Get_Listing_Of_Source_Files =
      listing_of_source_files__Reformat_Get_Listing_Of_Source_Files(context,
                                                                    in1
      )
    val df_Strip_Out_Feeds_Later_Than_Any_Empty_Feeds =
      Strip_Out_Feeds_Later_Than_Any_Empty_Feeds.apply(
        Strip_Out_Feeds_Later_Than_Any_Empty_Feeds_Context(
          context.spark,
          context.config.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds
        ),
        df_listing_of_source_files__Reformat_Get_Listing_Of_Source_Files
      )
    val df_nrmlz_Directories_2 = nrmlz_Directories_2(
      context,
      df_Strip_Out_Feeds_Later_Than_Any_Empty_Feeds
    )
    val df_Join_Only_Process_IPR_Files =
      Join_Only_Process_IPR_Files(context, in, df_nrmlz_Directories_2)
    val df_Check_for_Data_in_Hadoop_Files_Reformat =
      Check_for_Data_in_Hadoop_Files_Reformat(context,
                                              df_Join_Only_Process_IPR_Files
      )
    val (df_FBE_Only_Want_Stable_Files_and_Files_With_Data_out0,
         df_FBE_Only_Want_Stable_Files_and_Files_With_Data_out1
    ) = FBE_Only_Want_Stable_Files_and_Files_With_Data(
      context,
      df_Check_for_Data_in_Hadoop_Files_Reformat
    )
    (df_FBE_Only_Want_Stable_Files_and_Files_With_Data_out0,
     df_FBE_Only_Want_Stable_Files_and_Files_With_Data_out1
    )
  }

}
