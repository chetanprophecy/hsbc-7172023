package graph

import io.prophecy.libs._
import graph.Check_Predecessor_Complete.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Check_Predecessor_Complete {

  def apply(context: Context, in1: DataFrame, in: DataFrame): DataFrame = {
    val df_Sort = Sort(context, in)
    val df_LR_Just_Want_Earliest_Incomplete_Feeds =
      LR_Just_Want_Earliest_Incomplete_Feeds(context, df_Sort)
    val df_Join = Join(context, in1, df_LR_Just_Want_Earliest_Incomplete_Feeds)
    val (df_FBE_Previous_Incomplete_Feed_out0,
         df_FBE_Previous_Incomplete_Feed_out1
    ) = FBE_Previous_Incomplete_Feed(context, df_Join)
    val df_FBE_Previous_Incomplete_Feed_Reformat_out1 =
      FBE_Previous_Incomplete_Feed_Reformat_out1(
        context,
        df_FBE_Previous_Incomplete_Feed_out1
      )
    Trash_Source_Directories_that_are_complete_but_have_an_incomplete_predecessor(
      context,
      df_FBE_Previous_Incomplete_Feed_Reformat_out1
    )
    val df_FBE_Previous_Incomplete_Feed_Reformat_out0 =
      FBE_Previous_Incomplete_Feed_Reformat_out0(
        context,
        df_FBE_Previous_Incomplete_Feed_out0
      )
    df_FBE_Previous_Incomplete_Feed_Reformat_out0
  }

}
