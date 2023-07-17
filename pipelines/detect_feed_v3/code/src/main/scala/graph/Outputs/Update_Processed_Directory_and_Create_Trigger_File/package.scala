package graph.Outputs

import io.prophecy.libs._
import graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Update_Processed_Directory_and_Create_Trigger_File {

  def apply(context: Context, in1: DataFrame, in: DataFrame): Unit = {
    val df_Rollup_One_directory_per_feed =
      Rollup_One_directory_per_feed(context, in)
    val df_Rollup_One_directory_per_feed_Reformat =
      Rollup_One_directory_per_feed_Reformat(context,
                                             df_Rollup_One_directory_per_feed
      )
    Processed_Directory_File_1(context,
                               df_Rollup_One_directory_per_feed_Reformat
    )
    val df_Rollup_Extracts = Rollup_Extracts(context, in1)
    val df_Rollup_Extracts_Reformat =
      Rollup_Extracts_Reformat(context,   df_Rollup_Extracts)
    Write_Multiple_Trigger_Files(context, df_Rollup_Extracts_Reformat)
  }

}
