package graph.Outputs.Update_Files_Received_Log

import io.prophecy.libs._
import graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object listing_of_all_files_received_from_source {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_rup_Leading_Record_from_each_Source_Directorory =
      rup_Leading_Record_from_each_Source_Directorory(context, in)
    val df_rup_Leading_Record_from_each_Source_Directorory_Reformat =
      rup_Leading_Record_from_each_Source_Directorory_Reformat(
        context,
        df_rup_Leading_Record_from_each_Source_Directorory
      )
    val df_RF_Complete_Listing_of_Source_Extract_Tabless_Reformat =
      RF_Complete_Listing_of_Source_Extract_Tabless_Reformat(
        context,
        df_rup_Leading_Record_from_each_Source_Directorory_Reformat
      )
    val df_nrmlz_Directories_1 = nrmlz_Directories_1(
      context,
      df_RF_Complete_Listing_of_Source_Extract_Tabless_Reformat
    )
    df_nrmlz_Directories_1
  }

}
