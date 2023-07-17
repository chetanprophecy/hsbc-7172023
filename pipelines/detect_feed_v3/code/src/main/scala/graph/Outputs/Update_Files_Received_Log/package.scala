package graph.Outputs

import io.prophecy.libs._
import graph.Outputs.Update_Files_Received_Log.config._
import graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source
import graph.Outputs.Update_Files_Received_Log.listing_of_all_files_received_from_source.config.{
  Context => listing_of_all_files_received_from_source_Context
}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Update_Files_Received_Log {

  def apply(context: Context, in: DataFrame): Unit = {
    val df_listing_of_all_files_received_from_source =
      listing_of_all_files_received_from_source.apply(
        listing_of_all_files_received_from_source_Context(
          context.spark,
          context.config.listing_of_all_files_received_from_source
        ),
        in
      )
    Multipublish_Files_Received_Queue(
      context,
      df_listing_of_all_files_received_from_source
    )
  }

}
