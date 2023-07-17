package graph

import io.prophecy.libs._
import graph.Outputs.config._
import graph.Outputs.Update_Files_Received_Log
import graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File
import graph.Outputs.Update_Files_Received_Log.config.{
  Context => Update_Files_Received_Log_Context
}
import graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File.config.{
  Context => Update_Processed_Directory_and_Create_Trigger_File_Context
}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Outputs {

  def apply(context: Context, in: DataFrame): Unit = {
    Update_Files_Received_Log.apply(
      Update_Files_Received_Log_Context(context.spark,
                                        context.config.Update_Files_Received_Log
      ),
      in
    )
    Update_Processed_Directory_and_Create_Trigger_File.apply(
      Update_Processed_Directory_and_Create_Trigger_File_Context(
        context.spark,
        context.config.Update_Processed_Directory_and_Create_Trigger_File
      ),
      in,
      in
    )
  }

}
