package graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File

import io.prophecy.libs._
import graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Processed_Directory_File_1 {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .mode("overwrite")
      .save(
        "dbfs:/Users/ashish/hsbc/abinitio-demo/targets/Processed_Directory_File"
      )

}
