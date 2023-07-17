package graph.Obtain_List_Of_Unprocessed_Directories

import io.prophecy.libs._
import graph.Obtain_List_Of_Unprocessed_Directories.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Obtain_List_Of_Unprocessed_Directories_Processed_Directory_File {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(Array(StructField("directory_entry", StringType, true)))
      )
      .load(
        "/Users/ashish/hsbc/abinitio-demo/Obtain_List_Of_Unprocessed_Directories_Processed_Directory_File_1.csv"
      )

}
