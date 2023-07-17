package graph.Extract_New_Feed_Details.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds

import io.prophecy.libs._
import graph.Extract_New_Feed_Details.Strip_Out_Feeds_Later_Than_Any_Empty_Feeds.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Deselect_Feeds_to_Stdout {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .mode("overwrite")
      .save(
        "/Users/ashish/hsbc/abinitio-demo/targets/Write_Deselect_Feeds_to_Stdout"
      )

}
