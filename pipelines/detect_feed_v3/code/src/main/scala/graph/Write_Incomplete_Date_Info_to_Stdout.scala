package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Incomplete_Date_Info_to_Stdout {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .mode("overwrite")
      .save(
        "/Users/ashish/hsbc/abinitio-demo/targets/Write_Incomplete_Date_Info_to_Stdout"
      )

}
