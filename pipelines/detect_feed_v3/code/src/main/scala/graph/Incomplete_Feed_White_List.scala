package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Incomplete_Feed_White_List {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("country",      StringType, true),
            StructField("legal_entity", StringType, true),
            StructField("source",       StringType, true),
            StructField("extract",      StringType, true),
            StructField("schedule",     StringType, true),
            StructField("file_date",    StringType, true)
          )
        )
      )
      .load("/Users/ashish/hsbc/abinitio-demo/Incomplete_Feed_White_List_1.csv")

}
