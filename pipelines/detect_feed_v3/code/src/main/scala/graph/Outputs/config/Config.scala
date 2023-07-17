package graph.Outputs.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import graph.Outputs.Update_Files_Received_Log.config.{
  Config => Update_Files_Received_Log_Config
}
import graph.Outputs.Update_Processed_Directory_and_Create_Trigger_File.config.{
  Config => Update_Processed_Directory_and_Create_Trigger_File_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  Update_Files_Received_Log: Update_Files_Received_Log_Config =
    Update_Files_Received_Log_Config(),
  Update_Processed_Directory_and_Create_Trigger_File: Update_Processed_Directory_and_Create_Trigger_File_Config =
    Update_Processed_Directory_and_Create_Trigger_File_Config()
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
