package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import graph.Obtain_List_Of_Unprocessed_Directories.config.{
  Config => Obtain_List_Of_Unprocessed_Directories_Config
}
import graph.Get_Filelists_from_Filesets.config.{
  Config => Get_Filelists_from_Filesets_Config
}
import graph.Extract_New_Feed_Details.config.{
  Config => Extract_New_Feed_Details_Config
}
import graph.Check_Predecessor_Complete.config.{
  Config => Check_Predecessor_Complete_Config
}
import graph.Outputs.config.{Config => Outputs_Config}

case class Config(
  GRAPH_FILE_DATE_FORMAT:            String = "YYYYMMDD",
  GRAPH_LANDING_STRUCTURE:           String = "STANDARD",
  FEED_ARRIVAL_PUB_COUNTRY:          String = "aa",
  FEED_ARRIVAL_PUB_SCHEDULE:         String = "daily",
  FEED_ARRIVAL_PUB_LEGAL_ENTITY:     String = "hsbc",
  FEED_ARRIVAL_PUB_SOURCE:           String = "worldcheck",
  FEED_ARRIVAL_PUB_EXTRACT:          String = "scp",
  GRAPH_MANDATORY_FILESET_FROM_DATE: String = "19000101",
  Get_Filelists_from_Filesets: Get_Filelists_from_Filesets_Config =
    Get_Filelists_from_Filesets_Config(),
  Extract_New_Feed_Details: Extract_New_Feed_Details_Config =
    Extract_New_Feed_Details_Config(),
  Check_Predecessor_Complete: Check_Predecessor_Complete_Config =
    Check_Predecessor_Complete_Config(),
  Obtain_List_Of_Unprocessed_Directories: Obtain_List_Of_Unprocessed_Directories_Config =
    Obtain_List_Of_Unprocessed_Directories_Config(),
  Outputs: Outputs_Config = Outputs_Config()
) extends ConfigBase
