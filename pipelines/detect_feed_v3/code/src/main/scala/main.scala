import io.prophecy.libs._
import config.Context
import config._
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import graph._
import graph.Obtain_List_Of_Unprocessed_Directories
import graph.Get_Filelists_from_Filesets
import graph.Extract_New_Feed_Details
import graph.Check_Predecessor_Complete
import graph.Outputs
import graph.Obtain_List_Of_Unprocessed_Directories.config.{
  Context => Obtain_List_Of_Unprocessed_Directories_Context
}
import graph.Get_Filelists_from_Filesets.config.{
  Context => Get_Filelists_from_Filesets_Context
}
import graph.Extract_New_Feed_Details.config.{
  Context => Extract_New_Feed_Details_Context
}
import graph.Check_Predecessor_Complete.config.{
  Context => Check_Predecessor_Complete_Context
}
import graph.Outputs.config.{Context => Outputs_Context}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_Incomplete_Feed_White_List = Incomplete_Feed_White_List(context)
    Incomplete_Feed_White_List_lookup(context, df_Incomplete_Feed_White_List)
    val df_Obtain_List_Of_Unprocessed_Directories =
      Obtain_List_Of_Unprocessed_Directories.apply(
        Obtain_List_Of_Unprocessed_Directories_Context(
          context.spark,
          context.config.Obtain_List_Of_Unprocessed_Directories
        )
      )
    val df_Reformat_Get_File_Date_from_DB_Directory_Reformat =
      Reformat_Get_File_Date_from_DB_Directory_Reformat(
        context,
        df_Obtain_List_Of_Unprocessed_Directories
      )
    val df_Get_Filelists_from_Filesets = Get_Filelists_from_Filesets.apply(
      Get_Filelists_from_Filesets_Context(
        context.spark,
        context.config.Get_Filelists_from_Filesets
      ),
      df_Reformat_Get_File_Date_from_DB_Directory_Reformat
    )
    val (df_Extract_New_Feed_Details_out1, df_Extract_New_Feed_Details_out) =
      Extract_New_Feed_Details.apply(
        Extract_New_Feed_Details_Context(context.spark,
                                         context.config.Extract_New_Feed_Details
        ),
        df_Reformat_Get_File_Date_from_DB_Directory_Reformat,
        df_Get_Filelists_from_Filesets
      )
    val df_fbe_File_Date_Before_Mandatory_Cutoff =
      fbe_File_Date_Before_Mandatory_Cutoff(context,
                                            df_Extract_New_Feed_Details_out
      )
    val df_fbe_File_Date_Before_Mandatory_Cutoff_Reformat_out =
      fbe_File_Date_Before_Mandatory_Cutoff_Reformat_out(
        context,
        df_fbe_File_Date_Before_Mandatory_Cutoff
      )
    val df_Join_Establish_If_all_Expected_Files_Are_in_Place_Reformat_1 =
      Join_Establish_If_all_Expected_Files_Are_in_Place_Reformat_1(
        context,
        df_Get_Filelists_from_Filesets
      )
    val df_Join_Establish_If_all_Expected_Files_Are_in_Place_Reformat_0 =
      Join_Establish_If_all_Expected_Files_Are_in_Place_Reformat_0(
        context,
        df_Extract_New_Feed_Details_out1
      )
    val df_Join_Establish_If_all_Expected_Files_Are_in_Place_Join =
      Join_Establish_If_all_Expected_Files_Are_in_Place_Join(
        context,
        df_Join_Establish_If_all_Expected_Files_Are_in_Place_Reformat_1,
        df_Join_Establish_If_all_Expected_Files_Are_in_Place_Reformat_0
      )
    val df_Join_Establish_If_all_Expected_Files_Are_in_Place_Filter_Unused_1 =
      Join_Establish_If_all_Expected_Files_Are_in_Place_Filter_Unused_1(
        context,
        df_Join_Establish_If_all_Expected_Files_Are_in_Place_Join
      )
    val df_Join_Establish_If_all_Expected_Files_Are_in_Place_Filter_Unused_1_Final_Reformat =
      Join_Establish_If_all_Expected_Files_Are_in_Place_Filter_Unused_1_Final_Reformat(
        context,
        df_Join_Establish_If_all_Expected_Files_Are_in_Place_Filter_Unused_1
      )
    val (df_fbe_File_Date_On_or_After_Mandatory_Cut_toff_White_List_out0,
         df_fbe_File_Date_On_or_After_Mandatory_Cut_toff_White_List_out1
    ) = fbe_File_Date_On_or_After_Mandatory_Cut_toff_White_List(
      context,
      df_Join_Establish_If_all_Expected_Files_Are_in_Place_Filter_Unused_1_Final_Reformat
    )
    val df_fbe_File_Date_On_or_After_Mandatory_Cut_toff_White_List_Reformat_out0 =
      fbe_File_Date_On_or_After_Mandatory_Cut_toff_White_List_Reformat_out0(
        context,
        df_fbe_File_Date_On_or_After_Mandatory_Cut_toff_White_List_out0
      )
    val df_Rollup_Incomplete_Source_Extracts_File_Datess_UnionAll =
      Rollup_Incomplete_Source_Extracts_File_Datess_UnionAll(
        context,
        df_fbe_File_Date_Before_Mandatory_Cutoff_Reformat_out,
        df_fbe_File_Date_On_or_After_Mandatory_Cut_toff_White_List_Reformat_out0
      )
    val df_Rollup_Incomplete_Source_Extracts_File_Datess =
      Rollup_Incomplete_Source_Extracts_File_Datess(
        context,
        df_Rollup_Incomplete_Source_Extracts_File_Datess_UnionAll
      )
    val df_Rollup_Incomplete_Source_Extracts_File_Datess_Reformat =
      Rollup_Incomplete_Source_Extracts_File_Datess_Reformat(
        context,
        df_Rollup_Incomplete_Source_Extracts_File_Datess
      )
    val df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Reformat_1 =
      Join_Strip_Out_Incomplete_or_Unstable_Feeds_Reformat_1(
        context,
        df_Rollup_Incomplete_Source_Extracts_File_Datess_Reformat
      )
    val df_Join_Establish_If_all_Expected_Files_Are_in_Place_Filter =
      Join_Establish_If_all_Expected_Files_Are_in_Place_Filter(
        context,
        df_Join_Establish_If_all_Expected_Files_Are_in_Place_Join
      )
    val df_Join_Establish_If_all_Expected_Files_Are_in_Place_Final_Join_Reformat =
      Join_Establish_If_all_Expected_Files_Are_in_Place_Final_Join_Reformat(
        context,
        df_Join_Establish_If_all_Expected_Files_Are_in_Place_Filter
      )
    val df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Reformat_0 =
      Join_Strip_Out_Incomplete_or_Unstable_Feeds_Reformat_0(
        context,
        df_Join_Establish_If_all_Expected_Files_Are_in_Place_Final_Join_Reformat
      )
    val df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Join =
      Join_Strip_Out_Incomplete_or_Unstable_Feeds_Join(
        context,
        df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Reformat_1,
        df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Reformat_0
      )
    val df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter_Unused_0 =
      Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter_Unused_0(
        context,
        df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Join
      )
    val df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter_Unused_0_Final_Reformat =
      Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter_Unused_0_Final_Reformat(
        context,
        df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter_Unused_0
      )
    val df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter =
      Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter(
        context,
        df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Join
      )
    val df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Final_Join_Reformat =
      Join_Strip_Out_Incomplete_or_Unstable_Feeds_Final_Join_Reformat(
        context,
        df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter
      )
    val df_Check_Predecessor_Complete = Check_Predecessor_Complete.apply(
      Check_Predecessor_Complete_Context(
        context.spark,
        context.config.Check_Predecessor_Complete
      ),
      df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Filter_Unused_0_Final_Reformat,
      df_Join_Strip_Out_Incomplete_or_Unstable_Feeds_Final_Join_Reformat
    )
    val df_Replicate_1_RowDistributor_UnionAll =
      Replicate_1_RowDistributor_UnionAll(
        context,
        df_Check_Predecessor_Complete,
        df_fbe_File_Date_On_or_After_Mandatory_Cut_toff_White_List_out1
      )
    val df_Reformat_Missing_Files_Message_Reformat =
      Reformat_Missing_Files_Message_Reformat(
        context,
        df_Join_Establish_If_all_Expected_Files_Are_in_Place_Filter_Unused_1_Final_Reformat
      )
    Write_Missing_Files_to_Stdout(context,
                                  df_Reformat_Missing_Files_Message_Reformat
    )
    val df_Reformat_Active_Files_Message_Reformat =
      Reformat_Active_Files_Message_Reformat(context,
                                             df_Extract_New_Feed_Details_out
      )
    Write_Files_Still_Active_Files_to_Stdout(
      context,
      df_Reformat_Active_Files_Message_Reformat
    )
    Write_Db_and_Date_Info_to_Stdout(
      context,
      df_Reformat_Get_File_Date_from_DB_Directory_Reformat
    )
    Write_Incomplete_Date_Info_to_Stdout(
      context,
      df_Rollup_Incomplete_Source_Extracts_File_Datess_Reformat
    )
    Outputs.apply(Outputs_Context(context.spark, context.config.Outputs),
                  df_Replicate_1_RowDistributor_UnionAll
    )
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.legacy.timeParserPolicy",     "LEGACY")
    spark.conf.set("prophecy.metadata.pipeline.uri",        "pipelines/detect_feed_v3")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/detect_feed_v3")
    apply(context)
    MetricsCollector.end(spark)
  }

}
