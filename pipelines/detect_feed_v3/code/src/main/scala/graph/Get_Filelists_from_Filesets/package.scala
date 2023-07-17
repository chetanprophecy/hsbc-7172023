package graph

import io.prophecy.libs._
import graph.Get_Filelists_from_Filesets.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Get_Filelists_from_Filesets {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_Sort_File_Date = Sort_File_Date(context, in)
    val df_Dedup_Sorted_File_Date =
      Dedup_Sorted_File_Date(context, df_Sort_File_Date)
    val df_Reformat_Read_Filesets_Assign_Feed_Date_Reformat =
      Reformat_Read_Filesets_Assign_Feed_Date_Reformat(context,
                                                       df_Dedup_Sorted_File_Date
      )
    val df_Normalize_Unpack_Fileset = Normalize_Unpack_Fileset(
      context,
      df_Reformat_Read_Filesets_Assign_Feed_Date_Reformat
    )
    val df_FBE_Strip_Out_blank_lines =
      FBE_Strip_Out_blank_lines(context, df_Normalize_Unpack_Fileset)
    val df_Redefine_Format =
      Redefine_Format(context, df_FBE_Strip_Out_blank_lines)
    val df_FB_Only_Keep_Files_Going_To_IPR =
      FB_Only_Keep_Files_Going_To_IPR(context, df_Redefine_Format)
    val df_Set_Up_Source_File_Names_Reformat =
      Set_Up_Source_File_Names_Reformat(context,
                                        df_FB_Only_Keep_Files_Going_To_IPR
      )
    df_Set_Up_Source_File_Names_Reformat
  }

}
