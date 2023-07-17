package udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("get_fileset_version_inner", get_fileset_version_inner)
    registerAllUDFs(spark)
  }

  def get_fileset_version_inner = {
    udf(
      (
        _p_filedate:                String,
        _p_config_file:             String,
        _p_default_fileset_version: String,
        _p_feed_identifier:         String
      ) => {
        var p_filedate                               = _p_filedate
        var p_config_file                            = _p_config_file
        var p_default_fileset_version                = _p_default_fileset_version
        var p_feed_identifier                        = _p_feed_identifier
        var fileset_version_count                    = 0
        var fileset_version_rules_current            = Row("", "", "")
        var fileset_version_rules_current_vec        = Array[Row]()
        var fileset_version_rules_current_vec_sorted = Array[Row]()
        val data =
          loadLookupData(readLookupFile(p_config_file.toString).toString, "|")
        var current_fileset_version = ""
        _createLookup(
          "fileset_version_lookup",
          loadLookupData(readLookupFile(p_config_file.toString).toString, "|"),
          List("feed_identifier"),
          "feed_identifier",
          "version_date",
          "version"
        )
        fileset_version_count =
          if (
            (try _lookup_count("fileset_version_lookup",
                               p_feed_identifier.toString
            )
            catch {
              case error: Throwable => null
            }) != null
          ) _lookup_count("fileset_version_lookup", p_feed_identifier.toString)
          else 0
        (0 until convertToInt(fileset_version_count)).zipWithIndex
          .map({
            case (_i, iIndex) =>
              var i = _i
              fileset_version_rules_current =
                new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(
                  Array(
                    _lookup_row("fileset_version_lookup", p_feed_identifier)(
                      convertToInt(i)
                    ).array(convertToInt(0)).toString,
                    _lookup_row("fileset_version_lookup", p_feed_identifier)(
                      convertToInt(i)
                    ).array(convertToInt(1)),
                    _lookup_row("fileset_version_lookup", p_feed_identifier)(
                      convertToInt(i)
                    ).array(convertToInt(2)).toString
                  ),
                  StructType(
                    List(StructField("feed_identifier", StringType, true),
                         StructField("version_date",    StringType, true),
                         StructField("version",         StringType, true)
                    )
                  )
                )
              fileset_version_rules_current_vec =
                Array.concat(fileset_version_rules_current_vec,
                             Array.fill(1)(fileset_version_rules_current)
                )
          })
          .toArray
        fileset_version_rules_current_vec_sorted =
          fileset_version_rules_current_vec.sortBy(x =>
            x.getAs[String]("version_date")
          )
        import scala.util.control._
        val outer_loop = new Breaks
        outer_loop.breakable {
          fileset_version_rules_current_vec_sorted.zipWithIndex
            .map({
              case (_ii, iiIndex) =>
                var ii = _ii
                import scala.util.control._
                val find_fileset_version = new Breaks
                find_fileset_version.breakable {
                  if (ii.getAs[String]("version_date") >= p_filedate) {
                    current_fileset_version = ii.getAs[String]("version")
                    outer_loop.break
                  }
                }
            })
            .toArray
        }
        if (
          (try current_fileset_version
          catch {
            case error: Throwable => null
          }) == null
        ) p_default_fileset_version
        else if (current_fileset_version.isEmpty) p_default_fileset_version
        else current_fileset_version
      },
      StringType
    )
  }

}

object PipelineInitCode extends Serializable {

  def get_db_file_date(
    p_db_name:           org.apache.spark.sql.Column,
    p_file_date_format:  org.apache.spark.sql.Column,
    p_landing_structure: org.apache.spark.sql.Column
  ) = {
    var v_file_name: org.apache.spark.sql.Column =
      element_at(split(p_db_name, "\\."), lit(1))
    var v_file_date: org.apache.spark.sql.Column =
      when(p_landing_structure === lit("PARTITION"),
           string_substring(
             v_file_name,
             string_rindex(v_file_name.cast(StringType), lit("=")) + lit(1),
             lit(1000)
           )
      ).otherwise(
        string_substring(
          v_file_name,
          string_rindex(v_file_name.cast(StringType), lit("_")) + lit(1),
          lit(1000)
        )
      )
    var v_file_date_format_vec: org.apache.spark.sql.Column =
      string_split_no_empty(p_file_date_format.cast(StringType), lit("|"))
    when(array_contains(v_file_date_format_vec, lit("YYYYMMDD")).and(
           string_length(v_file_date) === lit(8)
         ),
         v_file_date
    ).when(array_contains(v_file_date_format_vec, lit("YYYYMM"))
              .and(string_length(v_file_date) === lit(6)),
            concat(v_file_date, lit("01"))
      )
      .when(
        array_contains(v_file_date_format_vec, lit("DDMMYYYY"))
          .and(string_length(v_file_date) === lit(8)),
        date_format(to_date(v_file_date, "ddMMyyyy"), "yyyyMMdd")
          .cast(StringType)
      )
      .when(
        array_contains(v_file_date_format_vec, lit("YYYY-MM-DD"))
          .and(string_length(v_file_date) === lit(10)),
        date_format(to_date(v_file_date, "yyyy-MM-dd"), "yyyyMMdd")
          .cast(StringType)
      )
      .otherwise(v_file_date)
  }

}
