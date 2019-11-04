package com.wj.dmp.parquet

import com.wj.dmp.utils.{NumUtils, SchemaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Log2ParquetV1 {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          | 输入参数不合法，缺少必要的参数
          | inputPath
          | outputPath
          |""".stripMargin)

      System.exit(-1)
    }

    val Array(logInputPath, resultOutputPath) = args

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    // 导入隐式转换
    import spark.implicits._

    val logDs: Dataset[String] = spark.read.textFile(logInputPath)

    val rddRow: RDD[Row] = logDs.map(_.split(",", -1)).filter(_.length > 80).rdd.map(arr => {
      Row(
        arr(0),
        NumUtils.toInt(arr(1)),
        NumUtils.toInt(arr(2)),
        NumUtils.toInt(arr(3)),
        NumUtils.toInt(arr(4)),
        arr(5),
        arr(6),
        NumUtils.toInt(arr(7)),
        NumUtils.toInt(arr(8)),
        NumUtils.toDouble(arr(9)),
        NumUtils.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        NumUtils.toInt(arr(17)),
        arr(18),
        arr(19),
        NumUtils.toInt(arr(20)),
        NumUtils.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        NumUtils.toInt(arr(26)),
        arr(27),
        NumUtils.toInt(arr(28)),
        arr(29),
        NumUtils.toInt(arr(30)),
        NumUtils.toInt(arr(31)),
        NumUtils.toInt(arr(32)),
        arr(33),
        NumUtils.toInt(arr(34)),
        NumUtils.toInt(arr(35)),
        NumUtils.toInt(arr(36)),
        arr(37),
        NumUtils.toInt(arr(38)),
        NumUtils.toInt(arr(39)),
        NumUtils.toDouble(arr(40)),
        NumUtils.toDouble(arr(41)),
        NumUtils.toInt(arr(42)),
        arr(43),
        NumUtils.toDouble(arr(44)),
        NumUtils.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        NumUtils.toInt(arr(57)),
        NumUtils.toDouble(arr(58)),
        NumUtils.toInt(arr(59)),
        NumUtils.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        NumUtils.toInt(arr(73)),
        NumUtils.toDouble(arr(74)),
        NumUtils.toDouble(arr(75)),
        NumUtils.toDouble(arr(76)),
        NumUtils.toDouble(arr(77)),
        NumUtils.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        NumUtils.toInt(arr(84))
      )
    })

    val dataFrame = spark.createDataFrame(rddRow, SchemaUtils.logSchema)

    dataFrame.write.parquet(resultOutputPath)

    spark.stop()
  }

}
