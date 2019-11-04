package com.wj.dmp.parquet

import com.wj.dmp.utils.{NumUtils, SchemaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Log2ParquetV2 {

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

    val rddLog: RDD[Log] = logDs.map(_.split(",", -1)).filter(_.length > 80).rdd.map(Log(_))

    val dataFrame = spark.createDataFrame(rddLog)

    dataFrame.write.partitionBy("provincename").parquet(resultOutputPath)

    spark.stop()
  }

}
