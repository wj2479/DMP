package com.wj.dmp.procity

import com.wj.dmp.parquet.Log2ParquetV1.getClass
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ProAndCityCount2Json {

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

    val logDs: DataFrame = spark.read.parquet(logInputPath)

    logDs.createTempView("v_logs")

    val result: DataFrame = spark.sql("select Provincename,Cityname,count(*) as cnt  from v_logs  group by Provincename,Cityname")

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val path = new Path(resultOutputPath)
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }

    result.coalesce(1).write.json(resultOutputPath) // 合并成一个分区

    spark.stop()
  }
}
