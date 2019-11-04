package com.wj.dmp.procity

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ProAndCityCount2Mysql {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        """
          | 输入参数不合法，缺少必要的参数
          | inputPath
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

    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.pwd"))

    // mode 设置数据写入的模式
    result.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"), load.getString("jdbc.table"), props)

    spark.stop()
  }
}
