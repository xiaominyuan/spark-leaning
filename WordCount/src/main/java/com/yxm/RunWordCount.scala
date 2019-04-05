package com.yxm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RunWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("log").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress","false")
    println("开始运行WORDCOUNT")
    val sc = new SparkContext(new SparkConf().setAppName("wordcount").setMaster("local[2]"))
    println("开始读取hdfs")
    val textFile = sc.textFile("hdfs://master:9000/user/hduser/test/LICENSE.txt",1)
    println("开始创建RDD")
    val countRDD = textFile.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => (x+y))
    println("开始保存文件")
    try{
      countRDD.saveAsTextFile("/usr/local/spark/workspace/WordCount/data/output")
    }catch {
      case e: Exception => println("输出目录已经存在")
    }
  }
}
