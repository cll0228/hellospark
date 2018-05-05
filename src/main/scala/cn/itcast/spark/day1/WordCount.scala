package cn.itcast.spark.day1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/14.
  */
object WordCount {
  def main(args: Array[String]) {
    //非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WC").setMaster("local[2]")

    val sc = new SparkContext(conf)

    //    //textFile会产生两个RDD：HadoopRDD  -> MapPartitinsRDD
    //    sc.textFile(args(0)).cache()
    //      // 产生一个RDD ：MapPartitinsRDD
    //      .flatMap(_.split(" "))
    //      //产生一个RDD MapPartitionsRDD
    //      .map((_, 1))
    //      //产生一个RDD ShuffledRDD
    //      .reduceByKey(_+_)
    //      //产生一个RDD: mapPartitions
    //      .saveAsTextFile(args(1))
    val value: RDD[(String, Int)] = sc.textFile("c://a.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    print(value)
    sc.stop()
  }
}
