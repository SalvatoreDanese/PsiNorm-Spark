package com.clustering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors


object KMeansExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KMeansExample").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val data = sc.textFile("data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val WSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSE")

    clusters.save(sc, "res")
    val sameModel = KMeansModel.load(sc, "res")

    sc.stop()
  }

}
