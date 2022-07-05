package com.clustering


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import scala.math._



object PsiNormRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PsiNormTest1").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)



    val data = sc.textFile("sc_10x.count.csv")

    val parsedData = data.map(s => Vectors.dense(s.split(';').map(_.toDouble))).cache()
    val nRows: Long = parsedData.count(); //count rows
    val rowsInt = nRows.toInt

    val collection = parsedData.collect()
    val sums: Array[Double] = new Array[Double](collection.apply(1).size)
    for(i <- 0 to rowsInt-1){
      var v = collection.apply(i)
      for(j <- 0 to v.size-1){
        sums(j) += log(v.apply(j) + 1)
      }
    }

    for(i <- 0 to rowsInt-1){
      var v = collection.apply(i).toArray
      for(j <- 0 to v.size-1){
        v(j) = v(j) * nRows / sums(j)
      }
    }

  }

}
