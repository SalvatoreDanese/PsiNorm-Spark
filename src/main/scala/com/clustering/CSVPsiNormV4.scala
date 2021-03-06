package com.clustering

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.io.{File, PrintWriter}
import math.{log}
import scala.collection.mutable.ArrayBuffer

object CSVPsiNormV4 {
  var colSums: Broadcast[Array[Double]] = null
  var nRows: Broadcast[Long] = null

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PsiNormTest1").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val data = sc.textFile("sc_10x.count2.csv")

    val parsedData: RDD[Array[Double]] = data.map(s => (s.split(',').map(_.toDouble))).cache()
    val nCols: Int = parsedData.take(1)(0).length
    nRows = sc.broadcast(parsedData.count())

    val nRows2 = parsedData.count()

    val zero: Array[Double] = Array.fill[Double](nCols)(0)
    colSums = sc.broadcast(parsedData.mapPartitions(preProcessing).
      aggregate(zero)(aggregate, combine))

    val normalized = parsedData.mapPartitions(normalization).collect()


    val str = "norm.csv"
    val file: PrintWriter = new PrintWriter(new File("norm.csv"))

    for(i<- 0 until normalized.length) {
      var str = normalized.apply(i)
      for(j<-0 until str.length) {
        var x = str.apply(j)
        if(x==0.0){
          file.print(0)
        }
        else{
          if(x<1){
            x = BigDecimal(x).setScale(15, BigDecimal.RoundingMode.HALF_DOWN).toDouble
          }
          else{
            x = BigDecimal(x).setScale(14, BigDecimal.RoundingMode.HALF_UP).toDouble
          }
          file.print(x)
        }
        if(j!=str.length-1){
          file.print(",")
        }
      }
      if(i!= normalized.length-1){
        file.println("")
      }
    }
    file.close()


  }

  def preProcessing(it: Iterator[Array[Double]]): Iterator[Array[Double]] = {

    val arr: Array[Array[Double]] = new Array[Array[Double]](1)
    arr(0) = it.next()
    arr(0) = arr(0).map(d => log(d+1))
    while(it.hasNext) {
      arr(0) = arr(0).zip(it.next()).map { x => x._1 + log(x._2+1) }
    }

    arr.iterator

  }

  def normalization(it: Iterator[Array[Double]]): Iterator[Array[Double]] = {

    var arr: ArrayBuffer[Array[Double]] = new ArrayBuffer[Array[Double]]()
    while(it.hasNext) {
      arr.+=(it.next().zip(colSums.value).map{ x => x._1 * nRows.value/x._2})
    }

    arr.iterator
  }

  def aggregate(accu: Array[Double], arr: Array[Double]): Array[Double] = {
    for (i <- 0 to arr.length - 1) {
      accu(i) += arr(i)
    }
    accu
  }

  def combine(arr1: Array[Double], arr2: Array[Double]): Array[Double] = {
    for (i <- 0 to arr1.length - 1) {
      arr1(i) += arr2(i)
    }
    arr1
  }

}