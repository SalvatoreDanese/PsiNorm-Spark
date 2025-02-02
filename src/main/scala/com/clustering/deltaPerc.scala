package com.clustering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Random

object deltaPerc {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PsiNormTest1").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val data1 = sc.textFile("normalizedSpark.csv")
    val data2 = sc.textFile("normalizedR.csv")

    val parsedData1: Array[Array[Double]] = data1.map(s => (s.split(',').map(_.toDouble))).collect()
    val parsedData2: Array[Array[Double]] = data2.map(s => (s.split(',').map(_.toDouble))).collect()

    val nRows = parsedData1.apply(0).length
    val temp = Array.fill[Double](nRows)(0)
    var max = 0.0
    var mean = 0.0

    for(i<-0 until parsedData1.length){
      var arr1 = parsedData1.apply(i)
      var arr2 = parsedData2.apply(i)
      for(j<-0 until arr1.length){
        if(arr1(j)!= 0.0){
          temp(j) = ((arr2(j)-arr1(j))/arr1(j)) * 100
        }
        else{
          temp(j) = (arr2(j) - arr1(j)) * 100
        }
      }
      mean += temp.sum/temp.size
      if(max < temp.max){
        max = temp.max
      }

    }

    println("Max delta: "+ max+"%")
    println("Mean delta: "+ mean/parsedData1.length+"%")


  }

}
