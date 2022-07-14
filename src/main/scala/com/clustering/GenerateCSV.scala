package com.clustering

import org.apache.spark.sql.catalyst.expressions.Rand

import java.io.PrintWriter
import scala.util.Random

object GenerateCSV {
  def main(args: Array[String]): Unit = {

    val pw: PrintWriter = new PrintWriter("generated.csv")
    val rand = new Random()
    for (i <- 0 until 10000) {
      for (j <- 0 until 100000) {
        var x = rand.nextInt(3001)
        val y = rand.nextInt(10)
        if (y < 8) {
          x = 0
        }
        if (j == 99999) {
          pw.println(x)
        } else {
          pw.print(x + ",")
        }
      }

    }
  }

}
