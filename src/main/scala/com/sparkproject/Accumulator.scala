package com.sparkproject
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object Accumulator {
    def main(args:Array[String]):Unit={
        Logger.getLogger("org").setLevel(Level.ERROR)
        val conf = new SparkConf().setAppName("Accumulator Demo").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val input = sc.textFile("src\\main\\resources\\input.txt")

        fnAccumulator(sc,input)
    }

    def fnAccumulator(sc:SparkContext,inputrdd:RDD[String])
    {
        val blankLinesAccumulator = sc.accumulator(0, "Blank Lines")

        inputrdd.foreach(line => {if(line.isEmpty) blankLinesAccumulator += 1})
        print(s"No of blank lines $blankLinesAccumulator")

    }
}
