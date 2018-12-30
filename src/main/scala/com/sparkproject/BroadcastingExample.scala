package com.sparkproject

import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession



object BroadcastingExample {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Accumulator Demo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.appName("BroadcastVariables").getOrCreate()
    // Register state data and schema as broadcast variables
    val localDF = spark.read.json("src\\main\\resources\\us_states.json")

    val broadcastStateData = sc.broadcast(localDF.collectAsList())
    val broadcastSchema = sc.broadcast(localDF.schema)

    localDF.printSchema()

    // Create a DataFrame based on the store locations.
    val storesDF = spark.read.json("src\\main\\resources\\store_locations.json")

    // Create a DataFrame of US state data with the broadcast variables.
    val stateDF = spark.createDataFrame(broadcastStateData.value, broadcastSchema.value)

    // Join the DataFrames to get an aggregate count of stores in each US Region
    println("How many stores are in each US region?")
    val joinedDF = storesDF.join(stateDF, "state").groupBy("census_region").count()
    joinedDF.show()

    spark.stop()

  }
}