package comp9313.lab6

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]){
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val words = input.flatMap { line => line.split(" ") }
    val counts = words.map(word => (word, 1)).reduceByKey(_+_)
    counts.saveAsTextFile(outputFolder)
  }
}