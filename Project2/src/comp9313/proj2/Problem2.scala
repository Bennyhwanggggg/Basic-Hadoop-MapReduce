package comp9313.proj2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import scala.util.control.Breaks._

object Problem2 {
  
  
	def main(args: Array[String]) {
    val inputFile = args(0)
    val k = args(1).toInt
    val conf = new SparkConf().setAppName("Project 1 problem 2").setMaster("local")
    val sc = new SparkContext(conf)
    
    
    
    val input = sc.textFile(inputFile)
    val lines = input.map( x => x.split(" "))
    
    // [1]: from node, [2]: to node
    val nodeArr = lines.map( e => (e(1).toLong, List(List()) ) )
    val edgeArr = lines.map( e => Edge(e(1).toLong, e(2).toLong, 1) ) // since distance is not important, just set to 1
   
    val nodes: RDD[(Long, List[List[Int]])] = sc.parallelize(nodeArr.collect())
    val edges: RDD[Edge[Int]] = sc.parallelize(edgeArr.collect())
    val graph = Graph(nodes, edges)
    
    val cycleDetection = graph.pregel(List(List[Int]()), k, EdgeDirection.Out)(
        (id, VD, A) => A,  // Don't need old information so completely replace them
        triplet => {
          // init
          var toSend = List(List[Int]())
          var sequence = List[Int]()
          var attributes = triplet.srcAttr
          // Go through each path and add the current point to the path if it satisfy the non-cycle condition
          for (sequence <- attributes) {
            // Don't send cycles. If the sequence already contains the srcId then it already has a cycle
            if (!sequence.contains(triplet.srcId.toInt) && !sequence.isEmpty) {
              var seq = sequence
              seq = seq ::: List(triplet.srcId.toInt)
              toSend = toSend :+ seq
            }
          }
          // If no attribute, send itself (usually first message)
          if (toSend.length <= 1){
            Iterator((triplet.dstId, List(List(triplet.srcId.toInt))))
          } else {
            Iterator((triplet.dstId, toSend))
          }

        },
        (a, b) => a ::: b
    )
 
    // collect the vertices attributes and filter by length and first number then sort then distinct then length = result
    val nCycles = cycleDetection.vertices.collect().map( x => {
      (x._1, x._2.filter { a => (!a.isEmpty && a(0) == x._1 && a.length == k) } )
    })  
    val result = nCycles.flatMap(x => x._2.map( a => a.sorted)).distinct.length
    println(result)
  }
}