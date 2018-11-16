import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object SetSimJoin {
  
  def main(args : Array[String]) {
    
    // Take inputs
    val inputFile1 = args(0)
    val inputFile2 = args(1)
    val outputFolder = args(2)
    val thresholdValue = args(3).toDouble
    
    val conf = new SparkConf().setAppName("SetSimJoin").setMaster("local")
    val sc = new SparkContext(conf)
    val input1 = sc.textFile(inputFile1)
    val input2 = sc.textFile(inputFile2)
    
    // pre-processing -> id1 n1 n2 n3
    val rSet = input1.map(line => (line.split(" "))).persist()
    val sSet = input2.map(line => (line.split(" "))).persist()
    
    val pairs1 = rSet.flatMap(d => {
      // Generate pair up to the prefix position
      for (i <- 1 to ((d.length - 1) - (Math.ceil((d.length-1)*thresholdValue)).toInt + 1)) yield {
        // generate (n1, (id1, n1, n2, n3)), (n2, (id1, n1, n2, n3)) 
        (d(i).toInt, (d(0).toInt, d.drop(1).mkString(" ").toString()))
      }
    })
    
    val pairs2 = sSet.flatMap(d => {
      // Generate pair up to the prefix positin
      for (i <- 1 to ((d.length - 1) - (Math.ceil((d.length-1)*thresholdValue)).toInt + 1)) yield {
        // generate (n1, (id1, n1, n2, n3)), (n2, (id1, n1, n2, n3)) 
        (d(i).toInt, (d(0).toInt, d.drop(1).mkString(" ").toString()))
      }
    })
    
    val p2 = pairs2.groupByKey().flatMap(x => x._2.toList).collect().toList
    
    val res = pairs1.groupByKey().flatMap(x => {
      
      val p1 = x._2.toList
      
      for (i <- 0 to p1.length-1; j <- 0 to p2.length -1) yield {
         val k1 = p1(i)._1.toInt
         val k2 = p2(j)._1.toInt
         var k = (k1, k2)
         
         val l1 = p1(i)._2.split(" ")
         val l2 = p2(j)._2.split(" ")
         val v = l1.intersect(l2).length.toDouble/(l2.length+l1.length-l1.intersect(l2).length.toDouble)
         (k, v)
   
      }
    })
    
    res.filter(_._2 >= thresholdValue).map(e => (e, 1)).reduceByKey(_+_).map(_._1).sortBy(e => (e._1._1, e._1._2))
    .map(e => s"${e._1}\t${e._2}").saveAsTextFile(outputFolder)
  }
}
