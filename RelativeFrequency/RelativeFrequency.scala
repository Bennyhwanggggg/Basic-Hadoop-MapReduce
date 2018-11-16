import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._


object RelativeFrequency {
  
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("RelativeFrequency").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)

    val lines = textFile.map( x => x.toLowerCase.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").
         filter( x=> x.length >= 1).filter(x => x.charAt(0) <= 'z' && x.charAt(0) >= 'a'))
 
    val pairs = lines.map( x => for(i <- 0 to x.length-1; j <- i+1 to x.length-1) yield (x(i), x(j)))
    
    // (pair, sum)
    val coOccurCount = pairs.map( x=> x.toList).flatMap(x => x.map(x => (x, 1))).
                          reduceByKey(_+_)
                
    // (word, sum)                      
    val totalCount = coOccurCount.map( x => (x._1._1, x._2)).reduceByKey(_+_)
    val coOccurCountModified = coOccurCount.map( x => (x._1._1, (x._1, x._2)))

    // (word, (wordcount, (pair, paircount))
    val joined = totalCount join coOccurCountModified
   
    // sort by first word, value, then second word
    val relativeFrequency = joined.map( x => (x._2._2._1, (x._2._2._2.toDouble/x._2._1.toDouble))).sortByKey().sortBy{ case (k, v) => (k._1, -v) }
    
    val formatedResult = relativeFrequency.map( x => x._1._1 + " " + x._1._2 + " " + x._2).saveAsTextFile(outputFolder)

  }
}
