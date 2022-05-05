import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    //Set application name
    val conf = new SparkConf().setAppName("Simple Application")
    //Put SparkContext into a value
    val sc = new SparkContext(conf)
    //create an RDD by taking all of the books in the HDFS books directory
    val myRDD = sc.textFile("hdfs://hadoopmaster:9000/user/hadoop/books/*.txt")
    //Take RDD and begin data sorting
    val myMappedRDD = myRDD
      //Use a space as a delimiter to take words from the books
      .flatMap(line => line.split(" "))
      //Map each word to one row, with a keypair assigned to it
      .map(word => (word,1))
      //Reduce so that all exact word matches are placed together, with their value to the right
      .reduceByKey(_+_,1)
      //Sort items in the RDD
      .map(item => item.swap)
      //Place all items in one ascending partition based on their keypair value
      .sortByKey(true,1)
      .map(item => item.swap)
      //Place all parts of the RDD into one file
      .coalesce(1,false)
      //Save the single text file RDD to the HDFS
      .saveAsTextFile("hdfs://hadoopmaster:9000/user/hadoop/rdd")
    sc.stop()
  }
}