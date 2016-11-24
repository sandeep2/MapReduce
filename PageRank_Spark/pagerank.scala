package pagerankspark

/**
  * Created by sandeep on 11/6/16.
  */
import org.apache.spark.{SparkConf, SparkContext}

object pagerank {

  // calling the pagerankcl class with arguements: the path to wikipedia files,
  // the path to temperary file and also the path to where the files should be written.
  def main(args: Array[String]) {
      val pagerank = new pagerankcl(args(0),args(1),args(2))
  }
}
