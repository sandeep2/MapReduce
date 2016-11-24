package pagerankspark

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sandeep on 11/8/16.
  */
class pagerankcl(val x: String, val y: String, val z: String) {
  // Driver code
  val conf = new SparkConf().setMaster("local[*]").setAppName("pagerank")
  val spark = new SparkContext(conf)

  val input = spark.textFile(x)
  // Each line is parsed using parser which returns a string having url+each_url_in_adjacency_list+"\n"+...
  val tuple = input.map(line => new parser().parse(line)).filter(fields => fields!= null).persist()
  // This tuple is written to disk so that we can create tuple of tuples of all (url,outgoing_link_url) paris
  tuple.coalesce(1,true).saveAsTextFile(y)
  // reading from wiitten file
  val lines = spark.textFile(y+"/part-00000")
  // each tuple is split into url and it's outlink if it's not a dangling node and adjacency list is created
  // by grouping by the url.
  val links = lines.map{ s =>
    val parts = s.split(" ")
    if(parts.length > 1)
      (parts(0), parts(1))
    else
      (parts(0), null)
  }.distinct().groupByKey().cache()
  // Each url is given in a initial pagerank of 1.0
  var ranks = links.mapValues(v => 1.0)
  // Iterating 10 times the calcualion of page rank
  for (i <- 1 to 10) {
    // Creating a Accumulator variable to add the contirubution of dangling nodes
    val delta = spark.doubleAccumulator("delta")
    // if the size of the adjacency list is zero and then its pagerank contribution is added to delta
    val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
      val size = urls.size
      if (size == 0){
        delta.add(rank)
      }
      // distributing the pagerank of url to each of its outgoing lists.
      urls.map(url => (url, rank / size))
    }
    val deltaVal = delta.value
    // Reduce by key to calculate the incoming links pageranks and using the pagerank formula to find the pagerank.
    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _ + deltaVal)
  }

  val output = ranks.collect()
  // Sorting by decreasing order and then taking top 100 from the list
  val sorted = output.sortWith(_._2 > _._2 )
  val sorter = sorted.take(101).takeRight(100)
  // Writing the output to the disk
  val finale = sorter.map(t => t._1 +" "+t._2.toString+"\n")
  val pw = new PrintWriter(new File(z))
  finale.foreach(pw.write(_))
  pw.close
  spark.stop()
}
