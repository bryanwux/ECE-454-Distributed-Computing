import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)
    // RDD[String]
    val textFile = sc.textFile(args(0))
    // RDD[Array[String]]
    val token = textFile.flatMap(line => line.split(","))
    print(token)
    val movie_name = token.first()

    val rating = token.zipWithIndex.mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,preservesPartitioning = true)
    println(rating.toString)
    //val rating_with_index_sorted = rating_t.sortByKey(False).map((r,i) => (i,r))
    //println(rating_with_index_sorted)
    //val highest = rating_with_index_sorted.filter(f=>if(f._2-rating_with_index_sorted.first()._2==0) true else false).map((i,r)=>i)

    //println(highest)

    val rdd = rating
    //val rdd = movie_name.union(highest)

    // modify this code
    //val output = textFile.map(x => x)
    val output = rdd
    output.saveAsTextFile(args(1))
  }
}
