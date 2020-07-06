import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)
    // RDD[String]
    val textFile = sc.textFile(args(0))
    // RDD[Array[String]]
    val token = textFile.map(line => line.split(","))
    val name = movie_name = token.first()
    val rating = token.drop(1)
    println(rating)
    val rating_with_index_sorted = rating.zipWithIndex.sortByKey(False).map((r,i) => (i,r))
    println(rating_with_index_sorted)

    val highest = rating_with_index_sorted.filter((i,r)=>if(r-rating_with_index_sorted.first()._2==0) true else false).map((i,r)=>i)

    println(highest)

    val rdd = name.union(highest)

    // modify this code
    //val output = textFile.map(x => x)
    val output = rdd
    output.saveAsTextFile(args(1))
  }
}
