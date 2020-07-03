import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)
    // RDD[String]
    val textFile = sc.textFile(args(0))
    // RDD[Array[String]]
    val line = textFile.map(line => line.split(","))
    val rdd = line.flatMap(line => {
         val movieName = line(0)
         val ratings = line.slice(1, line.size)
         val maxRating = ratings.max

        })


    // modify this code
    //val output = textFile.map(x => x)
    val output = rdd
    output.saveAsTextFile(args(1))
  }
}
