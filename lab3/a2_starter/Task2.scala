import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    val totalRatings = textFile
                        .flatMap(_.split(",").tail)
                        .filter(!_.isEmpty)
                        .count()

    // modify this code

    // Array => RDD
    sc.parallelize(totalRatings)
                .repartition(1)
                .saveAsTextFile(args(1))
  }
}
