import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task3 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code

    def mapIdtoRating(line: String): Array[(Int, Int)] = {
      val ratings = line.split(",", -1)
                        .zipWithIndex
                        .drop(1)
      // val nonEmptyRatings = ratings.filter(!_._1.isEmpty)
      val result = ratings.map{case(x,count) => if (x.isEmpty) (count,0) else (count,1)}
      return result
    }

    val output = textFile.flatMap(mapIdtoRating)
                         .reduceByKey(_+_)
                         .map(x => x._1 + "," + x._2)

    output.saveAsTextFile(args(1))
  }
}
