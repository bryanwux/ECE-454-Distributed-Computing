import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ListBuffer}

// please don't change the object name
object Task1 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))

    val result = textFile.map(x => {
      var rating = x.split(",", -1);
      var movie_name = rating(0);
      var user_max = new ListBuffer[Int]();
      var best = -1;
      for(i <- 1 until rating.length){
        if(rating(i)!="") {
          if(rating(i).toInt>best){
            user_max.clear();
            user_max+=i;
            best=rating(i).toInt;
          }else if(rating(i).toInt==best){
            user_max+=i;
          }
        }
      }
      movie_name+","+user_max.mkString(",");
    })
    // modify this code
    val output = result
    output.saveAsTextFile(args(1))
  }
}
