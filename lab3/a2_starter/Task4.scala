import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val pairs = textFile.map(l => l.split(","));
    val f = pairs.cartesian(pairs).filter(t => t._1(0).compareTo(t._2(0))<0);
    val output = f.map(x => {
      var movie_1=x._1(0);
      var movie_2=x._2(0);
      var similarity=0;
      for(i <- 1 until x._1.length){
        if(x._1(i)==x._2(i) && x._1(i)!="" && x._2(i)!=""){
          similarity+=1;
        }
      }
      movie_1+","+movie_2+","+similarity;
    })
    
    output.saveAsTextFile(args(1))
  }
}
