import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {

  def compare(a: Array[String], b: Array[String]): String={
    var movie_1=a(0);
    var movie_2=b(0);
    var similarity=0;
    for(i <- 1 until a.length){
      if(a(i)==b(i) && a(i)!="" && b(i)!=""){
        similarity+=1;
      }
    }
    movie_1+","+movie_2+","+similarity;
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val pairs = textFile.map(l => l.split(","));
    val f = pairs.cartesian(pairs).filter(t => t._1(0).compareTo(t._2(0))<=0);
    val output = f.map(x => compare(x._1,x._2));
    
    output.saveAsTextFile(args(1))
  }
}
