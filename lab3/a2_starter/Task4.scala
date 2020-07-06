import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {

  def compare(a: Array[String], b: Array[String]): String={
    var movie_1=a(0);
    var movie_2=b(0);
    var similarity=0;
    for(i <- 1 until a.length){
      if(a(i)==b(i) && a(i)!="" and b(i)!=""){
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
    val output = textFile.map(x => {
      var pairs = x.map(l => l.split(","));
      var cross = pairs.cartesian(pairs).filter(t => t_1(0).compareTo(t_2(0))<=0);
      var res = cross.map(x => compare(x_1,x_2));
    });
    
    output.saveAsTextFile(args(1))
  }
}
