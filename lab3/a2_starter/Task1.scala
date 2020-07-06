import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)
    // RDD[String]
    val textFile = sc.textFile(args(0))
    // RDD[Array[String]]
    val token = textFile.map(line => line.split("\n"))

    for(line <- token){
        println(line)
        //val segs = line.toString.map(x => x.split(","))
        //println(segs.toString)
        //val movie_name = segs.first()
        //println(movie_name.toString)
        //val rating_with_index_sorted = segs.zipWithIndex.mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,preservesPartitioning = true).sortByKey(false).map((r,i) => (i,r))
        //println(rating_with_index_sorted)
        //val highest = rating_with_index_sorted.filter(f=>if(f._2-rating_with_index_sorted.first()._2==0) true else false).map((i,r)=>i)
        //println(highest)
        //val result = movie_name.union(highest).collect()
        //val rdd = rdd.union(result)
    }

    //filter(f=>if(f._2-rating_with_index_sorted.first()._2==0) true else false).map((i,r)=>i)
    //val movie_name = token.first()

    //val rating = token.zipWithIndex.mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it,preservesPartitioning = true)
    //println(rating.toString)
    //val rating_with_index_sorted = rating_t.sortByKey(False).map((r,i) => (i,r))
    //println(rating_with_index_sorted)
    //val highest = rating_with_index_sorted.filter(f=>if(f._2-rating_with_index_sorted.first()._2==0) true else false).map((i,r)=>i)

    //println(highest)

    //val rdd = result
    //val rdd = movie_name.union(highest)

    // modify this code
    //val output = textFile.map(x => x)
    val output = token
    output.saveAsTextFile(args(1))
  }
}
