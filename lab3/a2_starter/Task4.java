import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {
  public static class MovieSimilarityMapper extends Mapper<Object, Text, Text, IntWritable>{
    private Text movieName = new Text();
    private IntWritable movieSimilarity = new IntWritable(); 
    private List<String> cache = new LinkedList<>();
  // called once at the beginning of each task
  @Override
  public void setup(Context context)
    throws IOException, InterruptedException{
      // cache input text file
      Path cahcedInput = context.getLocalCacheFiles()[0];
      BufferedReader reader = new BufferedReader(new FileReader(cahcedInput.toString()));
      cache.clear();
      String line;
      while ((line = reader.readLine()) != null) 
        cache.add(line);
  }

  // called once for each key-value pair in the input split
  public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException{
      String[] stringToken1 = value.toString().split(",", -1);
      String title1 = stringToken1[0];
      for (String c : cache) {
        String[] stringToken2 = c.split(",", -1);
        String title2 = stringToken2[0];

        // movie title should maintain ascending lexicographic order
        if (title1.compareTo(title2) < 0){
          int similarity = computeSimilarity(stringToken1,stringToken2);
          movieName.set(title1 + "," + title2);
          movieSimilarity.set(similarity);
          context.write(movieName, movieSimilarity);
        }
        else
          continue;
      }
    }
    // compute similarity between two movies
    public int computeSimilarity(String[] stringToken1, String[] stringToken2){
      int counter = 0;
      for (int i=1; i<stringToken1.length; i++){
        String rating1 = stringToken1[i];
        String rating2 = stringToken2[i];
        if(rating1.equals(rating2) && !rating1.isEmpty() && ! rating2.isEmpty()){
          counter++;
        }
      }
      return counter;
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task4");
    job.setJarByClass(Task4.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    // make the input file accessible to all the mappers for faster access of the data
    job.addCacheFile(new URI(otherArgs[0]));
  

    job.setMapperClass(MovieSimilarityMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}