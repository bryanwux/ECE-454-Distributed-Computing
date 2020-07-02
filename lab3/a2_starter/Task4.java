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
    private Text movieName;
    private IntWritable movieSimilarity; 
    private List<String> cache;
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

  public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException{
      String[] token1 = value.toString().split(",");
      String title1 = token1[0];
      for (String c : cache) {
        String[] token2 = c.split(",");
        String title2 = token2[0];

        // maintain lexicographic order between titles
        if (titleA.compareTo(titleB) < 0){
          int similarity = computeSimilarity(token1,token2);
          movieName.set(title1 + "," + title2);
          movieSimilarity.set(similarity);
          context.write(movieName, movieSimilarity);
        }
        else
          continue;
      }
    }

    public int computeSimilarity(String[] token1, String[] token2){
      int counter = 0;
      for (int i=1; i<token1.length; i++){
        String rating1 = token1[i];
        String rating2 = token2[i];
        if(rating1 == rating2 && !rating1.isEmpty() && ! rating2.isEmpty()){
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
    job.setNumReduceTasks(0);

    job.setMapperClass(MovieSimilarityMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
