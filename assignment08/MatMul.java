import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MatMul {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString().substring(1, -1), ",");
      ArrayList<String> myArray = new ArrayList<String>();
      while (itr.hasMoreTokens()) {
        myArray.add(itr.nextToken());
      }
      if (myArray.get(0) == "a") {
          for (int i = 0; i < 5; i++) {
              Text key = new Text();
              Text value = new Text();
              key.set(myArray.get(1) + "," + i);
              value.set(myArray.get(0) + "," + myArray.get(2) + "," + myArray.get(3));
          }
      }
      else if (myArray.get(0) == "b") {
        for (int i = 0; i < 5; i++) {
            Text key = new Text();
            Text value = new Text();
            key.set(i + "," + myArray.get(2));
            value.set(myArray.get(0) + "," + myArray.get(1) + "," + myArray.get(3));
        }
      }
      context.write(key, value);
    }
  }
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      ArrayList<int> arrayA = new ArrayList<int>();
      ArrayList<int> arrayB = new ArrayList<int>();
      for (int i = 0; i < 5; i++) {
          arrayA.add(0);
          arrayB.add(0);
      }
      for (IntWritable value : values) {
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
        ArrayList<String> myArray = new ArrayList<String>();
        while (itr.hasMoreTokens()) {
            myArray.add(itr.nextToken());
        }
        if (myArray.get(0) == "a") {
            arrayA.set(myArray.get(1), myArray.get(2));
        }
        else if (myArray.get(0) == "a") {
            arrayB.set(myArray.get(1), myArray.get(2));
        }
      }
      for (int i = 0; i < 5; i++) {
        sum += arrayA.get(i) * arrayB.get(i); 
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    // to reduce network bottleneck
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
