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
import java.util.ArrayList;
public class MatMul {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "]");
      
      while (itr.hasMoreTokens()) {
        ArrayList<String> myArray = new ArrayList<String>();
        String substr = itr.nextToken();
        StringTokenizer temp = new StringTokenizer(substr.substring(1, substr.length()), ", ");
        while(temp.hasMoreTokens()) {
          myArray.add(temp.nextToken());
        }
        Text result_key = new Text();
        Text result_value = new Text();
        if (myArray.get(0).equals("\"a\"")) {
            for (int i = 0; i < 5; i++) {
                result_key.set(myArray.get(1) + "," + i);
                result_value.set(myArray.get(0) + "," + myArray.get(2) + "," + myArray.get(3));
            }
        }
        else if (myArray.get(0).equals("\"b\"")) {
          for (int i = 0; i < 5; i++) {
              result_key.set(i + "," + myArray.get(2));
              result_value.set(myArray.get(0) + "," + myArray.get(1) + "," + myArray.get(3));
          }
        }
        context.write(result_key, result_value);
        }
      
    }
  }
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      ArrayList<Integer> arrayA = new ArrayList<Integer>();
      ArrayList<Integer> arrayB = new ArrayList<Integer>();
      for (int i = 0; i < 5; i++) {
          arrayA.add(0);
          arrayB.add(0);
      }
      for (Text value : values) {
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
        ArrayList<String> myArray = new ArrayList<String>();
        while (itr.hasMoreTokens()) {
            myArray.add(itr.nextToken());
        }
        if (myArray.get(0).equals("\"a\"")) {
            arrayA.set(Integer.parseInt(myArray.get(1)), Integer.parseInt(myArray.get(2)));
        }
        else if (myArray.get(0).equals("\"b\"")) {
            arrayB.set(Integer.parseInt(myArray.get(1)), Integer.parseInt(myArray.get(2)));
        }
      }
      for (int i = 0; i < 5; i++) {
        sum += arrayA.get(i) * arrayB.get(i);
      }
      context.write(key, Integer.toString(sum));
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "multipy matrix");
    job.setJarByClass(MatMul.class);
    job.setMapperClass(TokenizerMapper.class);
    // to reduce network bottleneck
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
