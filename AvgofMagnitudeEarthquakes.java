import java.io.IOException;
import java.util.StringTokenizer;

import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgofMagnitudeEarthquakes {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, FloatWritable>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
	//System.out.println(tokens[0]);
	//System.out.println(tokens[4]);
      String st = tokens[0];
	if (!st.equals("time"))	
	{
	      String string = st.substring(0,10);	
	
		word.set(string);      


		if(tokens[4] != null && !tokens[4].isEmpty()){
			//System.out.println(tokens[9]);
			FloatWritable magnitude = new FloatWritable(Float.parseFloat(tokens[4]));
			context.write(word, magnitude);
		}
		else
			context.write(word, new FloatWritable(0));
	    	}
	}
  }

  public static class IntSumReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0;
	float count=0;
	float avg;
      for (FloatWritable val : values) {
        sum += val.get();
	count++;
      }
      result.set(sum/count);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	conf.set("mapred.textoutputformat.separator", ",");
    Job job = Job.getInstance(conf, "Average Magnitude per day");
    job.setJarByClass(Earthquakes.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
