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

public class MinMaxMeanNasa {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	String[] tokens = value.toString().split("\\s+");
	word.set("FileSize");
	if(tokens.length==10){
		if(!tokens[9].equals("-"))
			context.write(word, new IntWritable(Integer.parseInt(tokens[9])));
	}
	else
		context.write(word, new IntWritable(0));

    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

	double count = 0.0;
        double sum =0.0;
        int min = Integer.MAX_VALUE;
        int max=0;
      	for (IntWritable val : values) {
		sum += val.get();
		count++;
		if(val.get()>max) max=val.get();
		if(val.get()!=0){
			if(val.get()<min)
			{
		 		min=val.get();
			}
	}

      }

      result.set((int)(sum/count));
      context.write(new Text("Mean"),result);

      result.set(max);
      context.write(new Text("Max"), result);

      result.set(min);
      context.write(new Text("Min"), result);

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Calculate Mean, Max, Min");
    job.setJarByClass(MinMaxMeanNasa.class);
    job.setMapperClass(TokenizerMapper.class);
  //  job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
