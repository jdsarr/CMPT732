import java.io.IOException;

 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Random;

public class EulerEstimator extends Configured implements Tool {

	   public static class EulerMapper
	    extends Mapper<LongWritable, Text, Text, LongWritable>{
		   
		   private int hash;
		   private long keyCode;
		   private Random rand;
		   private long iter;
		   private long count;
		   private double sum;
	 
	        @Override
	        public void map(LongWritable key, Text value, Context context
	                ) throws IOException, InterruptedException {
	        	
	        	hash = ((FileSplit) context.getInputSplit()).getPath().getName().hashCode();
	        	keyCode = key.get();
	        	rand = new Random(keyCode*((long)hash));
	        	iter = Long.parseLong(value.toString());
	        	count = 0;
	        	
	        	for(long i = 0; i<iter; i++){
	        		sum = 0;
	        		
	        		while(sum < 1){
	        			sum += rand.nextDouble();
	        			count++;
	        		}
	        	}
	        	
	        	context.getCounter("Euler", "iterations").increment(iter);
	        	context.getCounter("Euler", "count").increment(count);        	
	        	
	        }
	   }
	   

	    
	    public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new EulerEstimator(), args);
	        System.exit(res);
	    }
	 
	    @Override
	    public int run(String[] args) throws Exception {
	        Configuration conf = this.getConf();
	        Job job = Job.getInstance(conf, "EulerEstimator");
	        job.setJarByClass(EulerEstimator.class);
	 
	        job.setInputFormatClass(TextInputFormat.class);
	 
	        job.setMapperClass(EulerMapper.class);

	        job.setOutputFormatClass(NullOutputFormat.class);
	        TextInputFormat.addInputPath(job, new Path(args[0]));
	 
	        return job.waitForCompletion(true) ? 0 : 1;
	    }
	
}
