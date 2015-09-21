import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class RedditAverage extends Configured implements Tool {
	
	public static class RedAvgMapper
    extends Mapper<LongWritable, Text, Text, LongPairWritable>{
 
        private final static LongPairWritable pair = new LongPairWritable();
        private Text word = new Text();
        private ObjectMapper json_mapper = new ObjectMapper();
 
 
        @Override
        public void map(LongWritable key, Text value, Context context) 
        		        throws IOException, InterruptedException {
        	
        	
        	JsonNode data = json_mapper.readValue(value.toString(), JsonNode.class);
        	word.set(data.get("subreddit").textValue());
        	pair.set(1,data.get("score").longValue());
        	
        	//Testing purposes for MultiLineJSONInputFormat
        	
        	//word.set(value);
        	//pair.set(1, 1);
        	
        	context.write(word, pair);
        }
    }
	
	
	public static class RedAvgCombiner 
		extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		   
		private LongPairWritable result = new LongPairWritable();
		
		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
	                           Context context) throws IOException, InterruptedException {
		
			long sum0 = 0;
			long sum1 = 0;
			   
			for (LongPairWritable val : values) {
	            	
				sum0+= val.get_0();
				sum1+= val.get_1();    
			}
	            
			result.set(sum0, sum1);     
			context.write(key, result);
		}   
	}
	
	public static class RedAvgReducer 
		extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
	   
		private DoubleWritable result = new DoubleWritable();
	   
		@Override
	    public void reduce(Text key, Iterable<LongPairWritable> values,
                           Context context) throws IOException, InterruptedException {
		   
		   long sum0 = 0;
		   long sum1 = 0;
		   double avg  = 0;
		   
           for (LongPairWritable val : values) {
            	
        	   sum0+= val.get_0();
        	   sum1+= val.get_1();
            }
           
           
           
           avg = ((double)sum1)/((double)sum0);
           result.set(avg); 
           context.write(key, result);
        }   
   }
	
	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "reddit average");
        job.setJarByClass(RedditAverage.class);
 
        job.setInputFormatClass(MultiLineJSONInputFormat.class);
 
        job.setMapperClass(RedAvgMapper.class);
        job.setCombinerClass(RedAvgCombiner.class);
        job.setReducerClass(RedAvgReducer.class);
        
        job.setMapOutputValueClass(LongPairWritable.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
