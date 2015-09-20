import java.io.IOException;

 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {

	   public static class MaxWikiMapper
	    extends Mapper<LongWritable, Text, Text, LongWritable>{
	 
	        private final static LongWritable one = new LongWritable();
	        private Text word = new Text();
	        private String[] array = new String[4];
	 
	        @Override
	        public void map(LongWritable key, Text value, Context context
	                ) throws IOException, InterruptedException {
	        	
	        	array = value.toString().split(" ");
	        	
	        	if(array[0].equals("en") && !array[1].equals("Main_Page") && 
	        	   !array[1].startsWith("Special:")){
	        		
	        		String filename = ((FileSplit) context.getInputSplit()).getPath().getName().substring(11, 22);	        		
	        		one.set(Long.parseLong(array[3]));        		
	        		word.set(filename);
	        		context.write(word, one);     		
	        	}	             	
	        }
	    }
	   
	   
	   
	   public static class MaxWikiReducer
	    extends Reducer<Text, LongWritable, Text, LongWritable> {
		   private LongWritable result = new LongWritable();
		   
		   @Override
	        public void reduce(Text key, Iterable<LongWritable> values,
	                           Context context) throws IOException, InterruptedException {
	            long max = 0;
	            for (LongWritable val : values) {
	                if(val.get()>max)
	                	max = val.get();
	            }
	            
	            result.set(max);
	            context.write(key, result);
	        }   
	   }
	   

	    
	    public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
	        System.exit(res);
	    }
	 
	    @Override
	    public int run(String[] args) throws Exception {
	        Configuration conf = this.getConf();
	        Job job = Job.getInstance(conf, "wikipedia popular");
	        job.setJarByClass(WikipediaPopular.class);
	 
	        job.setInputFormatClass(TextInputFormat.class);
	 
	        job.setMapperClass(MaxWikiMapper.class);
	        job.setCombinerClass(MaxWikiReducer.class);
	        job.setReducerClass(MaxWikiReducer.class);
	 
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(LongWritable.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        TextInputFormat.addInputPath(job, new Path(args[0]));
	        TextOutputFormat.setOutputPath(job, new Path(args[1]));
	 
	        return job.waitForCompletion(true) ? 0 : 1;
	    }
	
}
