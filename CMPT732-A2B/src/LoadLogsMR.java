import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class LoadLogsMR extends Configured implements Tool {
	
	public static class LoadLogsReducer
		extends TableReducer<LongWritable, Text, LongWritable> {
		   		   
		@Override
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			try{
				for(Text val: values){
					context.write(key,LoadLogs.get_put(val.toString()));
				}
			}catch(Exception e){
				System.out.println(e.getMessage());
			}   
		} 		
	}
	   
	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LoadLogsMR(), args);
        System.exit(res);
    }
 
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Load Logs MR");
        job.setJarByClass(LoadLogsMR.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initTableReducerJob(args[1], LoadLogsReducer.class, job);
        job.setNumReduceTasks(3);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        	
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Mutation.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
	
}
