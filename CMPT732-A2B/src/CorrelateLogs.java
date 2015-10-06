import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CorrelateLogs extends Configured implements Tool {

	
	public static class CorrelateLogsMapper extends TableMapper<Text, LongPairWritable>{
		
		private Text host;
		private LongPairWritable pair;
		
		public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			
			 
			String hostString = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
										Bytes.toBytes("struct"),Bytes.toBytes("host"))));
			
			long bytesLong = Bytes.toLong(CellUtil.cloneValue(value.getColumnLatestCell(
										Bytes.toBytes("struct"),Bytes.toBytes("bytes"))));
			
			
			
			host.set(hostString);
			pair.set(1, bytesLong);
			context.write(host, pair);
		}
	}

	
	public static class CorrelateReducer 
	extends Reducer<Text, LongPairWritable, LongWritable, LongWritable> {
	   
		private LongWritable x = new LongWritable();
		private LongWritable y = new LongWritable();
	

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
                           Context context) throws IOException, InterruptedException {
	
			long sum0 = 0;
			long sum1 = 0;
		
			for (LongPairWritable val : values) {
            	
				sum0+= val.get_0();
				sum1+= val.get_1();    
			}
            
			x.set(sum0);
			y.set(sum1);
			context.write(x, y);
		}   
	}
	
	
	public static class ChainMapper
	extends Mapper<LongWritable,LongWritable, LongWritable, LongWritable> {
		
		long n;
		long Sx;
		long Sx2;
		long Sy;
		long Sy2;
		long Sxy;
		double r;
		double r2;
		
		
		protected void setup(Context context) throws IOException, InterruptedException {
			n = Sx = Sx2 = Sy = Sy2 = Sxy = 0;
		}
		
        public void map(LongWritable key, LongWritable value, Context context
                ) throws IOException, InterruptedException {
        	
        	n++;
        	Sx+=(key.get());
        	Sx2+=(key.get())^2;
        	Sy+=(value.get());
        	Sy2+=(value.get())^2;
        	Sxy+=(key.get()*value.get());
			
		}
        
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			 r = ((double)(n*Sxy-Sx*Sy))/
					 (Math.sqrt(n*Sx2-(Sx)^2)*Math.sqrt(n*Sy2-(Sy)^2));
			 r2 = r*r;
			 
			 System.out.println("n = " + n);
			 System.out.println("Sx = " + Sx);
			 System.out.println("Sx2 = " + Sx2);
			 System.out.println("Sy = " + Sy);
			 System.out.println("Sy2 = " + Sy2);
			 System.out.println("Sxy = " + Sxy);
			 System.out.println("r = " + r);
			 System.out.println("r2 = " + r2);
			 
		}
        
	}
	
	
	
	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LoadLogsMR(), args);
        System.exit(res);
    }
 
    
	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Correlate Logs");
        job.setJarByClass(LoadLogsMR.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("struct"),Bytes.toBytes("host"));
        scan.addColumn(Bytes.toBytes("struct"),Bytes.toBytes("bytes"));
        
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initTableMapperJob(args[1],scan,CorrelateLogsMapper.class, Text.class, LongPairWritable.class,job);

        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPairWritable.class);
        
        job.setNumReduceTasks(1);	
        ChainReducer.setReducer(job, CorrelateReducer.class, 
        		Text.class, LongPairWritable.class, LongWritable.class, LongWritable.class, new Configuration(false));
        
        return job.waitForCompletion(true) ? 0 : 1;
	}

}
