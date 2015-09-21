import java.io.IOException;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
 
import com.google.common.base.Charsets;
 
public class MultiLineJSONInputFormat extends TextInputFormat {
 
    public class MultiLineRecordReader extends RecordReader<LongWritable, Text> {
        LineRecordReader linereader;
        LongWritable current_key = new LongWritable(0);
        Text current_value = new Text("");
 
        public MultiLineRecordReader(byte[] recordDelimiterBytes) {
            linereader = new LineRecordReader(recordDelimiterBytes);
        }
 
        @Override
        public void initialize(InputSplit genericSplit,
                TaskAttemptContext context) throws IOException {
            linereader.initialize(genericSplit, context);

        }
 
        @Override
        public boolean nextKeyValue() throws IOException {
            /* do something better here to use linereader and
            set current_key and current_value */
        	
        	boolean ret = linereader.nextKeyValue();
        	current_value.clear();
        	String trimmed;                         //line without leading or trailing whitespace
        	
        	
        	if(ret == true && (linereader.getCurrentValue().toString().startsWith("{"))){
        		
            	while(ret == true){
            		trimmed = linereader.getCurrentValue().toString().trim();
            		current_value.append(trimmed.getBytes(),0,trimmed.length());
            		if(trimmed.endsWith("}"))
            			break;
            		ret = linereader.nextKeyValue();
            	}  			
            	
        	}else{
        		ret = false;
        	}
            return ret;
        }
 
        
        @Override
        public float getProgress() throws IOException {
            return linereader.getProgress();
            
        }
 
        @Override
        public LongWritable getCurrentKey() {
            return current_key;
            // return linereader.getCurrentKey();
        }
 
        @Override
        public Text getCurrentValue() {
            return current_value;
            // return linereader.getCurrentValue();
        }
 
        @Override
        public synchronized void close() throws IOException {
            linereader.close();
        }
    }
 
    // shouldn't have to change below here
 
    @Override
    public RecordReader<LongWritable, Text> 
    createRecordReader(InputSplit split,
            TaskAttemptContext context) {
        // same as TextInputFormat constructor, except return MultiLineRecordReader
        String delimiter = context.getConfiguration().get(
                "textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter)
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        return new MultiLineRecordReader(recordDelimiterBytes);
    }
 
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        // let's not worry about where to split within a file
        return false;
    }
}
