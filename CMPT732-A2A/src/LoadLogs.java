
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import java.text.SimpleDateFormat;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class LoadLogs {
	
	static private Configuration conf;
	static private Connection connection;
	static private Table table;
	
	//Pattern used for regex
	static final Pattern pattern = Pattern.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$");
	//Parser for a date string
	static SimpleDateFormat dateparse = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
	
	
	
    //Function that adds a row of info to table based on the currently read line
	public static Put get_put(String line) throws Exception {
    	
		//Add raw line into the row
    	byte[] rowkey = DigestUtils.md5(line);
    	Put put = new Put(rowkey);
    	put.addColumn(Bytes.toBytes("raw"), Bytes.toBytes("line"), Bytes.toBytes(line));

    	
    	//Add struct host, date, path, and bytes into the row
        Matcher matcher = pattern.matcher(line);
        if (matcher.find( )) {
        	
        	
        	String host   = matcher.group(1);
        	long   date   = dateparse.parse(matcher.group(2)).getTime();
        	String path   = matcher.group(3);
        	long   bytes  = Long.parseLong(matcher.group(4));
         	
        	put.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("host"), Bytes.toBytes(host));
        	put.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("date"), Bytes.toBytes(date));
        	put.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("path"), Bytes.toBytes(path));
        	put.addColumn(Bytes.toBytes("struct"), Bytes.toBytes("bytes"), Bytes.toBytes(bytes));
 
        } else {
        	//Print out "NO MATCH" if a line is not structured as expected
        	System.out.println("NO MATCH");
        	System.out.println(line);
        	System.out.println(" ");
        }
    	
        //Add row to the table
    	table.put(put);
    	
    	return put;
    }
    
    
	
    // Function that reads individual lines in a file and then calls the get_put function
    public static void read_files(String arg){
    	
    	try{
    		
    		String line;
	    	String path = arg;	
	    	File file = new File(path);
	    	//BufferedReader capable of reading UTF8 encoding
	    	BufferedReader in = new BufferedReader( 
	    			new InputStreamReader(
	    					new FileInputStream(file), "UTF8"));
		    		
	    	while((line = in.readLine()) != null){
	    		get_put(line);
	    	}
	    	in.close();
	    	
    	}	
	    catch (UnsupportedEncodingException e) 
	    {
			System.out.println(e.getMessage());
	    } 
	    catch (IOException e) 
	    {
			System.out.println(e.getMessage());
	    }
	    catch (Exception e)
	    {
			System.out.println(e.getMessage());
	    }
    }
    
    
    
    
    public static void main(String[] args) throws Exception {
    	
    	//Set up table with the name declared by args[0]
    	conf = HBaseConfiguration.create();
    	connection = ConnectionFactory.createConnection(conf);
    	table = connection.getTable(TableName.valueOf(args[0]));
    	
    	//read all individual files
    	for(int i=1; i < args.length; i++){
    		read_files(args[i]);
    	}
    }

}
