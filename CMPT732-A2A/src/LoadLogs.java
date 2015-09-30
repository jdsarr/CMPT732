
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

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
	
    public static Put get_put(String line) throws Exception {
    	
    	byte[] rowkey = DigestUtils.md5(line);
    	Put put = new Put(rowkey);
    	put.addColumn(Bytes.toBytes("raw"), Bytes.toBytes("line"), Bytes.toBytes(line));
    	
    	table.put(put);
    	
    	return put;
    }
    
    
    
    public static void read_files(String arg){
    	
    	try{
    		String line;
	    	String path = arg;	
	    	File file = new File(path);
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
    	

    	conf = HBaseConfiguration.create();
    	connection = ConnectionFactory.createConnection(conf);
    	table = connection.getTable(TableName.valueOf(args[0]));
    	
    	for(int i=1; i < args.length; i++){
    		read_files(args[i]);
    	}
    }

}
