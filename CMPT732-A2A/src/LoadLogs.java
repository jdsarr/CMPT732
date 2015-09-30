

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
    
    public static void main(String[] args) throws Exception {
    	
    	conf = HBaseConfiguration.create();
    	connection = ConnectionFactory.createConnection(conf);
    	table = connection.getTable(TableName.valueOf(args[0]));
    	
    	
    }

}
