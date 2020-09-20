package com.hbase.ops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DeleteData {
    public static final byte[] PERSONAL_CF = Bytes.toBytes("personal");
    public static final byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");
    public static final byte[] COLUMN_NAME = Bytes.toBytes("name");
    public static final byte[] COLUMN_ADDRESS = Bytes.toBytes("address");


    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfig.getHbaseConfig();
        Connection connection = ConnectionFactory.createConnection(conf);

     Table table = null;
     try{
         table = connection.getTable(TableName.valueOf("census"));
         Delete delete = new Delete(Bytes.toBytes("1"));
         delete.addColumn(PERSONAL_CF,COLUMN_ADDRESS);
         table.delete(delete);

     }finally {
         connection.close();
         if(table != null){
             table.close();
         }
     }
    }
}
