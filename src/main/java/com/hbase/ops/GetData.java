package com.hbase.ops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetData {
    public static final byte[] PERSONAL_CF = Bytes.toBytes("personal");
    public static final byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");
    public static final byte[] COLUMN_NAME = Bytes.toBytes("name");
    public static final byte[] COLUMN_ADDRESS = Bytes.toBytes("address");
    public static final byte[] COLUMN_FIELD = Bytes.toBytes("field");

    public static void main(String[] args) throws IOException {

        // configuration
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "localhost:60000");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "localhost");
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

        // connection
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf("employee"));
            Get get = new Get(Bytes.toBytes("1"));
            get.addColumn(PERSONAL_CF,COLUMN_NAME);
            get.addColumn(PERSONAL_CF,COLUMN_ADDRESS);
            get.addColumn(PROFESSIONAL_CF,COLUMN_FIELD);
           Result result = table.get(get);
           byte[] nameValue = result.getValue(PERSONAL_CF,COLUMN_NAME);
            System.out.println("Name: "+ Bytes.toString(nameValue));
            byte[] address = result.getValue(PERSONAL_CF,COLUMN_ADDRESS);
            System.out.println("Address: "+ Bytes.toString(address));

            List<Get> gets = new ArrayList<>();
            Get get2 = new Get(Bytes.toBytes("2"));
            get2.addColumn(PERSONAL_CF,COLUMN_NAME);
            get2.addColumn(PERSONAL_CF,COLUMN_ADDRESS);
            get2.addColumn(PROFESSIONAL_CF,COLUMN_FIELD);

            gets.add(get);
            gets.add(get2);

            Result[] results = table.get(gets);
            for(Result rslt: results){
                byte[] name = rslt.getValue(PERSONAL_CF,COLUMN_NAME);
                System.out.println("Name: "+ Bytes.toString(name));
            }

        } finally {
            connection.close();
            if (table != null) {
                table.close();
            }

        }
    }
}
