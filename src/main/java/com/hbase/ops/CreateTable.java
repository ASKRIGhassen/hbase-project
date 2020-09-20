package com.hbase.ops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateTable {
    public static final byte[] PERSONAL_CF = Bytes.toBytes("personal");
    public static final byte[] PROFESSIONAL_CF= Bytes.toBytes("professional");

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfig.getHbaseConfig();
        Connection connection = ConnectionFactory.createConnection(conf);
        try{
            Admin admin = connection.getAdmin();
            TableName employeeTable = TableName.valueOf("employee");
            List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
            cfs.add(ColumnFamilyDescriptorBuilder.of(PERSONAL_CF));
            cfs.add(ColumnFamilyDescriptorBuilder.of(PROFESSIONAL_CF));
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(employeeTable).setColumnFamilies(cfs).build();
            admin.createTable(tableDescriptor);
            System.out.println("Table created.");

        }finally {
            connection.close();
        }
    }
}
