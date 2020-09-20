package com.hbase.ops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutData {

    public static final byte[] PERSONAL_CF = Bytes.toBytes("personal");
    public static final byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");
    public static final byte[] NAME_CL = Bytes.toBytes("name");
    public static final byte[] ADDRESS_CL = Bytes.toBytes("address");
    public static final byte[] FIELD_CL = Bytes.toBytes("field");
    public static final byte[] EMPLOYED_CL = Bytes.toBytes("employed");

    public static void main(String[] args) throws IOException {

        //1- create a Configuration
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "localhost:60000");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");


        //2- establish a connection
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = null;
        try {
            System.out.println("inserting...");
            Admin admin = connection.getAdmin();
            TableName census = TableName.valueOf("employee");
            table = connection.getTable(census);
            Put ghassen = new Put(Bytes.toBytes("1"));
            ghassen.addColumn(PERSONAL_CF, NAME_CL, Bytes.toBytes("Ghassen ASKRI"));
            ghassen.addColumn(PERSONAL_CF, ADDRESS_CL, Bytes.toBytes("9 rue paul bert 92700"));
            ghassen.addColumn(PROFESSIONAL_CF, EMPLOYED_CL, Bytes.toBytes("yes"));
            ghassen.addColumn(PROFESSIONAL_CF, FIELD_CL, Bytes.toBytes("engineering"));
            table.put(ghassen);

            Put layene = new Put(Bytes.toBytes("2"));
            layene.addColumn(PERSONAL_CF, NAME_CL, Bytes.toBytes("LAYENE ASKRI"));
            layene.addColumn(PERSONAL_CF, ADDRESS_CL, Bytes.toBytes("9 rue paul bert 92700"));
            layene.addColumn(PROFESSIONAL_CF, EMPLOYED_CL, Bytes.toBytes("no"));

            Put rh = new Put(Bytes.toBytes("3"));
            rh.addColumn(PERSONAL_CF, NAME_CL, Bytes.toBytes("Rh KH"));
            rh.addColumn(PERSONAL_CF, ADDRESS_CL, Bytes.toBytes("9 rue paul bert 92700"));
            rh.addColumn(PROFESSIONAL_CF, EMPLOYED_CL, Bytes.toBytes("yes"));
            rh.addColumn(PROFESSIONAL_CF, FIELD_CL, Bytes.toBytes("biology"));

            Put bs = new Put(Bytes.toBytes("4"));
            bs.addColumn(PERSONAL_CF, NAME_CL, Bytes.toBytes("Bs ASKRI"));
            bs.addColumn(PERSONAL_CF, ADDRESS_CL, Bytes.toBytes("Tunisia"));
            bs.addColumn(PROFESSIONAL_CF, EMPLOYED_CL, Bytes.toBytes("yes"));
            bs.addColumn(PROFESSIONAL_CF, FIELD_CL, Bytes.toBytes("Industry"));

            Put ht = new Put(Bytes.toBytes("5"));
            ht.addColumn(PERSONAL_CF, NAME_CL, Bytes.toBytes("ht ASKRI"));
            ht.addColumn(PERSONAL_CF, ADDRESS_CL, Bytes.toBytes("Tunisia"));
            ht.addColumn(PROFESSIONAL_CF, EMPLOYED_CL, Bytes.toBytes("yes"));
            ht.addColumn(PROFESSIONAL_CF, FIELD_CL, Bytes.toBytes("Army"));

            List<Put> puts = new ArrayList<>();
            puts.add(layene);
            puts.add(rh);
            puts.add(bs);
            puts.add(ht);
            table.put(puts);

            System.out.println("DONE.");


        } finally {
            connection.close();
            if (table != null) {
                table.close();
            }
        }
    }
}
