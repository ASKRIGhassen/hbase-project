package com.hbase.ops;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class ScanData {

    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfig.getHbaseConfig();
        Connection connection = ConnectionFactory.createConnection(configuration);

        Table table = null;
        ResultScanner resultScanner = null;
        try {
            table = connection.getTable(TableName.valueOf("employee"));
            Scan scan = new Scan();
            resultScanner = table.getScanner(scan);
            for (Result res : resultScanner) {
                for (Cell cell : res.listCells()) {
                    String row = new String(CellUtil.cloneRow(cell));
                    String family = new String(CellUtil.cloneFamily(cell));
                    String column = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));

                    System.out.println(row + " " + family + " " + column + " " + value);
                }
            }

        } finally {
            connection.close();
            if (table != null) {
                table.close();
            }
            if(resultScanner!=null){
                resultScanner.close();
            }
        }

    }
}
