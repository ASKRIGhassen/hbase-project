package com.hbase.ops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class FilteredScan {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfig.getHbaseConfig();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        ResultScanner resultScanner = null;
        try {
            table = connection.getTable(TableName.valueOf("employee"));
            Filter filter = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("2")));
            Scan employeeScan = new Scan();
            employeeScan.setFilter(filter);
            resultScanner = table.getScanner(employeeScan);
            printResults(resultScanner);
            Filter filter2 = new RowFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("2")));
            employeeScan.setFilter(filter2);
            resultScanner = table.getScanner(employeeScan);
            printResults(resultScanner);

        } finally {
            connection.close();
            if (table != null) {
                table.close();
            }
            if (resultScanner != null) {
                resultScanner.close();
            }
        }

    }

    private static void printResults(ResultScanner resultScanner) {
        System.out.println("-----");
        for (Result res : resultScanner) {
            for (Cell cell : res.listCells()) {
                String row = new String(CellUtil.cloneRow(cell));
                String family = new String(CellUtil.cloneFamily(cell));
                String column = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));

                System.out.println(row + " " + family + " " + column + " " + value);
            }
        }
    }
}
