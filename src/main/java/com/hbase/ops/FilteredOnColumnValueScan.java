package com.hbase.ops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilteredOnColumnValueScan {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfig.getHbaseConfig();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        ResultScanner resultScanner = null;
        try {
            SingleColumnValueFilter columnFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("professional"),
                    Bytes.toBytes("employed"),
                    CompareOperator.EQUAL,
                    new BinaryComparator(Bytes.toBytes("yes")));
            columnFilter.setFilterIfMissing(true);

            Scan scan = new Scan();
            scan.setFilter(columnFilter);
            table = connection.getTable(TableName.valueOf("employee"));
            resultScanner = table.getScanner(scan);
            printResults(resultScanner);

           SingleColumnValueFilter columnFilter2 = new SingleColumnValueFilter(
              Bytes.toBytes("personal"),
              Bytes.toBytes("name"),
              CompareOperator.EQUAL,
              new SubstringComparator("ASKRI"));

            scan.setFilter(columnFilter2);
            resultScanner = table.getScanner(scan);
            printResults(resultScanner);
            // Multiple Filter

            List<Filter> filterList = new ArrayList<>();
            filterList.add(columnFilter);
            filterList.add(columnFilter2);
            FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ONE, filterList);

            scan.setFilter(filters);

            resultScanner = table.getScanner(scan);
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
