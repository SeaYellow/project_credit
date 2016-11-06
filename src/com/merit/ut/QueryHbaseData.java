package com.merit.ut;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Created by Administrator on 2016/9/9.
 */
public class QueryHbaseData {

    public static void main(String[] args) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "191.168.4.78:2181");
        cfg.set("hbase.master", "191.168.4.78");
        cfg.set("hbase.client.scanner.timeout.period", "60000");

        Connection connection = ConnectionFactory.createConnection(cfg);

        try (Table targetTable = connection.getTable(TableName.valueOf("credit"))) {
            Scan scan = new Scan();
            scan.setMaxVersions(100);
            ResultScanner scanner = targetTable.getScanner(scan);

            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    String rowKey = Bytes.toString(result.getRow());
                    String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String qualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                    System.out.println("rowKey: " + rowKey + "family: " + family + " qualifier: " + qualifier + " timestamp: " + sdf.format(cell.getTimestamp()));
                    System.out.println(new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
