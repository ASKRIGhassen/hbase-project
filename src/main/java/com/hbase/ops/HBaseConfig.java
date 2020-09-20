package com.hbase.ops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseConfig {

    private HBaseConfig() {

    }

    public static Configuration getHbaseConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "localhost:60000");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        return conf;
    }
}
