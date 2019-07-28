package com.jing.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author RanMoAnRan
 * @ClassName: HbaseUtil
 * @projectName Hbase-crud
 * @description: TODO
 * @date 2019/7/23 18:49
 */
@SuppressWarnings("all")
public class HbaseUtil {
    public static Table getTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取客户端操作对象
        Admin admin = connection.getAdmin();
        //构建表描述器
        TableName tableName = TableName.valueOf("myuser");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        //构建列族描述器
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("f1");
        HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("f2");
        hTableDescriptor.addFamily(hColumnDescriptor);
        hTableDescriptor.addFamily(hColumnDescriptor2);


        if (!admin.tableExists(tableName)) {
            //如果不存在则创建表
            admin.createTable(hTableDescriptor);
            admin.close();
        }

        return connection.getTable(tableName);
    }


    //根据结果集遍历获取数据

    public static void foreach(ResultScanner scanner){
        for (Result result : scanner) {
            System.out.println("rowkey:" + Bytes.toString(result.getRow()));
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] cloneQualifier = CellUtil.cloneQualifier(cell);

                String column = Bytes.toString(cloneQualifier);
                if (column.equals("age") || column.equals("id")) {
                    System.out.println(Bytes.toInt(CellUtil.cloneValue(cell)));
                } else {
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        }
    }
}
