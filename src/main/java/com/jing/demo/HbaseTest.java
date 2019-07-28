package com.jing.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author RanMoAnRan
 * @ClassName: HbaseTest
 * @projectName Hbase-crud
 * @description: TODO
 * @date 2019/7/23 17:26
 */
@SuppressWarnings("all")
public class HbaseTest {

    /**
     * 创建表
     *
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
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
        admin.close();
        connection.close();

    }

    /**
     * 批量插入数据
     *
     * @throws IOException
     */
    @Test
    public void addDatas() throws IOException {
        Table table = HbaseUtil.getTable();
        System.out.println(table.getName());

        //创建put对象，并指定rowkey
        Put put = new Put("0002".getBytes());
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(30));
        put.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("talk is cheap , show me the code"));


        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("what are you 弄啥嘞！"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("跟诸葛亮死掐"));


        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("貂蝉去哪了"));

        List<Put> listPut = new ArrayList<>();
        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);

        table.put(listPut);

        table.close();
    }

    /**
     * 查询数据，按照主键id进行查询
     *
     * @throws IOException
     */
    @Test
    public void searchData() throws IOException {
        Table table = HbaseUtil.getTable();
        Get get = new Get("0003".getBytes());
        Result result = table.get(get);
        System.out.println(result);
        //获取列族f1下age列的值
        byte[] value = result.getValue("f1".getBytes(), "age".getBytes());
        int age = Bytes.toInt(value);
        System.out.println(age);
        table.close();
    }


    /**
     * 查询指定rokwey下的所有列的值
     */
    @Test
    public void queryAllByRowkey() throws IOException {
        Table table = HbaseUtil.getTable();
        Get get = new Get("0002".getBytes());
        get.addFamily("f1".getBytes());
        Result result = table.get(get);
        // System.out.println(result);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
            String column = Bytes.toString(cloneQualifier);
            //System.out.println(column);
            if (column.equals("age") || column.equals("id")) {
                System.out.println(Bytes.toInt(CellUtil.cloneValue(cell)));
            } else {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }


    /**
     * rowFilter行过滤器
     */
    @Test
    public void rowFilter() throws IOException {
        Table table = HbaseUtil.getTable();
        Scan scan = new Scan();
        //获取rowkey小于0004的数据
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator("0004".getBytes()));
        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);

        HbaseUtil.foreach(scanner);

        table.close();
    }


    /**
     * qualifierFilter,列过滤器
     */
    @Test
    public void qualifierFilter() throws IOException {
        Table table = HbaseUtil.getTable();

        Scan scan = new Scan();
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("name"));
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = table.getScanner(scan);
        HbaseUtil.foreach(scanner);

        table.close();
    }

    /**
     * 列族过滤器FamilyFilter
     */
    @Test
    public void familyFilter() throws IOException {
        Table table = HbaseUtil.getTable();
        Scan scan = new Scan();
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("f1"));
        scan.setFilter(familyFilter);
        ResultScanner scanner = table.getScanner(scan);
        HbaseUtil.foreach(scanner);
        table.close();
    }

    /**
     * 值过滤器
     * ValueFilter
     */
    @Test
    public void valueFilter() throws IOException {
        Table table = HbaseUtil.getTable();
        Scan scan = new Scan();
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("15888888888"));
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        HbaseUtil.foreach(scanner);
    }

    /**
     * 单列值过滤器
     * 会返回满足指定条件下对应的rowkey下，所有列的数据
     */
    @Test
    public void singleColumnValueFilter() throws IOException {
        Table table = HbaseUtil.getTable();
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "曹操".getBytes());
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        HbaseUtil.foreach(scanner);
        table.close();
    }

    /**
     * 单列值排除过滤器：
     * 与SingleColumnValueFilter相反，会排除掉指定的列，其他的列全部返回
     */
    @Test
    public void singleColumnValueExcluedeFilter() throws IOException {
        Table table = HbaseUtil.getTable();
        Scan scan = new Scan();
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "曹操".getBytes());
        scan.setFilter(singleColumnValueExcludeFilter);
        ResultScanner scanner = table.getScanner(scan);
        HbaseUtil.foreach(scanner);
        table.close();
    }

    /**
     * 分页查询
     */

    @Test
    public void pageQuery() throws IOException {
        Table table = HbaseUtil.getTable();

        int pageNum = 3;
        int pageSize = 2;
        Scan scan = new Scan();
        if (pageNum == 1) {
            PageFilter filter = new PageFilter(pageSize);
            scan.setStartRow(Bytes.toBytes(""));
            scan.setFilter(filter);
            scan.setMaxResultSize(pageSize);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                //获取rowkey
                System.out.println(Bytes.toString(result.getRow()));
                //指定列族以及列打印列当中的数据出来
//            System.out.println(Bytes.toInt(result.getValue("f1".getBytes(), "id".getBytes())));
                System.out.println(Bytes.toString(result.getValue("f1".getBytes(), "name".getBytes())));
                //System.out.println(Bytes.toString(result.getValue("f2".getBytes(), "phone".getBytes())));
            }
        } else {
            String rowKey = "";
            for (int i = pageNum - 1; i <= pageNum; i++) {
                if (i == pageNum) {
                    PageFilter filter = new PageFilter(pageSize);
                    scan.setStartRow(Bytes.toBytes(rowKey));
                    scan.setFilter(filter);
                    ResultScanner scanner = table.getScanner(scan);
                    for (Result result : scanner) {
                        byte[] row = result.getRow();
                        rowKey = new String(row);
                        System.out.println(rowKey);
                        System.out.println(Bytes.toString(result.getValue("f1".getBytes(), "name".getBytes())));
                    }
                } else {
                    PageFilter filter = new PageFilter((pageNum - 1) * pageSize + 1);
                    scan.setStartRow(Bytes.toBytes(rowKey));
                    scan.setFilter(filter);
                    ResultScanner scanner = table.getScanner(scan);
                    for (Result result : scanner) {
                        byte[] row = result.getRow();
                        rowKey = new String(row);
                    }
                }
            }
        }
        table.close();
    }


    /**
     * 多过滤器综合查询
     * 需求：使用SingleColumnValueFilter查询f1列族，name为刘备的数据，并且同时满足rowkey的前缀以00开头的数据（PrefixFilter）
     * 过滤器之间是并的关系（&&）
     */
    @Test
    public void queryFilterList() throws IOException {
        Table table = HbaseUtil.getTable();
        Scan scan = new Scan();
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "曹操".getBytes());
        scan.setFilter(singleColumnValueFilter);
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);

        HbaseUtil.foreach(scanner);
        table.close();
    }


    /**
     * 根据rowkey删除数据
     */
    @Test
    public  void  deleteByRowKey() throws IOException {
        Table table = HbaseUtil.getTable();
        Delete delete = new Delete("0001".getBytes());
        table.delete(delete);
        table.close();
    }


    /**
     * 删除表
     * @throws IOException
     */
    @Test
    public void delTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取客户端操作对象
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("user");
        //先禁用表
        admin.disableTable(tableName);
        //删除表
        admin.deleteTable(tableName);
    }
}


