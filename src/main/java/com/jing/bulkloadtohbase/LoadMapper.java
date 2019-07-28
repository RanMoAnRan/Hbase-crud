package com.jing.bulkloadtohbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author RanMoAnRan
 * @ClassName: LoadMapper
 * @projectName Hbase-crud
 * @description: TODO
 * @date 2019/7/23 21:51
 */
public class LoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        Put put = new Put(Bytes.toBytes(split[0]));
        put.addColumn("f1".getBytes(), "name".getBytes(), split[1].getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(Integer.parseInt(split[2])));
        context.write(new ImmutableBytesWritable(Bytes.toBytes(split[0])), put);
    }
}
