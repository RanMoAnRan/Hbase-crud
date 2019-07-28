package com.jing.hbaseandmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * @author RanMoAnRan
 * @ClassName: ReadHdFileToHbase
 * @projectName Hbase-crud
 * @description: 通过mapreduce，将hdfs上的文件写入hbase
 * @date 2019/7/23 21:07
 */
public class ReadHdFileToHbase {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","node01:2181");
        Job job = new Job(config,"ReadHdFileToHbase");
        job.setJarByClass(MrToHbaseDemo.class);    // class that contains mapper

        //设置inputformat格式
        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/tmp/user.txt"));
        //将K1 V1 转换程k2 v2
        job.setMapperClass(ReadHdFileToHbaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //reducer
        TableMapReduceUtil.initTableReducerJob(
                "myuser2",      // output table
                ReadHdFileToHbaseReducer.class,             // reducer class
                job);
        job.setNumReduceTasks(1);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

    }


    private static class ReadHdFileToHbaseMapper extends Mapper<LongWritable,Text,Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,NullWritable.get());
        }
    }

    private static class ReadHdFileToHbaseReducer extends TableReducer<Text,NullWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String[] arr = key.toString().split(",");
            String rowkey = arr[0];
            String name = arr[1];
            String age = arr[2];

            Put put = new Put(rowkey.getBytes());
            put.addColumn("f1".getBytes(),"name".getBytes(),name.getBytes());
            put .addColumn("f1".getBytes(),"age".getBytes(),age.getBytes());
            context.write(new ImmutableBytesWritable(rowkey.getBytes()),put);
        }
    }
}
