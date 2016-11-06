package com.merit.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by Administrator on 2016/9/9.
 */
public class BulkLoadMR extends Configured implements Tool {

    public static void main(String[] args) {
        int ret = 0;
        try {
            ret = ToolRunner.run(new Configuration(), new BulkLoadMR(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(ret);
    }


    @Override
    public int run(String[] args) throws Exception {
        System.out.println("================================Start BulkLoadMR================================");
        if (args.length != 4) {
            System.err.println("Usage: BulkLoadMR <in> <out> <table name> <zookeeper quorum>");
            return -1;
        }
        String inputPath = args[0];
        String outputPath = args[1];
        String tableName = args[2];
        String zQuorum = args[3];

        System.out.println("inputPath: " + inputPath + " outputPath: " + outputPath + " Table name: " + tableName);
        Configuration conf = getConf();
        conf.set("hbase.zookeeper.quorum", zQuorum);
        conf.set("hbase.client.scanner.timeout.period", "60000");
        Job job = Job.getInstance(conf, "Bulk Loading HBase Table: " + tableName);
        job.setJarByClass(BulkLoadMR.class);
        job.setMapperClass(BulkLoadMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        FileInputFormat.addInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tName = TableName.valueOf(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tName), connection.getRegionLocator(tName));

        Date startTime = new Date();
        System.out.println("Job started: " + startTime);

        boolean res = job.waitForCompletion(true);

        System.out.println("Result is: " + res);

        if (job.isSuccessful()) {
            // 修改输出文件权限
            final FileSystem fileSystem = FileSystem.get(conf);
            fileSystem.setOwner(new Path(outputPath), "hbase", "hdfs");
            final FileStatus[] fileStatuses = fileSystem.listStatus(new Path(outputPath));
            for (FileStatus fstatus : fileStatuses) {
                fileSystem.setOwner(fstatus.getPath(), "hbase", "hdfs");
            }
            new HFileLoader().doBulkLoad(outputPath, tName, conf);
            res = true;
        } else {
            throw new Exception("Bulk Loading HBase failed.");
        }
        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        long totalTime = (endTime.getTime() - startTime.getTime()) / 1000L;
        System.out.println("================================End BulkLoadMR=>[Total Use Time: " + totalTime + " s]================================");
        return res ? 0 : -1;
    }


    public static class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ImmutableBytesWritable ibw = new ImmutableBytesWritable();
        private Text keyVal = new Text();

        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>.Context context) throws IOException, InterruptedException {
            String itr = value.toString();
            if (StringUtils.isNotEmpty(itr)) {
                // 分隔符
                String[] strs = itr.split("\t");
                if (strs.length > 2) {
                    String tel = strs[1];
                    try {
                        Date timeStamp = sdf.parse(strs[2]);
                        keyVal.set(tel);
                        KeyValue kv = new KeyValue(tel.getBytes(), "f".getBytes(), "q".getBytes(), timeStamp.getTime(), itr.getBytes());
                        ibw.set(keyVal.getBytes());
                        context.write(ibw, kv);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public class HFileLoader {
        public void doBulkLoad(String pathToHFile, TableName tName, Configuration configuration) {
            try {
                HBaseConfiguration.addHbaseResources(configuration);
                LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);
                HTable hTable = new HTable(configuration, tName);
                loadFfiles.doBulkLoad(new Path(pathToHFile), hTable);
                System.out.println("Bulk Load Completed..");
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
    }
}
