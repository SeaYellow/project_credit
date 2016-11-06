package com.merit.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * Created by Administrator on 2016/9/8.
 */
public class CreditTelMR extends Configured implements Tool {

    public static void main(String[] args) {
        int ret = 0;
        try {
            ret = ToolRunner.run(new Configuration(), new CreditTelMR(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println("================================Start CreditTelMR================================");
        if (args.length < 3) {
            System.err.println("Usage: CreditTelMR <in> [<in>...] <out> <num reduce>");
            System.exit(-1);
        }

        String output = args[(args.length - 2)];
        int numReduce = Integer.parseInt(args[args.length - 1]);

        System.out.println("OutPutPath: " + output + " Num reduces: " + numReduce);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "CreditTelMR");

        final FileSystem fs = FileSystem.get(conf);
        Path resOutPath = new Path(output);
        if (fs.exists(resOutPath)) {
            throw new IOException("Output directory " + output + "already exists");
        }


        job.setNumReduceTasks(numReduce);
        job.setJarByClass(CreditTelMR.class);
        job.setMapperClass(CreditTelMapper.class);
        job.setReducerClass(CreditTelReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        for (int i = 0; i < args.length - 2; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        Path sortOutPath = new Path("/tmp/credit/" + "Credit_Tel_Sort_" + System.currentTimeMillis());
        System.out.println("Sort Tel MR out path: " + sortOutPath.toString());
        FileOutputFormat.setOutputPath(job, sortOutPath);

        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        int ret = job.waitForCompletion(true) ? 0 : 1;
        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        System.out.println("The job took " + (endTime.getTime() - startTime.getTime()) / 1000L + " seconds.");

        if (ret != 0) {
            throw new Exception("CreditTelMR end failed.");
        }

        Job sortJob = Job.getInstance(conf, "CreditTelSortMR");
        sortJob.setNumReduceTasks(1);
        sortJob.setJarByClass(CreditTelMR.class);
        sortJob.setMapperClass(CreditTelSortMapper.class);
        sortJob.setReducerClass(CreditTelSortReducer.class);

        sortJob.setMapOutputKeyClass(CreditCountWritable.class);
        sortJob.setMapOutputValueClass(Text.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(IntWritable.class);

        sortJob.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(sortJob, sortOutPath);
        FileOutputFormat.setOutputPath(sortJob, resOutPath);

        Date startSortTime = new Date();
        System.out.println("SortJob started: " + startSortTime);
        ret = sortJob.waitForCompletion(true) ? 0 : 1;
        Date endSortTime = new Date();
        System.out.println("SortJob ended: " + endSortTime);
        System.out.println("The sort job took " + (endSortTime.getTime() - startSortTime.getTime()) / 1000L + " seconds.");
        long totalTime = (endSortTime.getTime() - startTime.getTime()) / 1000L;
        System.out.println("================================End CreditTelMR=>[Total Use Time: " + totalTime + " s]================================");
        return ret;
    }


    public static class CreditTelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text tVal = new Text();

        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String itr = value.toString();
            if (StringUtils.isNotEmpty(itr)) {
                // 分隔符
                String[] strs = itr.split("\t");
                if (strs.length > 1) {
                    tVal.set(strs[1]);
                    context.write(tVal, one);
                } else {
                    // 异常数据处理
                    tVal.set("Error_Record");
                    context.write(tVal, one);
                }
            }
        }
    }


    public static class CreditTelReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable res = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            res.set(sum);
            context.write(key, res);
        }
    }

    public static class CreditTelSortMapper extends Mapper<LongWritable, Text, CreditCountWritable, Text> {
        private static final CreditCountWritable intVal = new CreditCountWritable();
        private Text keyVal = new Text();

        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, CreditCountWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            keyVal.set(strs[0]);
            intVal.setCount(Integer.parseInt(strs[1]));
            context.write(intVal, keyVal);
        }
    }

    public static class CreditTelSortReducer extends Reducer<CreditCountWritable, Text, Text, IntWritable> {
        private static final IntWritable count = new IntWritable();

        public void reduce(CreditCountWritable num, Iterable<Text> keys, Reducer<CreditCountWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            for (Text key : keys) {
                count.set(num.getCount());
                context.write(key, count);
            }
        }
    }


}
