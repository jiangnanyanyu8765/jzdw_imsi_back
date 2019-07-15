package com.bonc.mrImsiBack.test;

import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import com.bonc.mrImsiBack.utils.DateUtils;
import com.bonc.mrImsiBack.utils.ReadParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Date;

public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String configPath = args[0];
        ReadParam.readXML(conf, configPath);
        String queuename = "";
        conf.set("mapreduce.job.queuename", queuename);

        // 参数可以读取子文件夹下的内容
        conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");

        String splitSize = conf.get("splitsize", "128");           //分片大小（M）
        String reducenum = conf.get("reducenum", "5");           //Reducer数
//        String input = "C:\\Users\\wtrover\\Desktop\\山西\\wc_test\\in1";          //MR目录
        String input = args[1];          //MR目录
//        String input2 = "C:\\Users\\wtrover\\Desktop\\山西\\wc_test\\in2";          //MR目录
//        String input2 = "C:\\Users\\wtrover\\Desktop\\山西\\wc_test\\in2";          //MR目录
//        String result = "C:\\Users\\wtrover\\Desktop\\山西\\wc_test\\out";             //输出目录
        String result = args[2];             //输出目录
        Boolean isCombiner = conf.getBoolean("isCombine", false);         //是否要合并邻区
//        String needMapper2 = conf.get("needMapper2", "0");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setJobName("WordCount");

        MultipleInputs.addInputPath(job, new Path(input), CombineTextInputFormat.class, WcMapper.class);
//        if ("1".equals(needMapper2)) {
//            MultipleInputs.addInputPath(job, new Path(input2), CombineTextInputFormat.class, WcMapper.class);
//        }
        CombineTextInputFormat.setMaxInputSplitSize(job, Integer.parseInt(splitSize) * 1024 * 1024);

        job.setNumReduceTasks(Integer.parseInt(reducenum));
        job.setReducerClass(WcReducer.class);

        // 是否需要执行Combiner类
        if (isCombiner) {
            job.setCombinerClass(WcReducer.class);
        }

//        // 设置自定义分区策略
//        String partitionerClass = conf.get("partitionerClass");
//        if (partitionerClass==null || partitionerClass.equals("")) {
//            System.out.println("partitionerClass must be not empty");
//            System.exit(1);
//        } else {
//            Class onwClass = Class.forName(partitionerClass);
//            job.setPartitionerClass(onwClass);
//        }
//
//        // 设置自定义二次排序策略
//        String sortClass = conf.get("sortcomparer");
//        if (sortClass==null || sortClass.equals("")) {
//            System.out.println("sortClass must be not empty");
//            System.exit(1);
//        } else {
//            Class onwClass = Class.forName(sortClass);
//            job.setSortComparatorClass(onwClass);
//        }
//
//        // 设置自定义分组策略
//        String groupClass = conf.get("groupcomparer");
//        if (groupClass==null || groupClass.equals("")) {
//            System.out.println("groupClass must be not empty");
//            System.exit(1);
//        } else {
//            Class onwClass = Class.forName(groupClass);
//            job.setGroupingComparatorClass(onwClass);
//        }

        // 指定处理结果的输出数据存放路径
        Path outputPath = new Path(result);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println(outputPath + "输出路径存在，已删除！");
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        FileOutputFormat.setCompressOutput(job, false);

        Date startDate = new Date();
        boolean flag = job.waitForCompletion(true);
        if (flag) {

            Counters counters = job.getCounters();
            System.out.println("map = "+counters.findCounter(LOG_PROCESSOR_COUNTER.WC_MAP_WORD).getValue());
            System.out.println("reduce = "+counters.findCounter(LOG_PROCESSOR_COUNTER.WC_REDUCE_WORD).getValue());
            Date elapsed = new Date();
            String elapsedTime = DateUtils.getElapsedTime(elapsed.getTime() - startDate.getTime());
            System.out.println("spend time: "+ elapsedTime);
            System.out.println("success!");
        } else {
            System.out.println("fail!");
        }
    }

}
