package com.bonc.mrImsiBack.job;

import com.bonc.decodeMrXdr.entity.LocatorCombinedKeyMr;
import com.bonc.decodeMrXdr.filter.MrFilterShanXi;
import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import com.bonc.mrImsiBack.utils.DateUtils;
import com.bonc.mrImsiBack.utils.PathDealUtils;
import com.bonc.mrImsiBack.utils.ReadParam;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class ImsiBackApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String configPath = null;       //配置文件路径
        String queuename = null;        //队列
        String str_date = null;         //天
        String str_date_hour = null;    //小时
        if (args.length == 0) {
            System.out.println("args must be at least 1.");
            System.exit(1);
        } else if (args.length == 1) {
            System.out.println("you input one param ,the first param is config param");
            configPath = args[0];
        } else if (args.length == 2) {
            System.out.println("you input two param ,the first param is config param ,the second param is date");
            configPath = args[0];
            str_date = args[1];
        } else if (args.length == 3) {
            System.out.println("you input three param ,the first param is config param ,the second param is date ,the third param is hour");
            configPath = args[0];
            str_date = args[1];
            str_date_hour = args[2];
        }

        Configuration conf = new Configuration();
        ReadParam.readXML(conf, configPath);

        if (StringUtils.isEmpty(conf.get("queuename")) ||
                StringUtils.isEmpty(conf.get("mr.inputpath")) ||
                StringUtils.isEmpty(conf.get("xdr.inputpath")) ||
                StringUtils.isEmpty(conf.get("resultpath"))) {
            System.out.printf("Please check %s, queuename,mrInputpath,xdrInputpath,resultpath must exist", configPath);
            System.exit(1);
        }

        // 设置任务队列
        queuename = conf.get("queuename");
        conf.set("mapreduce.job.queuename", queuename);

        // 设置时间
        if (str_date != null && !"null".equals(str_date)) {
            conf.set("str_date", str_date);
        }
        if (str_date_hour != null && !"null".equals(str_date_hour)) {
            conf.set("str_date_hour", str_date_hour);
        }

        // 参数可以读取子文件夹下的内容
        conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");

        String splitSize = conf.get("splitsize", "128");           //分片大小（M）
        String reducenum = conf.get("reducenum", "20");           //Reducer数
        String imsibackfill = conf.get("mr_imsibackfill", "1");  //是否需要回填imsi
        String xdrInput = conf.get("xdr.inputpath");        //信令目录
        String mrInput = conf.get("mr.inputpath");          //MR目录
        String result = conf.get("resultpath");             //输出目录
        Boolean isCombiner = conf.getBoolean("isCombine", false);         //是否要合并邻区
        int xdrinputlength = 1;                             //默认xdr数据为xdr.inputpath
        String[] xdrinputArray = null;

        Job job = Job.getInstance(conf);
        job.setJarByClass(ImsiBackApp.class);

        job.setMapOutputKeyClass(LocatorCombinedKeyMr.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        String mrinpath = mrInput;
        String outpath = result;
        if (conf.get("str_date") != null && conf.get("str_date_hour") != null) {
            mrinpath = mrInput + str_date.substring(2, 8); //山西
//            mrinpath = mrInput + File.separator + str_date + File.separator + str_date_hour; //贵州
            outpath = result + File.separator + str_date + File.separator + str_date_hour;
            //截取xdr时间
            PathDealUtils.setXdrTimeLimit(conf, str_date, str_date_hour); //山西，贵州
            if(conf.getInt("advanceMinute", 20) == 0 && conf.getInt("laterMinute", 0) == 0){
                //一个小时的xdr数据 2018091210
                xdrinputlength = 1;
            }else if(conf.getInt("advanceMinute", 20) > 0 && conf.getInt("laterMinute", 0) == 0){
                //2018091209 和 2018091210 的xdr数据
                xdrinputlength = 2;
            }else if(conf.getInt("advanceMinute", 20) > 0 && conf.getInt("laterMinute", 0) > 0){
                //2018091209、2018091210和2018091211 的xdr数据
                xdrinputlength = 3;
            }
            xdrinputArray = new String[xdrinputlength];
            PathDealUtils.getXdrInputPath(xdrInput, str_date, str_date_hour, xdrinputlength, xdrinputArray); //山西
//            PathDealUtils.getXdrInputPathGuizhou(xdrInput, str_date, str_date_hour, xdrinputlength, xdrinputArray); //贵州

            job.setJobName("ImsiBackFill_"+str_date+str_date_hour);
        } else if (conf.get("str_date") != null) {
//            mrinpath = mrInput + File.separator + str_date.substring(2, 8); //山西
            mrinpath = mrInput + File.separator + str_date; //贵州
            outpath = result + File.separator + str_date;
            xdrinputArray = new String[1];
            xdrinputArray[0] = xdrInput + str_date; //山西

            job.setJobName("ImsiBackFill_"+str_date);
        } else {
            xdrinputArray = new String[1];
            xdrinputArray[0] = xdrInput;

            job.setJobName("ImsiBackFill");
        }

        MultipleInputs.addInputPath(job, new Path(mrinpath), CombineTextInputFormat.class, MrMapper.class);
        if ("1".equals(imsibackfill)) {
            for (int i = 0; i < xdrinputlength; i++) {
                System.out.println("mme: " + xdrinputArray[i]);
                MultipleInputs.addInputPath(job, new Path(xdrinputArray[i]), CombineTextInputFormat.class, MmeXdrMapper.class);
            }
        }
        CombineTextInputFormat.setMaxInputSplitSize(job, Integer.parseInt(splitSize) * 1024 * 1024);
        if (!"".equals(conf.get("pathfilterclass",""))) {
            CombineTextInputFormat.setInputPathFilter(job, MrFilterShanXi.class);
        }

        job.setNumReduceTasks(Integer.parseInt(reducenum));
        job.setReducerClass(ImsiBackUpReducer.class);

        // 是否需要执行Combiner类
        if (isCombiner) {
            job.setCombinerClass(MrCombiner.class);
        }

        // 设置自定义分区策略
        String partitionerClass = conf.get("partitionerClass");
        if (partitionerClass==null || partitionerClass.equals("")) {
            System.out.println("partitionerClass must be not empty");
            System.exit(1);
        } else {
            Class onwClass = Class.forName(partitionerClass);
            job.setPartitionerClass(onwClass);
        }

        // 设置自定义二次排序策略
        String sortClass = conf.get("sortcomparer");
        if (sortClass==null || sortClass.equals("")) {
            System.out.println("sortClass must be not empty");
            System.exit(1);
        } else {
            Class onwClass = Class.forName(sortClass);
            job.setSortComparatorClass(onwClass);
        }

        // 设置自定义分组策略
        String groupClass = conf.get("groupcomparer");
        if (groupClass==null || groupClass.equals("")) {
            System.out.println("groupClass must be not empty");
            System.exit(1);
        } else {
            Class onwClass = Class.forName(groupClass);
            job.setGroupingComparatorClass(onwClass);
        }

        // 指定处理结果的输出数据存放路径
        Path outputPath = new Path(outpath);
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
            System.out.println("mr_single_records = "+counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_RECORDS).getValue());
            System.out.println("mr_error_records = "+counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_WRONG_RECORDS).getValue());
            System.out.println("mr_combine_records = "+counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_COMBINE_RECORDS).getValue());
            System.out.println("xdr_error_records = "+counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_XDR_WRONG_RECORDS).getValue());
            System.out.println("xdr_records = "+counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_XDR_RECORDS).getValue());
            System.out.println("all_data_records = "+counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_DATA_RECORDS).getValue());
            System.out.println("backfill_success = "+counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_imsiMatchCount).getValue());
            System.out.println("backfill_fail = "+counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_imsiNoMatchCount).getValue());
            System.out.println("ratio = " + (counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_imsiMatchCount).getValue()+0.0d) / (counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_imsiMatchCount).getValue() + counters.findCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_imsiNoMatchCount).getValue()));
            Date elapsed = new Date();
            String elapsedTime = DateUtils.getElapsedTime(elapsed.getTime() - startDate.getTime());
            System.out.println("spend time: "+ elapsedTime);
            System.out.println("success!");
        } else {

            System.out.println("fail!");
        }

    }
}
