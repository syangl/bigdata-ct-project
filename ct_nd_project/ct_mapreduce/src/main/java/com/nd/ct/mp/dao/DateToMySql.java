package com.nd.ct.mp.dao;

import com.nd.ct.mp.bean.DateBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @ClassName: DateToMySql
 * @PackageName:com.nd.ct.mp
 * @Description: TODO
 * @Author LiuSiyang
 * @Date 2022/7/13 14:24
 * @Version 1.0.0
 */
public class DateToMySql {
    public static class DateToMapper extends Mapper<LongWritable, Text, IntWritable, DateBean> {
        private IntWritable k = new IntWritable();
        private DateBean v;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
            String[] split = line.split(",", -1);
            k.set(Integer.parseInt(split[0]));
            v = new DateBean(Integer.parseInt(split[0]), split[1], split[2], split[3]);
            context.write(k, v);
        }
    }

    public static class DateToReducer extends Reducer<IntWritable, DateBean, DateBean, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<DateBean> values, Context context) throws IOException, InterruptedException {
            DateBean dataBean = values.iterator().next();
            DateBean data = new DateBean(dataBean.getId(), dataBean.getYear(), dataBean.getMonth(), dataBean.getDay());
            context.write(data, NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        DBConfiguration.configureDB(configuration,"com.mysql.jdbc.Driver",
                "jdbc:mysql://hadoop101:3306/ct_tb?characterEncoding=utf-8",
                "root","root");
        //获取job对象
        Job job = Job.getInstance(configuration);
        //关联mapper和reducer
        job.setMapperClass(DateToMapper.class);
        job.setReducerClass(DateToReducer.class);
        //设置jar位置
        job.setJarByClass(DateToMySql.class);
        //设置Mapper输出类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DateBean.class);
        //设置最终输出类型
        job.setOutputKeyClass(DateBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入输出
        FileInputFormat.setInputPaths(job,new Path("D:\\Studyandwork\\资料文件\\学习\\中软实习学习\\南开大学实训\\2.资料\\电信项目\\data\\date.csv"));
        String[] fields={"id","year","month","day"};
        DBOutputFormat.setOutput(job,"tb_date",fields);
        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
