package com.nd.ct.mp.dao;

import com.nd.ct.mp.bean.UserBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * @ClassName: UserToMySql
 * @PackageName:com.nd.ct.mp
 * @Description: TODO
 * @Author LiuSiyang
 * @Date 2022/7/13 14:58
 * @Version 1.0.0
 */
public class UserToMySql {
    public static class UserToMapper extends Mapper<LongWritable, Text, Text, UserBean> {
        private Text k = new Text();
        private UserBean v;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = new String(value.getBytes(), 0, value.getLength(), "UTF-8");
            String[] split = line.split("\t");
            k.set(split[0]);
            v = new UserBean(split[0], split[1]);
            context.write(k, v);
        }
    }

    public static class UserToReducer extends Reducer<Text, UserBean, UserBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<UserBean> values, Context context) throws IOException, InterruptedException {
            UserBean userBean = values.iterator().next();
            UserBean user = new UserBean(userBean.getTell(), userBean.getName());
            context.write(user, NullWritable.get());
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
        job.setMapperClass(UserToMapper.class);
        job.setReducerClass(UserToReducer.class);
        //设置jar位置
        job.setJarByClass(UserToMySql.class);
        //设置Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserBean.class);
        //设置最终输出类型
        job.setOutputKeyClass(UserBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入输出
        FileInputFormat.setInputPaths(job,new Path("D:\\Studyandwork\\资料文件\\学习\\中软实习学习\\南开大学实训\\2.资料\\电信项目\\data\\contact.log"));
        String[] fields={"tell","name"};
        DBOutputFormat.setOutput(job,"tb_user",fields);
        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
