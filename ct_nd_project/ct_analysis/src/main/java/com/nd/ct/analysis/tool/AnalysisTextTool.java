package com.nd.ct.analysis.tool;

import com.nd.ct.analysis.io.MySQLTextOutputFormat;
import com.nd.ct.analysis.mapper.AnalysisTextMapper;
import com.nd.ct.analysis.reducer.AnalysisTextReducer;
import com.nd.ct.common.constact.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * @ClassName: AnalysisTextTool
 * @PackageName:com.nd.ct.analysis.tool
 * @Description: 分析数据的工具类
 * @Author LiuSiyang
 * @Date 2022/7/14 14:50
 * @Version 1.0.0
 */
public class AnalysisTextTool implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        //获取job
        Job job= Job.getInstance();
        //设置jar位置
        job.setJarByClass(AnalysisTextTool.class);
        //扫描主叫列族
        Scan scan=new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));
        //设置mapper
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getValue(),
                scan,
                AnalysisTextMapper.class,
                Text.class,
                Text.class,
                job
        );
        //设置reducer
        job.setReducerClass(AnalysisTextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输出
        job.setOutputFormatClass(MySQLTextOutputFormat.class);
        //提交
        boolean flag = job.waitForCompletion(true);
        if(flag){
            return JobStatus.State.SUCCEEDED.getValue();
        }else{
            return JobStatus.State.FAILED.getValue();
        }
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
