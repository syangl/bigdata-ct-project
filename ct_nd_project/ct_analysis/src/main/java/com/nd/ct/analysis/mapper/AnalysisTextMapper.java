package com.nd.ct.analysis.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @ClassName: AnalysisTextMapper
 * @PackageName:com.nd.ct.analysis.mapper
 * @Description: 分析数据mapper
 * @Author LiuSiyang
 * @Date 2022/7/14 14:40
 * @Version 1.0.0
 */
public class AnalysisTextMapper extends TableMapper<Text, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        /*
        5_19536627742_20220130194138_19508752064_3328_1
         */
        String rowKey= Bytes.toString(key.get());
        String[] values = rowKey.split("_");
        String call1=values[1];
        String call2=values[3];
        String calltime=values[2];
        String duration=values[4];
        String year=calltime.substring(0,4);
        String month=calltime.substring(0,6);
        String day=calltime.substring(0,8);
        //主叫年
        context.write(new Text(call1+"_"+year),new Text(duration));
        //主叫月
        context.write(new Text(call1+"_"+month),new Text(duration));
        //主叫日
        context.write(new Text(call1+"_"+day),new Text(duration));
        //被叫年
        context.write(new Text(call2+"_"+year),new Text(duration));
        //被叫月
        context.write(new Text(call2+"_"+month),new Text(duration));
        //被叫日
        context.write(new Text(call2+"_"+day),new Text(duration));

    }
}
