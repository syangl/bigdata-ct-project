package com.nd.ct.analysis.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: AnalysisTextReducer
 * @PackageName:com.nd.ct.analysis.reducer
 * @Description: 分析数据Reducer
 * @Author LiuSiyang
 * @Date 2022/7/14 14:45
 * @Version 1.0.0
 */
public class AnalysisTextReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //统计通话次数
        int sumCall=0;
        //统计通话总时长
        int sumDuration=0;
        for (Text value : values) {
            int duration=Integer.parseInt(value.toString());
            sumDuration+=duration;
            sumCall++;
        }
        System.out.println(sumCall+"_"+sumDuration);
        context.write(key,new Text(sumCall+"_"+sumDuration));
    }
}
