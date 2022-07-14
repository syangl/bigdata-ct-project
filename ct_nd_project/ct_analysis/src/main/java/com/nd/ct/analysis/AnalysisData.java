package com.nd.ct.analysis;

import com.nd.ct.analysis.tool.AnalysisTextTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName: AnalysisData
 * @PackageName:com.nd.ct.analysis
 * @Description: 分析数据
 * @Author LiuSiyang
 * @Date 2022/7/14 14:57
 * @Version 1.0.0
 */
public class AnalysisData {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new AnalysisTextTool(), args);
    }
}
