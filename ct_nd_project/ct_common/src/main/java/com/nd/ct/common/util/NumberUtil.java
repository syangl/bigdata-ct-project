package com.nd.ct.common.util;

import java.text.DecimalFormat;

/**
 * @ClassName: NumberUtil
 * @PackageName:com.nd.ct.common.util
 * @Description: 数字工具类
 * @Author LiuSiyang
 * @Date 2022/7/11 10:36
 * @Version 1.0.0
 */
public class NumberUtil {
    /**
     * 将数字格式化为字符串
     * @param num
     * @param length
     * @return
     */
    public static String format(int num, int length){
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            stringBuilder.append("0");
        }
        DecimalFormat decimalFormat = new DecimalFormat(stringBuilder.toString());
        return decimalFormat.format(num);
    }
    //test
    public static void main(String[] args) {
        System.out.println(format(50,6));
    }
}
