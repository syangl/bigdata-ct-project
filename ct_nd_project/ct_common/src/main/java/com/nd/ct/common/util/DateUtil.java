package com.nd.ct.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName: DateUtil
 * @PackageName:com.nd.ct.common.util
 * @Description: 日期工具类
 * @Author LiuSiyang
 * @Date 2022/7/11 10:22
 * @Version 1.0.0
 */
public class DateUtil {
    /**
     * 将指定的日期按照指定的格式格式化为字符串
     * @param date
     * @param format
     * @return
     */
    public static String format(Date date, String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        return simpleDateFormat.format(date);
    }
    /**
     * 将日期字符串按照指定的格式解析为日期对象
     * @param dateString
     * @param format
     * @return
     */
    public static Date parse(String dateString,String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = simpleDateFormat.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }
}
