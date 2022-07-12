package com.nd.ct.common.bean;

/**
 * @ClassName: Data
 * @PackageName:com.nd.ct.common.bean
 * @Description: 数据对象
 * @Author LiuSiyang
 * @Date 2022/7/11 9:25
 * @Version 1.0.0
 */
public class Data implements Val{
    public String content;

    @Override
    public void setValue(Object val) {
        content = (String) val;
    }

    @Override
    public Object getValue() {
        return content;
    }
}
