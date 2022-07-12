package com.nd.ct.common.constact;

import com.nd.ct.common.bean.Val;

/**
 * @ClassName: Names
 * @PackageName:com.nd.ct.common.constact
 * @Description: //名称常量枚举类
 * @Author LiuSiyang
 * @Date 2022/7/11 9:27
 * @Version 1.0.0
 */
public enum Names implements Val {
    //namespace
    NAMESPACE("ct"),//命名空间
    TOPIC("ct"),//主题
    TABLE("ct:calllog"),//通话日志
    CF_CALLER("caller"),//列族，主叫
    CF_CALLEE("callee"),//列族，被叫
    CF_INFO("info"),//列族
    ;
    private String name;
    private Names(String name){
        this.name=name;
    }

    @Override
    public void setValue(Object val) {
        this.name=(String) val;
    }

    @Override
    public String getValue() {
        return name;
    }
}
