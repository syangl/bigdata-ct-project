package com.nd.ct.producer.bean;

import com.nd.ct.common.bean.Data;

/**
 * @ClassName: Contact
 * @PackageName:com.nd.ct.producer.bean
 * @Description: 联系人
 * @Author LiuSiyang
 * @Date 2022/7/11 9:55
 * @Version 1.0.0
 */
public class Contact extends Data {
    //电话号码
    private String tell;
    //名字
    private String name;

    public String getTell() {
        return tell;
    }

    public void setTell(String tell) {
        this.tell = tell;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setValue(Object val) {
        content= (String) val;
        String[] split = content.split("\t");
        setTell(split[0]);
        setName(split[1]);
    }

    @Override
    public String toString() {
        return "Contact[" +
                "tell='" + tell + '\'' +
                ", name='" + name + '\'' +
                ']';
    }

}
