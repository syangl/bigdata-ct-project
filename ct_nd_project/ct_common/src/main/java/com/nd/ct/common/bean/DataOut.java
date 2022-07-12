package com.nd.ct.common.bean;

import java.io.Closeable;
import java.io.IOException;

/**
 * @ClassName: DataOut
 * @PackageName:com.nd.ct.common.bean
 * @Description: 数据的输出
 * @Author LiuSiyang
 * @Date 2022/7/11 9:24
 * @Version 1.0.0
 */
public interface DataOut extends Closeable {
    //output path
    void setPath(String path);
    void write(Object obj) throws Exception;
    void write(String obj) throws Exception;
}
