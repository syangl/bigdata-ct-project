package com.nd.ct.common.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * @ClassName: DataIn
 * @PackageName:com.nd.ct.common.bean
 * @Description: 数据输入
 * @Author LiuSiyang
 * @Date 2022/7/11 9:23
 * @Version 1.0.0
 */
public interface DataIn extends Closeable {
    //input path
    void setPath(String path);
    //read data
    Object read() throws IOException;
    <T extends Data> List<T> read(Class<T> clazz) throws IOException;
}
