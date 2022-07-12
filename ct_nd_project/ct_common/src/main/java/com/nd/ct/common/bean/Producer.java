package com.nd.ct.common.bean;

import java.io.Closeable;
import java.io.IOException;

/**
 * @ClassName: Producer
 * @PackageName:com.nd.ct.common.bean
 * @Description: 生产者接口
 * @Author LiuSiyang
 * @Date 2022/7/11 9:24
 * @Version 1.0.0
 */
public interface Producer extends Closeable {
    //data input
    void setIn(DataIn in);
    //data output
    void setOut(DataOut out);
    //produce data
    void produce() throws IOException;
}
