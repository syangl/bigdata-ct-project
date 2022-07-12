package com.nd.ct.common.bean;

import java.io.Closeable;
import java.io.IOException;

/**
 * @ClassName: Consumer
 * @PackageName:com.nd.ct.common.bean
 * @Description: 消费者接口
 * @Author LiuSiyang
 * @Date 2022/7/12 8:40
 * @Version 1.0.0
 */
public interface Consumer extends Closeable {
    void consumer() throws IOException;
}
