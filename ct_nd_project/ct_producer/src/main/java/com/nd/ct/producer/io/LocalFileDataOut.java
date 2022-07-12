package com.nd.ct.producer.io;

import com.nd.ct.common.bean.DataOut;

import java.io.*;

/**
 * @ClassName: LocalFileDataOut
 * @PackageName:com.nd.ct.producer.io
 * @Description: TODO
 * @Author LiuSiyang
 * @Date 2022/7/11 9:37
 * @Version 1.0.0
 */
public class LocalFileDataOut implements DataOut {
    private PrintWriter writer=null;
    public LocalFileDataOut(String path) {
        setPath(path);
    }

    @Override
    public void setPath(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream((path))));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Object obj) throws Exception {
        write(obj.toString());
    }

    /**
     * 将数据字符串生成到文件中
     * @param data
     * @throws Exception
     */
    @Override
    public void write(String data) throws Exception {
        //把流中的文件放到文件中
        writer.println(data);
        writer.flush();
    }

    /**
     * 释放资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (writer!=null){
            writer.close();
        }
    }
}
