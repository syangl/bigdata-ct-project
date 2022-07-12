package com.nd.ct.producer.io;

import com.nd.ct.common.bean.Data;
import com.nd.ct.common.bean.DataIn;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: LocalFileDataIn
 * @PackageName:com.nd.ct.producer.io
 * @Description: 本地数据输入
 * @Author LiuSiyang
 * @Date 2022/7/11 9:36
 * @Version 1.0.0
 */
public class LocalFileDataIn implements DataIn {
    //string input stream
    private BufferedReader reader = null;

    public LocalFileDataIn(String path) {
        setPath(path);
    }

    @Override
    public void setPath(String path) {
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }


    @Override
    public Object read() throws IOException {
        return null;
    }

    /**
     * 读取数据，返回集合
     *
     * @param clazz
     * @param <T>
     * @return
     * @throws IOException
     */
    @Override
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {
        //create a list
        List<T> ts = new ArrayList<>();
        try {
            //read data
            String line = null;
            while ((line = reader.readLine()) != null) {
                //数据转换为指定类型的对象，封装为集合返回
                T t = clazz.newInstance();
                t.setValue(line);
                ts.add(t);
            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return ts;
    }

    @Override
    public void close() throws IOException {
        if(reader!=null){
            reader.close();
        }
    }
}
