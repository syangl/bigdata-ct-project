package com.nd.ct.producer;

import com.nd.ct.common.bean.Producer;
import com.nd.ct.producer.bean.LocalFileProducer;
import com.nd.ct.producer.io.LocalFileDataIn;
import com.nd.ct.producer.io.LocalFileDataOut;

import java.io.IOException;

/**
 * @ClassName: Bootstrap
 * @PackageName:com.nd.ct.producer
 * @Description: 启动对象
 * @Author LiuSiyang
 * @Date 2022/7/11 9:21
 * @Version 1.0.0
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        if (args.length <= 0){
            System.out.println("系统参数不正确，请按照格式传参：java -jar producer.jar contact.log call.log");
            System.exit(-1);
        }
        //构建生产者对象
        Producer producer = new LocalFileProducer();
//        producer.setIn(new LocalFileDataIn("D:\\Studyandwork\\资料文件\\学习\\中软实习学习\\南开大学实训\\笔记\\day11-电信项目\\contact.log"));
//        producer.setOut(new LocalFileDataOut("D:\\Studyandwork\\资料文件\\学习\\中软实习学习\\南开大学实训\\笔记\\day11-电信项目\\call.log"));
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));
        //生产数据
        producer.produce();
        //关闭生产者对象
        producer.close();
    }
}
