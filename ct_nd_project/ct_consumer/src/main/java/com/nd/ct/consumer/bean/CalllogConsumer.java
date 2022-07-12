package com.nd.ct.consumer.bean;

import com.nd.ct.common.bean.Consumer;
import com.nd.ct.common.constact.Names;
import com.nd.ct.consumer.dao.HBaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @ClassName: CalllogConsumer
 * @PackageName:com.nd.ct.consumer.bean
 * @Description: 通话日志消费者对象
 * @Author LiuSiyang
 * @Date 2022/7/12 9:10
 * @Version 1.0.0
 */
public class CalllogConsumer implements Consumer {
    //消费数据
    @Override
    public void consumer() {
        try {
            //创建配置对象
            Properties prop=new Properties();
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
            //获取flume采集的数据
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
            //关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));
            //实现HBase存储
            HBaseDao hBaseDao = new HBaseDao();
            //初始化
            hBaseDao.init();
            //消费数据
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                    //存储数据
                    hBaseDao.insertData(record.value());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {

    }
}
