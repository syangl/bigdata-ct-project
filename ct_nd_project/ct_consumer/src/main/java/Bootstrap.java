import com.nd.ct.common.bean.Consumer;
import com.nd.ct.consumer.bean.CalllogConsumer;

import java.io.IOException;

/**
 * @ClassName: Bootstrap
 * @PackageName:PACKAGE_NAME
 * @Description: 启动消费者
 * @Author LiuSiyang
 * @Date 2022/7/12 8:55
 * @Version 1.0.0
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        //创建消费者
        Consumer consumer = new CalllogConsumer();
        //消费数据
        consumer.consumer();
        //关闭资源
        consumer.close();
    }
}
