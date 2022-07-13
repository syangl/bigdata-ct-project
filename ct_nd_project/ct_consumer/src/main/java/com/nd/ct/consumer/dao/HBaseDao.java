package com.nd.ct.consumer.dao;

import com.nd.ct.common.bean.BaseHBaseDao;
import com.nd.ct.common.constact.Names;
import com.nd.ct.common.constact.ValueConstant;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: HBaseDao
 * @PackageName:com.nd.ct.consumer.dao
 * @Description: TODO
 * @Author LiuSiyang
 * @Date 2022/7/12 10:15
 * @Version 1.0.0
 */
public class HBaseDao extends BaseHBaseDao {
    //初始化
    public void init() throws IOException {
        start();
        //创建命名空间
        createNamespaceNX(Names.NAMESPACE.getValue());
        //创建表
        createTableXX(
                Names.TABLE.getValue(),
                "com.nd.ct.consumer.coprocessor.InsertCalleeCoprocessor",
                ValueConstant.REGION_COUNT,
                Names.CF_CALLER.getValue(),
                Names.CF_CALLEE.getValue()
        );
        end();
    }

    //插入数据
    public void insertData(String data) throws IOException {
        //将通话日志保存到HBase表中
        //1.获取通话日志数据
        String[] values = data.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];
        //2.创建数据对象
        /*
       rowKey设计
       (1)长度原则
            最大长值64KB,推荐长为0~100byte
            最好8的倍数,能短则短，如果rowkey太长会影响存储性能
       (2)唯一原则:rowKey应该具备唯一性
       (3)散列原则
            盐值散列：不能使用时间戳直接作为rowKey,会导致数据倾斜,在rowkey前加随机数
            字符串反转:可以在时间戳反转,用的最多的地方是在时间戳和电话号码
                      15623513131=>13131532651
            计算分区号:让分区号没有规律就可以,hashMap
        */
        //rowKey=regionNum+call1+time+call2+duration
        //主叫用户
        String callerRowKey = genRegionNum(call1, calltime) + "_" + call1 + "_" + calltime + "_" + call2 + "_" + duration + "_1";
        Put callerPut = new Put(Bytes.toBytes(callerRowKey));
        byte[] callerfamily = Bytes.toBytes(Names.CF_CALLER.getValue());
        //增加列
        callerPut.addColumn(callerfamily, Bytes.toBytes("call1"), Bytes.toBytes(call1));
        callerPut.addColumn(callerfamily, Bytes.toBytes("call2"), Bytes.toBytes(call2));
        callerPut.addColumn(callerfamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
        callerPut.addColumn(callerfamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
        //1表示主叫
        callerPut.addColumn(callerfamily,Bytes.toBytes("flag"),Bytes.toBytes("1"));

        /**
         *         //被叫用户（添加协处理后就不需要在这里插入被叫记录）
         *         String calleeRowKey = genRegionNum(call2, calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";
         *         Put calleePut=new Put(Bytes.toBytes(calleeRowKey));
         *         byte[] calleefamily=Bytes.toBytes(Names.CF_CALLEE.getValue());
         *         //增加列
         *         calleePut.addColumn(calleefamily,Bytes.toBytes("call2"),Bytes.toBytes(call2));
         *         calleePut.addColumn(calleefamily,Bytes.toBytes("call1"),Bytes.toBytes(call1));
         *         calleePut.addColumn(calleefamily,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
         *         calleePut.addColumn(calleefamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));
         *         //0表示被叫
         *         calleePut.addColumn(calleefamily,Bytes.toBytes("flag"),Bytes.toBytes("0"));
         */

        //3.保存数据
        List<Put> puts = new ArrayList<>();
        puts.add(callerPut);
        //添加协处理后就不需要在这里插入被叫记录
        //puts.add(calleePut);
        putData(Names.TABLE.getValue(), puts);
    }

    /**
     * 增加多条数据
     * @param name
     * @param puts
     */
    protected void putData(String name, List<Put> puts) throws IOException {
        //获取表对象
        Connection conn = getConnect();
        Table table = conn.getTable(TableName.valueOf(name));
        //增加数据
        table.put(puts);
        //释放资源
        table.close();
    }
}
