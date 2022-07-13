package com.nd.ct.consumer.coprocessor;


import com.nd.ct.common.bean.BaseHBaseDao;
import com.nd.ct.common.constact.Names;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;
import java.util.Optional;

/**
 * @ClassName: InsertCalleeCoprocessor
 * @PackageName:com.nd.ct.consumer.coprocessor
 * @Description: 使用协处理器保存被叫用户的数据
 * @Author LiuSiyang
 * @Date 2022/7/13 9:35
 * @Version 1.0.0
 * 协处理的使用
 * 1.创建表
 * 2.让表直接协处理类（和表有关联）
 * 3.打包
 */
public class InsertCalleeCoprocessor implements RegionCoprocessor, RegionObserver {
    /**
     * 保存主叫用户数据之后，由hbase自动保存被叫用户数据
     * @param c
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c,
                        Put put, WALEdit edit, Durability durability) throws IOException {
        //获取表
        Table table = c.getEnvironment().getConnection().getTable(TableName.valueOf(Names.TABLE.getValue()));
        //主叫用户的rowKey
        String callerRowKey = Bytes.toString(put.getRow());
        //1_156_2022_133_1012_1
        String[] split = callerRowKey.split("_");
        String call1 = split[1];
        String call2 = split[3];
        String callTime = split[2];
        String duration = split[4];
        String flag = split[5];
        //只有主叫用户保存后才需要触发被叫用户的保存
        if ("1".equals(flag)) {
            CoprocessorDao dao = new CoprocessorDao();
            String calleeRowKey=dao.getRegionNum(call2,callTime)+"_"+call2+"_"+
                    callTime+"_"+call1+"_"+duration+"_0";
            //保存数据
            Put calleePut=new Put(Bytes.toBytes(calleeRowKey));
            byte[] calleefamily= Bytes.toBytes(Names.CF_CALLEE.getValue());
            calleePut.addColumn(calleefamily,Bytes.toBytes("call2"),Bytes.toBytes(call2));
            calleePut.addColumn(calleefamily,Bytes.toBytes("call1"),Bytes.toBytes(call1));
            calleePut.addColumn(calleefamily,Bytes.toBytes("calltime"),Bytes.toBytes(callTime));
            calleePut.addColumn(calleefamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));
            calleePut.addColumn(calleefamily,Bytes.toBytes("flag"),Bytes.toBytes("0"));

            System.out.println(calleePut.toString());
            table.put(calleePut);
        }
        table.close();
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    //内部类
    private class CoprocessorDao extends BaseHBaseDao {
        public int getRegionNum(String tel, String date) {
            return genRegionNum(tel, date);
        }
    }
}
