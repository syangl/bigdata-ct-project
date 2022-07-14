package com.nd.ct.cache;

import com.nd.ct.common.util.JdbcUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @ClassName: Bootstrap
 * @PackageName:com.nd.ct.cache
 * @Description: 启动缓存客户端，向redis中增加缓存数据
 * @Author LiuSiyang
 * @Date 2022/7/14 16:58
 * @Version 1.0.0
 */
public class Bootstrap {
    public static void main(String[] args) {
        //读取mysql数据
        Map<String,Integer> userMap=new HashMap<>();
        Map<String,Integer> dateMap=new HashMap<>();
        Connection connection=null;
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            connection= JdbcUtil.getConnection();
            //读取用户、时间数据
            String queryUserSql="select uid,tell from tb_user";
            ps = connection.prepareStatement(queryUserSql);
            rs = ps.executeQuery();
            while(rs.next()){
                Integer uid=rs.getInt(1);
                String tell=rs.getString(2);
                userMap.put(tell,uid);
            }
            rs.close();
            //时间
            String queryDateSql="select id,year,month,day from tb_date";
            ps = connection.prepareStatement(queryDateSql);
            rs=ps.executeQuery();
            while(rs.next()){
                Integer id=rs.getInt(1);
                String year=rs.getString(2);
                String month=rs.getString(3);
                if(month.length()==1){
                    month="0"+month;
                }
                String day=rs.getString(4);
                if(day.length()==1){
                    day="0"+day;
                }
                dateMap.put(year+month+day,id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println(userMap.size());
        System.out.println(dateMap.size());
        //向redis存储数据
        Jedis jedis=new Jedis("hadoop104",6379);
        jedis.auth("123456");
        Iterator<String> keyIterator = userMap.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key=keyIterator.next();
            Integer value = userMap.get(key);
            jedis.hset("tb_user",key,""+value);
        }
        keyIterator=dateMap.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key=keyIterator.next();
            Integer value = dateMap.get(key);
            jedis.hset("tb_date",key,""+value);
        }
    }
}
