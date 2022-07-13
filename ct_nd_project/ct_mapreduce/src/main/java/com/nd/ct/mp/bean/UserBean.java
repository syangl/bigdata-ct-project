package com.nd.ct.mp.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @ClassName: UserBean
 * @PackageName:com.nd.ct.mp
 * @Description: user序列化
 * @Author LiuSiyang
 * @Date 2022/7/13 14:59
 * @Version 1.0.0
 */
public class UserBean implements DBWritable, Writable {
    private String tell;
    private String name;
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tell);
        out.writeUTF(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tell = in.readUTF();
        this.name = in.readUTF();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,tell);
        preparedStatement.setString(2,name);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.tell = resultSet.getString("tell");
        this.name = resultSet.getString("name");
    }

    public UserBean() {
    }

    public UserBean(String tell, String name) {
        this.tell = tell;
        this.name = name;
    }

    public String getTell() {
        return tell;
    }

    public void setTell(String tell) {
        this.tell = tell;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
