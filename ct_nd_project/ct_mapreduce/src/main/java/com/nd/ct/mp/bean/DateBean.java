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
 * @ClassName: DateBean
 * @PackageName:com.nd.ct.mp
 * @Description: date序列化
 * @Author LiuSiyang
 * @day 2022/7/13 14:13
 * @Version 1.0.0
 */
public class DateBean implements DBWritable, Writable {
    private int id;
    private String year;
    private String month;
    private String day;
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(year);
        out.writeUTF(month);
        out.writeUTF(day);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.year = in.readUTF();
        this.month = in.readUTF();
        this.day = in.readUTF();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1,id);
        preparedStatement.setString(2,year);
        preparedStatement.setString(3,month);
        preparedStatement.setString(4,day);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getInt("id");
        this.year = resultSet.getString("year");
        this.month = resultSet.getString("month");
        this.day = resultSet.getString("day");
    }

    public DateBean() {
    }

    public DateBean(int id, String year, String month, String day) {
        this.id = id;
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }
}
