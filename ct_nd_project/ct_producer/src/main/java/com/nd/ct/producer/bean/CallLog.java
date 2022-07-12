package com.nd.ct.producer.bean;

/**
 * @ClassName: CallLog
 * @PackageName:com.nd.ct.producer.bean
 * @Description: 通话记录
 * @Author LiuSiyang
 * @Date 2022/7/11 10:42
 * @Version 1.0.0
 */
public class CallLog {
    private String call1;//主叫
    private String call2;//被叫
    private String callTime;//通话时间
    private String duration;//通话时长

    @Override
    public String toString() {
        return call1 +
                "\t" + call2  +
                "\t" + callTime +
                "\t" + duration;
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCallTime() {
        return callTime;
    }

    public void setCallTime(String callTime) {
        this.callTime = callTime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public CallLog(String call1, String call2, String callTime, String duration) {
        this.call1 = call1;
        this.call2 = call2;
        this.callTime = callTime;
        this.duration = duration;
    }
}
