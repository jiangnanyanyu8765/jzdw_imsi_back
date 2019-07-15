package com.bonc.mrImsiBack.test;

import mrLocateV2.mrdata.MrPoint;

public class MrKey implements Comparable<MrKey> {
    private long timestamp;
    private MrPoint mr;

    public MrKey(MrPoint mr) {
        this.timestamp = mr.getTimeStamp();
        this.mr = mr;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public MrPoint getMr() {
        return mr;
    }

    public void setMr(MrPoint mr) {
        this.mr = mr;
    }

    @Override
    public int compareTo(MrKey o) {
        return (int)(this.timestamp - o.getTimestamp());
    }
}
