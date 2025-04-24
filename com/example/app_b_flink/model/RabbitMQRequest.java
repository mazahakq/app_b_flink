package com.example.app_b_flink.model;

public class RabbitMQRequest {
    private final int num1;
    private final int num2;
    private final String corrId;
    private final long timestamp;

    public RabbitMQRequest(int num1, int num2, String corrId, long timestamp) {
        this.num1 = num1;
        this.num2 = num2;
        this.corrId = corrId;
        this.timestamp = timestamp;
    }

    public int getNum1() { return num1; }
    public int getNum2() { return num2; }
    public String getCorrId() { return corrId; }
    public long getTimestamp() { return timestamp; }
    
    @Override
    public String toString() {
        return "RabbitMQRequest{" +
               "num1=" + num1 +
               ", num2=" + num2 +
               ", corrId='" + corrId + '\'' +
               ", timestamp=" + timestamp +
               '}';
    }
}