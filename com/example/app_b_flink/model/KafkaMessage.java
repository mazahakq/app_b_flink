package com.example.app_b_flink.model;

public class KafkaMessage {
    private final String number;
    private final String guid;

    public KafkaMessage(String number, String guid) {
        this.number = number;
        this.guid = guid;
    }

    public String getNumber() { return number; }
    public String getGuid() { return guid; }
    
    @Override
    public String toString() {
        return "KafkaMessage{" +
               "number='" + number + '\'' +
               ", guid='" + guid + '\'' +
               '}';
    }
}