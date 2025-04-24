package com.example.app_b_flink.model;

public class ResultResponse {
    private final int number;
    private final int result;
    private final String corrId;
    private final String guid;

    public ResultResponse(int number, int result, String corrId, String guid) {
        this.number = number;
        this.result = result;
        this.corrId = corrId;
        this.guid = guid;
    }

    public int getNumber() { return number; }
    public int getResult() { return result; }
    public String getCorrId() { return corrId; }
    public String getGuid() { return guid; }
    
    @Override
    public String toString() {
        return "ResultResponse{" +
               "number=" + number +
               ", result=" + result +
               ", corrId='" + corrId + '\'' +
               ", guid='" + guid + '\'' +
               '}';
    }
}