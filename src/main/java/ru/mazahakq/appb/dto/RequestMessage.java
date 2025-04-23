package ru.mazahakq.appb.dto;

    public class RequestMessage {
        private int num1;
        private int num2;
        private String corr_id;
        private String timestamp;

        public RequestMessage() {
        }

        public RequestMessage(String corr_id) {
            this.corr_id = corr_id;
        }

        public int getNum1() {
            return num1;
        }

        public int getNum2() {
            return num2;
        }

        public String getCorr_id() {
            return corr_id;
        }

        public String getTimestamp() {
            return timestamp;
        }

    }