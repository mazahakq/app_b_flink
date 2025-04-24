package ru.mazahakq.appb.dto;

    // Класс запроса из RabbitMQ
    public class RequestMessage {
        private Long num1;
        private Long num2;
        private String corr_id;
        private String timestamp;

        public RequestMessage() {
        }

        public RequestMessage(String corr_id) {
            this.corr_id = corr_id;
        }

        public Long getNum1() {
            return num1;
        }

        public Long getNum2() {
            return num2;
        }

        public String getCorr_id() {
            return corr_id;
        }

        public String getTimestamp() {
            return timestamp;
        }

    }