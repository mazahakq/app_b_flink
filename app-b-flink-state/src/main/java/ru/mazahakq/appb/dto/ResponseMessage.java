package ru.mazahakq.appb.dto;

    // Класс ответа в RabbitMQ
    public class ResponseMessage {
        private Long result;
        private Long number;
        private String corr_id;
        private String guid;

        public ResponseMessage() {
        }

        public ResponseMessage(String corr_id) {
            this.corr_id = corr_id;
        }

        public ResponseMessage(Long number, Long result, String corr_id, String guid) {
            this.number = number;
            this.result = result;
            this.corr_id = corr_id;
            this.guid = guid;
        }

        public Long getResult() {
            return result;
        }

        public String getCorr_id() {
            return corr_id;
        }

        public String getGuid() {
            return guid;
        }

        public Long getNumber() {
            return number;
        }

        public void setCorr_id(String corr_id) {
            this.corr_id = corr_id;
        }

        public void setResult(Long result) {
            this.result = result;
        }

        public void setGuid(String guid) {
            this.guid = guid;
        }

        public void setNumber(Long number) {
            this.number = number;
        }

    }