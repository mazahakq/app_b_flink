package ru.mazahakq.appb.dto;

    // Класс объекта в State
    public class MessageState {
        private String guid;
        private Long number;

        public MessageState() {
        }

        public MessageState(String guid, Long number) {
            this.guid = guid;
            this.number = number;
        }

        public String getGuid() {
            return guid;
        }

        public void setGuid(String guid) {
            this.guid = guid;
        }

        public Long getNumber() {
            return number;
        }

        public void setNumber(Long number) {
            this.number = number;
        }
    }