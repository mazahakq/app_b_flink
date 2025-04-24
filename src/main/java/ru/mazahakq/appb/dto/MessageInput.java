package ru.mazahakq.appb.dto;

    // Вспомогательный класс для обработки сообщений
    public class MessageInput {
        private String guid;
        private Long number;

        public MessageInput() {
        }

        public MessageInput(String guid, Long number) {
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