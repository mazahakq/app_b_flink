package ru.mazahakq.appb.dto;

    // Вспомогательный класс для обработки сообщений
    public class Message {
        private String guid;
        private int number;

        public Message() {
        }

        public Message(String guid, int number) {
            this.guid = guid;
            this.number = number;
        }

        public String getGuid() {
            return guid;
        }

        public void setGuid(String guid) {
            this.guid = guid;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }
    }