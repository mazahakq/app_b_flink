package ru.mazahakq.appb.operation;

import ru.mazahakq.appb.dto.RequestMessage;
import ru.mazahakq.appb.dto.ResponseMessage;

public class ProcessingAgg {

    public static ResponseMessage operationMessage(RequestMessage message) {
            Long num1 = message.getNum1();
            Long num2 = message.getNum2();
            String corrId = message.getCorr_id();
            Long result = (long) (num1 + num2);
            ResponseMessage response = new ResponseMessage();
            response.setCorr_id(corrId);
            response.setResult(result);
            response.setNumber(num1);
            return response;
    }
}