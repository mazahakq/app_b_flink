package ru.mazahakq.appb;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class RpcHandler extends ProcessFunction<byte[], Void> {

    private transient Channel rabbitChannel;

    @Override
    public void open(org.apache.flink.configuration.Configuration config) throws IOException, TimeoutException {
        rabbitChannel = createChannel();
    }

    @Override
    public void processElement(byte[] value, Context ctx, Collector<Void> out) throws Exception {
        // Преобразуем байты в строку
        String bodyStr = new String(value, StandardCharsets.UTF_8);
        JSONObject requestData = new JSONObject(bodyStr);
    
        // Читаем числа
        int n1 = requestData.getInt("num1");
        int n2 = requestData.getInt("num2");
        int result = n1 + n2;
    
        // Формируем ответ
        JSONObject response = new JSONObject().put("result", result);
    
        // Чтение свойств
        String replyTo = requestData.getString("reply_to");
        String correlationId = requestData.getString("correlation_id");
    
        // Отправляем ответ клиенту
        sendResponse(response.toString(), rabbitChannel, replyTo, correlationId);
    }
    
    private void sendResponse(String response, Channel channel, String replyTo, String correlationId) throws IOException {
        // Создаем новые свойства сообщения
        AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .contentType("application/json")
                .build();
    
        // Отправляем ответ в очередь, указанную клиентом
        channel.basicPublish("", replyTo, replyProps, response.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Создание канала связи с RabbitMQ.
     */
    private Channel createChannel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("rabbitmq");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");

        Connection conn = factory.newConnection();
        return conn.createChannel();
    }

    @Override
    public void close() throws IOException, TimeoutException {
        if (rabbitChannel != null && rabbitChannel.isOpen()) {
            rabbitChannel.close();
        }
    }
}
