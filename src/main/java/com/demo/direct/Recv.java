package com.demo.direct;

import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Envelope;

/**
 * ${TODO} 
 * @author yangbin
 * @date 2019/1/10 18:11
 * @since ${TODO} 
 */
public class Recv {

    private static final String QUEUE_NAME = "direct-queue-yb-001";

    public static void main(String[] args) throws Exception {
        // 1 建立连接
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.29.1.167");
        factory.setUsername("admin");
        factory.setPassword("admin");
        Connection connection = factory.newConnection();

        // 2 获取信道
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println("waiting for message...");

        // 3 回调函数
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            BasicProperties basicProperties = delivery.getProperties();
            Envelope messageEnvelope = delivery.getEnvelope();
            String messageContent = new String(delivery.getBody());
            System.out.println(String.format("消费消息：%s", messageContent));
        };

        // 4 消费消息，该方法是一个无限循环阻塞方法，会一直循环等待接收消息
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
            System.err.println(String.format("系统异常，消费者[%s]不能正常消费消息", consumerTag));
        });
    }

}