package com.demo.fanout;

import com.demo.util.MQUtil;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Envelope;

/**
 * ${TODO} 
 * @author yangbin
 * @date 2019/4/25 14:18
 * @since ${TODO} 
 */
public class ReceiveLog {

    private static final String EXCHANGE_NAME = "fanout-exchange-yb-01";

    public static void main(String[] args) throws Exception {

        // 连接到mq代理服务器
        Connection connection = MQUtil.getConnection();

        // 获取信道
        Channel channel = connection.createChannel();

        // 创建交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        // 创建一个私有的临时的队列 exclusive=true auto-delete=true durable=false
        String queueName = channel.queueDeclare().getQueue();

        // 绑定队列到交换器
        channel.queueBind(queueName, EXCHANGE_NAME, "");
        System.out.println("wait to consume message...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageContent = new String(delivery.getBody());
            System.out.println(String.format("fanout消费消息：%s", messageContent));
        };

        // 消费消息
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });


    }
}