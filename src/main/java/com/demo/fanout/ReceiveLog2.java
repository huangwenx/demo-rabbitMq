package com.demo.fanout;

import com.demo.util.MQUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

/**
 * ${TODO} 
 * @author yangbin
 * @date 2019/4/25 14:18
 * @since ${TODO} 
 */
public class ReceiveLog2 {

    private static final String EXCHANGE_NAME = "direct-exchange-yb-02";

    public static void main(String[] args) throws Exception {

        // 连接到mq代理服务器
        Connection connection = MQUtil.getConnection();

        // 获取信道
        Channel channel = connection.createChannel();

        // 创建交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        // 创建一个私有的临时的队列 exclusive=true auto-delete=true durable=false
        String queueName = channel.queueDeclare().getQueue();

        if (args.length < 1) {
            System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
            System.exit(1);
        }

        // 将一个队列通过多个routingKey绑定到交换器
        for (String routingKey : args) {
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }
        System.out.println("wait to consume message...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageContent = new String(delivery.getBody());
            String routingKey = delivery.getEnvelope().getRoutingKey();
            System.out.println(String.format("direct消费消息：routingKey: [%s], message: [%s]", routingKey, messageContent));
        };

        // 消费消息
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });


    }
}