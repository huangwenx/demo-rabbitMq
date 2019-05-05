package com.demo.topic;

import com.demo.util.MQUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * ${TODO} 
 * @author yangbin
 * @date 2019/4/25 19:05
 * @since ${TODO} 
 */
public class EmitTopic {

    private static final String EXCHANGE_NAME = "topic-exchange-yb-01";

    public static void main(String[] args) throws Exception {
        // 创建连接，获取信道
        try (Connection connection = MQUtil.getConnection(); Channel channel = connection.createChannel()) {
            // 创建交换器：auto-delete=false  durable=false
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            // 发送消息
            String message = System.getProperty("message");
            String routingKey = System.getProperty("routingKey");
            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
            System.out.println(String.format("TOPIC推送消息[%s]成功", message));
        }
    }

}