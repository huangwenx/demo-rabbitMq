package com.demo.fanout;

import com.demo.util.HostConstant;
import com.demo.util.MQUtil;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 发送者
 * @author yangbin
 * @date 2019/4/24 17:07
 * @since ${TODO} 
 */
public class EmitLog {

    private static final String EXCHANGE_NAME = "fanout-exchange-yb-01";

    public static void main(String[] args) throws Exception {
        // 创建连接，获取信道
        try (Connection connection = MQUtil.getConnection(); Channel channel = connection.createChannel()) {
            // 创建交换器：auto-delete=false  durable=false
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            // 发送消息
            String message = "hello world";
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
            System.out.println(String.format("推送消息[%s]成功", message));
        }
    }
}