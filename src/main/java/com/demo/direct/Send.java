package com.demo.direct;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 消息生产者
 * <p>消息生产者不需要创建队列，只负责将消息发送到交换器，并告知相应的RoutingKey</p>
 * @author yangbin
 * @date 2019/1/10 17:31
 * @since ${TODO} 
 */
public class Send {

    private static final String ROUTING_KEY = "direct-queue-yb-001";

    public static void main(String[] args) throws Exception {
        // 1 建立到代理服务器的连接
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.29.1.167");
        factory.setUsername("admin");
        factory.setPassword("admin");

        // 2 获得信道
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            // 发布消息的属性设置：发送纯文本类型消息
            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties();
            basicProperties = basicProperties.builder().contentType("text/plain").contentEncoding("UTF-8").build();

            /*
                发布消息
                第一个参数：exchange 交换器名称 空字符串表明使用的是默认的交换器
                第二个参数：routingKey 路由键
                第三个参数：信息属性设置
                第四个参数：信息内容
             */
            String message = args[0];
            channel.basicPublish("", ROUTING_KEY, basicProperties, message.getBytes());
            System.out.println(String.format("推送消息[%s]成功", message));
        }
    }
}