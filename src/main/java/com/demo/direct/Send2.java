package com.demo.direct;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;

/**
 * 消息生产者
 * <p>消息生产者不需要定义队列，只需要定义routingKey和exchange即可，消息发送到exchange上</p>
 *
 * @author yangbin
 * @since ${TODO}
 */
public class Send2 {

    private static final String ROUTING_KEY = "RK-01";

    private static final String EXCHANGE_NAME = "direct-exchange-yb-01";

    public static void main(String[] args) throws Exception {
        // 1 建立到代理服务器的连接
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.29.1.167");
        factory.setUsername("admin");
        factory.setPassword("admin");

        // 2 获得信道
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            /*
                定义交换器
                第一个参数：交换器名称
                第二个参数：交换器类型
                第三个参数：是否持久化
             */
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);

            // 将信道设置为confirm模式
            channel.confirmSelect();

            /*
                发布消息
                第一个参数：exchange 交换器名称 空字符串表明使用的是默认的交换器
                第二个参数：routingKey 路由键
                第三个参数：信息属性设置
                第四个参数：信息内容
             */
            for (String message : args) {
                System.out.println(String.format("推送消息[%s]成功", message));
                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
//                // 手动同步逐条确认，消息到达交换器就会返回true
//                if (channel.waitForConfirms()) {
//                    System.out.println(String.format("MQ接收消息[%s]成功", message));
//                } else {
//                    System.out.println(String.format("MQ接收消息[%s]失败，消息丢失", message));
//                }
            }

            // 异步监听确认交换器是否收到了消息，适用在web程序中，因为web程序不会运行完就结束
            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println(String.format("MQ接收消息[%s:%b]成功", deliveryTag, multiple));
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println(String.format("MQ接收消息[%s:%b]失败，消息丢失", deliveryTag, multiple));
                }
            });

            // 异步监听执行结果：
            // 可以看出，代码是异步执行的，消息确认有可能是批量确认的.
            // 是否批量确认在于返回的multiple的参数，此参数为bool值.
            // 如果true表示批量执行了deliveryTag这个值以前的所有消息，如果为false的话表示单条确认。
            /*
            * MQ接收消息[2:true]成功
            * MQ接收消息[3:false]成功
            * MQ接收消息[4:false]成功
            * MQ接收消息[5:false]成功
            * MQ接收消息[6:false]成功
            * */

            // sleep 10s保证有时间让监听器执行方法
            Thread.sleep(10000);

        }
    }
}