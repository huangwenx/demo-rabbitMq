package com.demo.direct;

import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Envelope;

/**
 * 消息消费者
 * @author yangbin
 * @date 2019/1/10 18:11
 * @since ${TODO} 
 */
public class Recv2 {

    private static final String QUEUE_NAME = "direct-queue-yb-001";
    private static final String ROUTING_KEY = "RK-01";
    private static final String EXCHANGE_NAME = "direct-exchange-yb-01";

    public static void main(String[] args) throws Exception {
        // 1 建立连接
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.29.1.167");
        factory.setUsername("admin");
        factory.setPassword("admin");
        Connection connection = factory.newConnection();

        // 2 获取信道
        Channel channel = connection.createChannel();

        // 设置prefetch为1，使用情况：当多个消费者中有一些消费者消费很慢的才能进行确认的时候，告诉mq在这个消费者回送ack之前不再发消息给他，而是将消息分发到其他消费者。
        // 默认情况下，即使其中的一个消费者消费很慢，MQ仍然会均衡发送消息到这个消费者
        // 设置为1，标识该channel只能有1个未处于ack状态的delivery，如果已经有了一个，则改channel中不会再进行消息的传递，会大大的降低吞吐量
        channel.basicQos(100);

        // 创建交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);

        // 创建队列
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        // 将队列绑定到交换器上
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        System.out.println("waiting for message...");

        // 3 回调函数
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageContent = new String(delivery.getBody());
            System.out.println(String.format("消费消息：%s", messageContent));
            try {
                doWork(messageContent);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                // 模拟某种情况下处理任务时间很长的场景
                if (args.length > 0 && Boolean.parseBoolean(args[0])) {
                    try {
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                // 该方法表明确认指定的deliveryTag这条消息（如果设置为true，则会确认该channel上的所有到达的消息）
                // 如果设置了autoAck为false，一定要显示的确认消息。否则会造成消息rabbitMQ无法收到消息的确认信息，该消息就会一直保留在队列中不会删除，这样会导致占用内存越来越大
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println(String.format("消费[%s]完毕，手动确认ack", messageContent));
            }
        };

        // 4 消费消息，该方法是一个无限循环阻塞方法，会一直循环等待接收消息
        // 当autoAck为true时，收到消费者消息后，会自动确认。这样会导致一个问题：当该消费者后续消费这个消息出错的情况下，这条消息就会丢掉，不会再次发送给下一个消费者了
        // 为了避免上述情况造成的消息丢失，设置autoAck为false(缺省值默认是false)，当消费者消费完成消息时手动确认
        channel.basicConsume(QUEUE_NAME, deliverCallback, consumerTag -> {
            System.err.println(String.format("系统异常，消费者[%s]不能正常消费消息", consumerTag));
        });
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            if (ch == '.') {
                Thread.sleep(1000);
            }
        }
    }

}