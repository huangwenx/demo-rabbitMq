package com.demo.rpc;

import com.demo.util.MQUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * ${TODO} 
 * @author yangbin
 * @date 2019/4/26 10:24
 * @since ${TODO} 
 */
public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String rpcQueueName = "rpc_queue";

    public RPCClient() throws Exception {
        connection = MQUtil.getConnection();
        channel = connection.createChannel();
    }

    public String call(String message) throws IOException, InterruptedException {
        // 生成唯一的id，全局唯一标识一个请求
        final String correlationId = UUID.randomUUID().toString();

        // 定义临时响应消息队列，存放消费者的回应消息
        String replyQueueName = channel.queueDeclare().getQueue();

        // 发送消息，带上correlationId,并设置消费者回应消息队列
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties()
                .builder()
                .correlationId(correlationId)
                .replyTo(replyQueueName)
                .build();
        channel.basicPublish("", rpcQueueName, basicProperties, message.getBytes("UTF-8"));

        // 消费回应消息，将消息放入阻塞队列中消费
        final BlockingQueue<String> responseData = new ArrayBlockingQueue<>(1);
        String consumeTag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            // 如果correlationId相等，说明这条响应消息是该请求对应的回应消息
            if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                responseData.offer(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });

        // 返回消费者回应的消息，并取消该信道上的指定消费者
        String reply = responseData.take();
        channel.basicCancel(consumeTag);
        return reply;
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}