package com.demo.util;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * ${TODO}
 * @author yangbin
 * @date 2019/4/25 14:20
 * @since ${TODO}
 */
public class MQUtil {

    public static Connection getConnection() throws Exception {
        return getConnection(null);
    }

    public static Connection getConnection(String vhost) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(HostConstant.HOST_NAME);
        connectionFactory.setUsername(HostConstant.USERNAME);
        connectionFactory.setPassword(HostConstant.PASSWORD);
        if (!StringUtils.isEmpty(vhost)) {
            connectionFactory.setVirtualHost(vhost);
        }
        return connectionFactory.newConnection();
    }
}