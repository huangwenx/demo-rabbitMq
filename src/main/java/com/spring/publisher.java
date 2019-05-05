package com.spring;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Exchange;

import javax.annotation.Resource;

/**
 * ${TODO} 
 * @author yangbin
 * @date 2019/4/29 10:47
 * @since ${TODO} 
 */
public class publisher {

    @Resource
    private AmqpTemplate amqpTemplate;

    public void fun01() {
    }
}