package com.chaojianok.flink.connectors.rabbitmq.java.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtil {

    private static Logger logger = LoggerFactory.getLogger(PropertyUtil.class);

    public static Properties loadProperties() {
        Properties properties = new Properties();
        InputStream in = PropertyUtil.class.getClassLoader().getResourceAsStream("application.properties");
        try {
            properties.load(in);
        } catch (IOException e) {
            logger.warn("加载配置文件失败，warn=" + e);
        }
        return properties;
    }

    public static String getValue(String key) {
        return loadProperties().getProperty(key);
    }

}
