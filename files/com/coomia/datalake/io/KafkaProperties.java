/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.coomia.flink.demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 生成kafka的消费者
 *
 * @Author: zlzhang0122
 * @Date: 2019/9/4 17:40
 */
public class KafkaProperties<T> {
    private String topic;
    private Properties properties;

    public KafkaProperties(String topic, Properties properties) {
        this.topic = topic;
        this.properties = properties;

        //为使用默认kafka的用户配置基础配置
        this.setDefaultKafkaProperties();
    }

    /**
     * 默认配置
     */
    private void setDefaultKafkaProperties() {
        //启用auto commit offset, 每5s commit一次
        this.properties.setProperty("enable.auto.commit", "true");
        this.properties.setProperty("auto.commit.interval.ms", "5000");
    }

    public FlinkKafkaConsumer<T> buildConsumer(Class<T> clazz) {
        if (checkProperties()) {
            return new FlinkKafkaConsumer<T>(topic, new ConsumerDeserializationSchema(clazz), properties);
        } else {
            return null;
        }
    }

    public FlinkKafkaConsumer<String> buildStringConsumer() {
        if (checkProperties()) {
            return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
        } else {
            return null;
        }
    }

    public FlinkKafkaProducer<String> buildStringProducer() {
        if (checkProperties()) {
            return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
        } else {
            return null;
        }
    }

    /**
     * 验证
     *
     * @return
     */
    private boolean checkProperties() {
        boolean isValued = true;

        if (!properties.containsKey("bootstrap.servers")) {
            isValued = false;
        } else {
            String brokers = properties.getProperty("bootstrap.servers");
            if (brokers == null || brokers.isEmpty()) {
                isValued = false;
            }
        }

        if (this.topic == null || this.topic.isEmpty()) {
            isValued = false;
        }

        if (!properties.containsKey("group.id")) {
            isValued = false;
        } else {
            String groupId = properties.getProperty("group.id");
            if (groupId == null || groupId.isEmpty()) {
                isValued = false;
            }
        }

        return isValued;
    }
}
