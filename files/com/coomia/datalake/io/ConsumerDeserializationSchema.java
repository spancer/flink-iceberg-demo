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

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/4 17:43
 */
public class ConsumerDeserializationSchema<T> implements DeserializationSchema<T> {
  private Class<T> clazz;

  public ConsumerDeserializationSchema(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public T deserialize(byte[] bytes) throws IOException {
    // 确保 new String(bytes) 是json 格式，如果不是，请自行解析
    return JSON.parseObject(new String(bytes), clazz);
  }

  @Override
  public boolean isEndOfStream(T t) {
    return false;
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return TypeExtractor.getForClass(clazz);
  }
}
