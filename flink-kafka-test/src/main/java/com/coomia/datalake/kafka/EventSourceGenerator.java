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
package com.coomia.datalake.kafka;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import com.alibaba.fastjson.JSON;

/**
 * using rich source function to generate event data.
 * 
 * @author spancer
 * @date 2019/03/19
 * 
 */
public class EventSourceGenerator extends RichParallelSourceFunction<String> {

  private static final long serialVersionUID = -3345711794203267205L;
  private long dpv = 1-000-000-000;
  private int maxUid;

  /**
   * 
   */
  public EventSourceGenerator(long dpv, int maxUid) {
    this.dpv = dpv;
    this.maxUid = maxUid;
  }

  @Override
  public void run(SourceContext<String> ctx) throws Exception {
    while (dpv > 0) {
      ctx.collect(JSON.toJSONString(RandomEventDataUtil.randomRecord(maxUid)));
      dpv--;
    }

  }

  @Override
  public void cancel() {
    dpv = 0;

  }

}
