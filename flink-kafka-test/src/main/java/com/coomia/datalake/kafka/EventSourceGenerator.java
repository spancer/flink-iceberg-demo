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
  private int dpv = 100000000;
  private int maxUid;

  /**
   * 
   */
  public EventSourceGenerator(int dpv, int maxUid) {
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
