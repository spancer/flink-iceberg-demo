/*
 *   Copyright (c) 2015-2021 Coomia Network Technology Co., Ltd. All Rights Reserved.
 *  
 *   <p>
 *   This software is licensed not sold. Use or reproduction of this software by any unauthorized individual or entity is strictly prohibited. This software is the confidential and proprietary information of Coomia Network Technology Co., Ltd. Disclosure of such confidential information  and shall use it only in accordance with the terms of the license agreement you entered into with Coomia Network Technology Co., Ltd.
 *  
 *   <p>
 *   Coomia Network Technology Co., Ltd. MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. Coomia Network Technology Co., Ltd. SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF
 *   USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ANY DERIVATIVES THEREOF.
 */

package com.coomia.datalake.es;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Serializable;

public class ElasticsearchSinkFunctionWithConf implements ElasticsearchSinkFunction<String>, Serializable {

    private static final long serialVersionUID = 1L;
    private String index;
    private String type;

    public ElasticsearchSinkFunctionWithConf(String index, String type) {
        this.index = index;
        this.type = type;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(Requests.indexRequest().index(index).type(type).source(element, XContentType.JSON));
    }
}