/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.tieredstore.provider.oss;

import com.aliyun.oss.OSS;
import com.aliyuncs.exceptions.ClientException;
import org.apache.rocketmq.tieredstore.provider.TieredStoreClientPool;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OssClientPool extends TieredStoreClientPool<OSS> {
    private final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);
    private boolean isShutdown = false;

    public OssClientPool(int instanceNum, String endpoint) {
        super(instanceNum, endpoint);
    }

    @Override
    protected OSS initSingleClient() {
        OSS client = null;
        try {
            client = OssUtil.buildOssClient(endpoint);
        } catch (ClientException ce) {
            logger.error("Get OSS client failed, Exception: ", ce);
        }
        return client;
    }

    @Override
    public void shutdown() {
        if (!isShutdown) {
            clientList.forEach(OSS::shutdown);
            isShutdown = true;
        }
    }

    public boolean isShutdown() {
        return isShutdown;
    }
}
