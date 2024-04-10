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

package org.apache.rocketmq.tieredstore.provider;

import com.aliyuncs.exceptions.ClientException;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TieredStoreClientPool<T> {
    private final static int POSITIVE_MASK = 0x7FFFFFFF;
    protected final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);
    protected final ArrayList<T> clientList;
    private final AtomicInteger nextIndex;
    private int instanceNum;
    protected String endpoint;

    public TieredStoreClientPool(int instanceNum, String endpoint) {
        this.clientList = new ArrayList<>();
        this.instanceNum = instanceNum;
        this.endpoint = endpoint;
        for (int i = 0;i < instanceNum;++i) {
            T client = initSingleClient();
            if (client != null) {
                clientList.add(client);
            }
        }
        this.instanceNum = clientList.size();
        this.nextIndex = new AtomicInteger(-1);
    }

    protected abstract T initSingleClient();

    public abstract void shutdown();

    public T getClient() throws ClientException, InterruptedException {
        int current;
        int next;
        T result;
        do {
            current = nextIndex.get();
            next = (current + 1) & POSITIVE_MASK;
            result = clientList.get(next % instanceNum);
        } while (!nextIndex.compareAndSet(current, next));
        return result;
    }

}
