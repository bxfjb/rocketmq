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
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public abstract class TieredStoreClientPool<T> {
    protected final ArrayList<T> clientList = new ArrayList<>();
    private final int instanceNum;
    private AtomicInteger nextIndex;
    protected String endpoint;

    public TieredStoreClientPool(int instanceNum, String endpoint) {
        this.instanceNum = instanceNum;
        this.endpoint = endpoint;
        for (int i = 0;i < instanceNum;++i) {
            clientList.add(initSingleClient());
        }
        this.nextIndex = new AtomicInteger(-1);
    }

    protected abstract T initSingleClient();

    public abstract void shutdown();

    public T getClient() throws ClientException, InterruptedException {
        return clientList.get(nextIndex.addAndGet(1) % instanceNum);
    }

}
