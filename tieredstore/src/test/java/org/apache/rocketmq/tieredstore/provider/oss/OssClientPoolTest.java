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

import com.aliyuncs.exceptions.ClientException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;

public class OssClientPoolTest {
    private final String bucketName = OssConstant.oss_xiaomi_cmp_test_bucket;
    private OssConfig ossConfig;
    @Before
    public void setUp() throws ClientException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        ossConfig = new OssConfig(
                OssConstant.oss_xiaomi_cmp_test_bucket,
                OssConstant.oss_endpoint_beijing,
                OssConstant.oss_access_key_id_value,
                OssConstant.oss_access_key_secret_value,
                1);
        OssUtil.init(ossConfig);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void miCmpTest() throws ClientException, InterruptedException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        OssAccess access = OssAccess.getInstance(ossConfig);

        access.shutdown();
    }

    @Test
    public void performanceTest() throws Exception {
        concurrentTest(1);
        concurrentTest(3);
        concurrentTest(5);
        concurrentTest(10);
        concurrentTest(20);
        concurrentTest(40);
    }

    public void concurrentTest(int poolSize) throws InterruptedException, ClientException {
        ossConfig.setClientPoolSize(poolSize);
        OssAccess access = OssAccess.getInstance(ossConfig);
        long begin, end;
        begin = System.currentTimeMillis();
        String data = poolSize + "!";
        concurrentTask(access, data, Integer.toString(poolSize));
        end = System.currentTimeMillis();
        System.out.println(poolSize + " clients time cost: " + (end - begin) + "ms");
        truncateAll(access, Integer.toString(poolSize));
        access.listObjects(bucketName).forEach(ossObjectSummary -> System.out.println(ossObjectSummary.getKey()));
        access.shutdown();
    }

    private void truncateAll(OssAccess access, String dir) throws ClientException, InterruptedException {
        for (int i = 0; i < 20; ++i) {
            access.deleteObject(bucketName, "rocketmq/consumeQueue/" + dir + "/" + i);
            access.deleteObject(bucketName, "rocketmq/commitLog/" + dir + "/" + i);
        }
    }

    private void concurrentTask(OssAccess access, String data, String dir) throws InterruptedException {
        Thread[] threads = new Thread[40];
        for (int i = 0; i < 20; ++i) {
            int finalI = i;
            Runnable cqTask = () -> {
                try {
                    access.appendObject(new ByteArrayInputStream(data.getBytes()), bucketName, "rocketmq/consumeQueue/" + dir + "/" + finalI);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };
            Runnable commitLogTask = () -> {
                try {
                    access.appendObject(new ByteArrayInputStream(data.getBytes()), bucketName, "rocketmq/commitLog/" + dir + "/" + finalI);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };
            threads[i] = new Thread(cqTask);
            threads[i + 20] = new Thread(commitLogTask);
        }
        for (int i = 0; i < 40; ++i) {
            threads[i].start();
        }
        for (int i = 0; i < 40; ++i) {
            threads[i].join();
        }
    }
}
