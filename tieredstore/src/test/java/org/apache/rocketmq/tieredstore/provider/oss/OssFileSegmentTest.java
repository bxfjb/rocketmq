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
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class OssFileSegmentTest {
    private final String bucketName = OssConstant.oss_xiaomi_cmp_test_bucket;
    private final String endpoint = OssConstant.oss_endpoint_beijing;
    private TieredMessageStoreConfig storeConfig;
    private OssConfig ossConfig;
    private MessageQueue mq;
    private OssFileSegment ossFileSegment;

    @Before
    public void setUp() throws ClientException, NoSuchFieldException, IllegalAccessException {
        storeConfig = new TieredMessageStoreConfig();
        mq = new MessageQueue("OSSFileSegmentTestTopic1", "broker", 0);
        TieredStoreExecutor.init();
        ossConfig = new OssConfig(bucketName, endpoint, OssConstant.oss_access_key_id_value, OssConstant.oss_access_key_secret_value, 1);
        OssUtil.init(ossConfig);
    }

    @Test
    public void appendAndReadTest() throws ClientException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        ossFileSegment = new OssFileSegment(
                storeConfig, ossConfig, FileSegmentType.CONSUME_QUEUE, TieredStoreUtil.toPath(mq), 0);
        byte[] source = new byte[4096];
        new Random().nextBytes(source);
        ByteBuffer buffer = ByteBuffer.wrap(source);
        ossFileSegment.append(buffer, 0);
        ossFileSegment.commit();
        Assert.assertTrue(ossFileSegment.exists());

        ByteBuffer result = ossFileSegment.read(0, 4096);
        Assert.assertArrayEquals(source, result.array());

        ossFileSegment.append(buffer, 0);
        ossFileSegment.commit();

        result = ossFileSegment.read(4096, 4096);
        Assert.assertArrayEquals(source, result.array());

        ossFileSegment.destroyFile();
    }

    @Test
    public void putAndReadTest() throws ClientException, NoSuchFieldException, IllegalAccessException {
        ossFileSegment = new OssFileSegment(
                storeConfig, ossConfig, FileSegmentType.INDEX, TieredStoreUtil.toPath(mq), 0);
        byte[] source = new byte[4096];
        new Random().nextBytes(source);
        ByteBuffer buffer = ByteBuffer.wrap(source);
        ossFileSegment.append(buffer, 0);
        ossFileSegment.commit();
        Assert.assertTrue(ossFileSegment.exists());

        ByteBuffer result = ossFileSegment.read(0, 4096);
        Assert.assertArrayEquals(source, result.array());

        ossFileSegment.destroyFile();
    }

    @After
    public void tearDown() throws IOException {
        TieredStoreTestUtil.destroyCompositeFlatFileManager();
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreExecutor.shutdown();
    }
}
