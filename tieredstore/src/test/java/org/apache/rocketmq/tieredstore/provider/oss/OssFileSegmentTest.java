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
    }

    @Test
    public void appendAndReadTest() throws ClientException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        ossConfig = new OssConfig(bucketName, endpoint, OssConstant.oss_access_key_id_value, OssConstant.oss_access_key_secret_value, 0, false);
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
        ossConfig = new OssConfig(bucketName, endpoint, OssConstant.oss_access_key_id_value, OssConstant.oss_access_key_secret_value, 0, false);
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
