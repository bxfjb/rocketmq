package org.apache.rocketmq.tieredstore.provider.oss;

import com.aliyuncs.exceptions.ClientException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;

public class OssClientPoolTest {
    private final String bucketName = OssConstant.oss_xiaomi_cmp_test_bucket;
    @Before
    public void setUp() throws ClientException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        OssUtil.init(OssConstant.oss_access_key_id_value, OssConstant.oss_access_key_secret_value);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void miCmpTest() throws ClientException, InterruptedException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        OssAccess access = new OssAccess(false, 1, OssConstant.oss_endpoint_beijing);
        String bucketName = OssConstant.oss_xiaomi_cmp_test_bucket;


        access.shutdown();
    }

    @Test
    public void performanceTest() throws Exception {
        concurrentTest(false, 0);
        concurrentTest(true, 3);
        concurrentTest(true, 5);
        concurrentTest(true, 10);
        concurrentTest(true, 20);
        concurrentTest(true, 40);
    }

    public void concurrentTest(boolean enablePool, int poolSize) throws InterruptedException, ClientException {
        OssAccess access = new OssAccess(enablePool, poolSize, OssConstant.oss_endpoint_beijing);
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
