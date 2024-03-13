package org.apache.rocketmq.tieredstore.provider.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.*;
import com.aliyuncs.exceptions.ClientException;
import org.apache.rocketmq.tieredstore.provider.ClientInstance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class OssAccess {
    private final boolean enableClientPool;
    private OssClientPool clientPool;
    private OSS client;

    public OssAccess(boolean enableClientPool, int clientPoolSize, String endpoint) throws ClientException {
        this.enableClientPool = enableClientPool;
        if (enableClientPool) {
            clientPool = new OssClientPool(clientPoolSize, endpoint);
        } else {
            client = OssUtil.buildOssClient(endpoint);
        }
    }

    private Object internalCall(Method method, Object... args) throws ClientException, InterruptedException, InvocationTargetException, IllegalAccessException {
        Object result;
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            result = method.invoke(clientInstance.getClient(), args);
            clientPool.returnClient(clientInstance.getIndex());
        } else {
            result = method.invoke(client, args);
        }
        return result;
    }

    public void createBucket(String bucketName) throws ClientException, InterruptedException {
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            clientInstance.getClient().createBucket(bucketName);
            clientPool.returnClient(clientInstance.getIndex());
        } else {
            client.createBucket(bucketName);
        }
    }

    public boolean isBucketExist(String bucketName) throws ClientException, InterruptedException {
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            boolean exist = clientInstance.getClient().doesBucketExist(bucketName);
            clientPool.returnClient(clientInstance.getIndex());
            return exist;
        } else {
            return client.doesBucketExist(bucketName);
        }
    }

    public List<Bucket> listAllBuckets() throws ClientException, InterruptedException {
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            List<Bucket> bucketList = clientInstance.getClient().listBuckets();
            clientPool.returnClient(clientInstance.getIndex());
            return bucketList;
        } else {
            return client.listBuckets();
        }
    }

    public List<OSSObjectSummary> listObjects(String bucketName) throws ClientException, InterruptedException {
        ObjectListing objectListing;
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            objectListing = clientInstance.getClient().listObjects(bucketName);
            clientPool.returnClient(clientInstance.getIndex());
        } else {
            objectListing = client.listObjects(bucketName);
        }
        if (objectListing != null) {
            return objectListing.getObjectSummaries();
        }
        return new ArrayList<>();
    }

    public Integer getSize(String bucketName, String objectName) throws InterruptedException, ClientException {
        SimplifiedObjectMeta meta;
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            meta = clientInstance.getClient().getSimplifiedObjectMeta(bucketName, objectName);
            clientPool.returnClient(clientInstance.getIndex());
        } else {
            meta = client.getSimplifiedObjectMeta(bucketName, objectName);
        }
        if (meta != null) {
            return Math.toIntExact(meta.getSize());
        }
        return -1;
    }

    public boolean isObjectExist(String bucketName, String objectName)
            throws InterruptedException, ClientException {
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            boolean exist = clientInstance.getClient().doesObjectExist(bucketName, objectName);
            clientPool.returnClient(clientInstance.getIndex());
            return exist;
        } else {
            return client.doesObjectExist(bucketName, objectName);
        }
    }

    public ByteBuffer getWholeObject(String bucketName, String objectName) throws IOException,
            InterruptedException, ClientException {
        int size = getSize(bucketName, objectName);
        return getRangeObject(bucketName, objectName, 0, size);
    }

    public ByteBuffer getRangeObject(String bucketName, String objectName, long offset, int size) throws IOException,
            InterruptedException, ClientException {
        GetObjectRequest request = new GetObjectRequest(bucketName, objectName);
        request.setRange(offset, offset + size - 1);

        OSSObject result;
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            result = clientInstance.getClient().getObject(request);
            clientPool.returnClient(clientInstance.getIndex());
        } else {
            result = client.getObject(request);
        }

        byte[] buf = new byte[size];
        int off = 0;
        InputStream in = result.getObjectContent();
        for (int n = 0; n != -1; ) {
            off += n;
            n = in.read(buf, off, buf.length);
        }
        in.close();
        result.close();
        return ByteBuffer.wrap(buf);
    }

    public AppendObjectResult appendObject(InputStream inputStream, String bucketName, String objectName)
            throws InterruptedException, ClientException{
        if (!isObjectExist(bucketName, objectName)) {
            return appendObject(inputStream, bucketName, objectName, 0);
        }
        long offset = getSize(bucketName, objectName);
        return appendObject(inputStream, bucketName, objectName, offset);
    }

    public AppendObjectResult appendObject(InputStream inputStream, String bucketName, String objectName,
            long offset) throws InterruptedException, ClientException {
        ObjectMetadata meta = new ObjectMetadata();
        // 指定上传的内容类型。
        meta.setContentType("text/plain");
        // 通过AppendObjectRequest设置多个参数。
        AppendObjectRequest request = new AppendObjectRequest(bucketName, objectName, inputStream ,meta);
        // 设置文件的追加位置。
        request.setPosition(offset);

        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            AppendObjectResult result = clientInstance.getClient().appendObject(request);
            clientPool.returnClient(clientInstance.getIndex());
            return result;
        } else {
            return client.appendObject(request);
        }
    }

    public PutObjectResult putObject(InputStream inputStream, String bucketName,
            String objectName) throws InterruptedException, ClientException{
        // 创建PutObjectRequest对象。
        PutObjectRequest request = new PutObjectRequest(bucketName, objectName, inputStream);
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            PutObjectResult result = clientInstance.getClient().putObject(request);
            clientPool.returnClient(clientInstance.getIndex());
            return result;
        } else {
            return client.putObject(request);
        }
    }

    public void deleteObject(String bucketName, String objectName)
            throws InterruptedException, ClientException {
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            clientInstance.getClient().deleteObject(bucketName, objectName);
            clientPool.returnClient(clientInstance.getIndex());
        } else {
            client.deleteObject(bucketName, objectName);
        }
    }

    public void shutdown() {
        if (enableClientPool) {
            clientPool.shutdown();
        } else {
            client.shutdown();
        }
    }
}
