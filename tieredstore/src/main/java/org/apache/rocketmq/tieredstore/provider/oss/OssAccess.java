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
    private static OssAccess access;
    private final OssClientPool clientPool;

    public static OssAccess getInstance(OssConfig ossConfig) throws ClientException {
        if (access == null) {
            access = new OssAccess(ossConfig.getClientPoolSize(), ossConfig.getEndpoint());
        }
        return access;
    }

    private OssAccess(int clientPoolSize, String endpoint) throws ClientException {
        clientPool = new OssClientPool(clientPoolSize, endpoint);
    }

    private OSS getClient() throws ClientException, InterruptedException {
        return clientPool.getClient();
    }

    public void createBucket(String bucketName) throws ClientException, InterruptedException {
        getClient().createBucket(bucketName);
    }

    public boolean isBucketExist(String bucketName) throws ClientException, InterruptedException {
        return getClient().doesBucketExist(bucketName);
    }

    public List<Bucket> listAllBuckets() throws ClientException, InterruptedException {
        return getClient().listBuckets();
    }

    public List<OSSObjectSummary> listObjects(String bucketName) throws ClientException, InterruptedException {
        ObjectListing objectListing = getClient().listObjects(bucketName);
        if (objectListing != null) {
            return objectListing.getObjectSummaries();
        }
        return new ArrayList<>();
    }

    public Integer getSize(String bucketName, String objectName) throws InterruptedException, ClientException {
        SimplifiedObjectMeta meta = getClient().getSimplifiedObjectMeta(bucketName, objectName);
        if (meta != null) {
            return Math.toIntExact(meta.getSize());
        }
        return -1;
    }

    public boolean isObjectExist(String bucketName, String objectName)
            throws InterruptedException, ClientException {
        return getClient().doesObjectExist(bucketName, objectName);
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

        OSSObject result = getClient().getObject(request);

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
        OSS client = getClient();
        return client.appendObject(request);
    }

    public PutObjectResult putObject(InputStream inputStream, String bucketName,
            String objectName) throws InterruptedException, ClientException{
        // 创建PutObjectRequest对象。
        PutObjectRequest request = new PutObjectRequest(bucketName, objectName, inputStream);
        return getClient().putObject(request);
    }

    public void deleteObject(String bucketName, String objectName)
            throws InterruptedException, ClientException {
        getClient().deleteObject(bucketName, objectName);
    }

    public void shutdown() {
        clientPool.shutdown();
        access = null;
    }
}
