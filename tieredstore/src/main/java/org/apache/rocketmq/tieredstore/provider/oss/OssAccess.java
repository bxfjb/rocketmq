package org.apache.rocketmq.tieredstore.provider.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.AppendObjectRequest;
import com.aliyun.oss.model.AppendObjectResult;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.SimplifiedObjectMeta;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyuncs.exceptions.ClientException;
import org.apache.rocketmq.tieredstore.provider.ClientInstance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

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

    public int getSize(String bucketName, String objectName) throws InterruptedException,
            com.aliyun.oss.ClientException, com.aliyuncs.exceptions.ClientException {
        SimplifiedObjectMeta meta;
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            meta = clientInstance.getClient().getSimplifiedObjectMeta(bucketName, objectName);
            clientPool.returnClient(clientInstance.getIndex());
        } else {
            meta = client.getSimplifiedObjectMeta(bucketName, objectName);
        }

        return (int) meta.getSize();
    }

    public boolean isObjectExist(String bucketName, String objectName) throws InterruptedException,
            com.aliyun.oss.ClientException, com.aliyuncs.exceptions.ClientException {
        boolean exist = false;
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            exist = clientInstance.getClient().doesObjectExist(bucketName, objectName);
            clientPool.returnClient(clientInstance.getIndex());
        } else {
            exist = client.doesObjectExist(bucketName, objectName);
        }
        return exist;
    }

    public ByteBuffer getWholeObject(String bucketName, String objectName) throws IOException,
            InterruptedException, com.aliyun.oss.ClientException, com.aliyuncs.exceptions.ClientException {
        int size = getSize(bucketName, objectName);
        return getRangeObject(bucketName, objectName, 0, size);
    }

    public ByteBuffer getRangeObject(String bucketName, String objectName, long offset, int size) throws IOException,
            InterruptedException, com.aliyun.oss.ClientException, com.aliyuncs.exceptions.ClientException {
        OSSObject result;
        GetObjectRequest request = new GetObjectRequest(bucketName, objectName);
        request.setRange(offset, offset + size - 1);

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

    public AppendObjectResult appendObject(InputStream inputStream, String bucketName, String objectName) throws InterruptedException, com.aliyun.oss.ClientException, com.aliyuncs.exceptions.ClientException {
        if (!isObjectExist(bucketName, objectName)) {
            return appendObject(inputStream, bucketName, objectName, 0);
        }
        long offset = getSize(bucketName, objectName);
        return appendObject(inputStream, bucketName, objectName, offset);
    }

    public AppendObjectResult appendObject(InputStream inputStream, String bucketName, String objectName,
            long offset) throws InterruptedException, com.aliyun.oss.ClientException, com.aliyuncs.exceptions.ClientException {
        ObjectMetadata meta = new ObjectMetadata();
        // 指定上传的内容类型。
        meta.setContentType("text/plain");
        // 通过AppendObjectRequest设置多个参数。
        AppendObjectRequest request = new AppendObjectRequest(bucketName, objectName, inputStream ,meta);
        // 设置文件的追加位置。
        request.setPosition(offset);
        AppendObjectResult result;

        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            result = clientInstance.getClient().appendObject(request);
            clientPool.returnClient(clientInstance.getIndex());
        } else {
            result = client.appendObject(request);
        }

        return result;
    }

    public PutObjectResult putObject(InputStream inputStream, String bucketName,
            String objectName) throws InterruptedException, com.aliyun.oss.ClientException, com.aliyuncs.exceptions.ClientException {
        // 创建PutObjectRequest对象。
        PutObjectRequest request = new PutObjectRequest(bucketName, objectName, inputStream);
        PutObjectResult result;

        // 上传字符串。
        if (enableClientPool) {
            ClientInstance<OSS> clientInstance = clientPool.getClient();
            result = clientInstance.getClient().putObject(request);
            clientPool.returnClient(clientInstance.getIndex());
        } else {
            result = client.putObject(request);
        }

        return result;
    }

    public void deleteObject(String bucketName, String objectName)
        throws InterruptedException, com.aliyun.oss.ClientException, com.aliyuncs.exceptions.ClientException {
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
