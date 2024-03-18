package org.apache.rocketmq.tieredstore.provider.oss;

import java.security.InvalidParameterException;

public class OssConfig {
    private final String bucketName;
    private final String endpoint;
    private final String keyId;
    private final String keySecret;
    private int clientPoolSize;

    public OssConfig(String bucketName, String endpoint, String keyId, String keySecret, int clientPoolSize) {
        if (clientPoolSize < 1) {
            throw new InvalidParameterException(String.format("[OssConfig] Invalid clientPoolSize parameter[%d]", clientPoolSize));
        }
        this.bucketName = bucketName;
        this.endpoint = endpoint;
        this.keyId = keyId;
        this.keySecret = keySecret;
        this.clientPoolSize = clientPoolSize;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getKeyId() {
        return keyId;
    }

    public String getKeySecret() {
        return keySecret;
    }

    public int getClientPoolSize() {
        return clientPoolSize;
    }

    public void setClientPoolSize(int clientPoolSize) {
        this.clientPoolSize = clientPoolSize;
    }
}
