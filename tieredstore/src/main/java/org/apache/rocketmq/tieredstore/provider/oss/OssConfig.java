package org.apache.rocketmq.tieredstore.provider.oss;

public class OssConfig {
    private final String bucketName;
    private final String endpoint;
    private final String keyId;
    private final String keySecret;
    private final int clientPoolSize;
    private final boolean enableClientPool;

    public OssConfig(String bucketName, String endpoint, String keyId, String keySecret, int clientPoolSize, boolean enableClientPool) {
        this.bucketName = bucketName;
        this.endpoint = endpoint;
        this.keyId = keyId;
        this.keySecret = keySecret;
        this.clientPoolSize = clientPoolSize;
        this.enableClientPool = enableClientPool;
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

    public boolean isEnableClientPool() {
        return enableClientPool;
    }
}
