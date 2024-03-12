package org.apache.rocketmq.tieredstore.provider.oss;

import com.aliyun.oss.OSS;
import com.aliyuncs.exceptions.ClientException;
import org.apache.rocketmq.tieredstore.provider.TieredStoreClientPool;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OssClientPool extends TieredStoreClientPool<OSS> {
    private final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);
    private boolean isShutdown = false;

    public OssClientPool(int instanceNum, String endpoint) {
        super(instanceNum, endpoint);
    }

    @Override
    protected OSS initSingleClient() {
        OSS client = null;
        try {
            client = OssUtil.buildOssClient(endpoint);
        } catch (ClientException ce) {
            logger.error("Get OSS client failed, Exception: ", ce);
        }
        return client;
    }

    @Override
    public void shutdown() {
        if (!isShutdown) {
            clientList.forEach(OSS::shutdown);
            isShutdown = true;
        }
    }

    public boolean isShutdown() {
        return isShutdown;
    }
}
