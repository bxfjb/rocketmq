package org.apache.rocketmq.tieredstore.provider;

import com.aliyuncs.exceptions.ClientException;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public abstract class TieredStoreClientPool<T> {
    protected final ArrayList<T> clientList = new ArrayList<>();
    private final int instanceNum;
    private AtomicInteger nextIndex;
    protected String endpoint;

    public TieredStoreClientPool(int instanceNum, String endpoint) {
        this.instanceNum = instanceNum;
        this.endpoint = endpoint;
        for (int i = 0;i < instanceNum;++i) {
            clientList.add(initSingleClient());
        }
        this.nextIndex = new AtomicInteger(-1);
    }

    protected abstract T initSingleClient();

    public abstract void shutdown();

    public T getClient() throws ClientException, InterruptedException {
        return clientList.get(nextIndex.addAndGet(1) % instanceNum);
    }

}
