package org.apache.rocketmq.tieredstore.provider;

import com.aliyuncs.exceptions.ClientException;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

public abstract class TieredStoreClientPool<T> {
    protected final ArrayList<T> clientList = new ArrayList<>();
    private final int instanceNum;
    private final ReentrantLock lock;
    private final Semaphore semaphore;
    private long statusBitmap; // 0: idle, 1: busy
    protected String endpoint;

    public TieredStoreClientPool(int instanceNum, String endpoint) {
        this.instanceNum = instanceNum;
        this.endpoint = endpoint;
        for (int i = 0;i < instanceNum;++i) {
            clientList.add(initSingleClient());
        }
        this.lock = new ReentrantLock();
        this.semaphore = new Semaphore(instanceNum);
    }

    protected abstract T initSingleClient();

    public abstract void shutdown();

    public ClientInstance<T> getClient() throws ClientException, InterruptedException {
        T client;
        int index;

        semaphore.acquire();
        lock.lock();
        index = Long.numberOfTrailingZeros(~statusBitmap);
        statusBitmap |= (1L << index);
        lock.unlock();

        client = clientList.get(index);
        if (client == null) {
            throw new TieredStoreException(TieredStoreErrorCode.STORAGE_PROVIDER_ERROR, "Get OssClient Failed");
        }
        return new ClientInstance<>(client, index);
    }

    public void returnClient(int index) {
        if (index >= 0 && index < instanceNum && ((statusBitmap >> index) & 1L) == 1L) {
            lock.lock();
            statusBitmap &= ~(1L << index);
            lock.unlock();
            semaphore.release();
        }
    }
}
