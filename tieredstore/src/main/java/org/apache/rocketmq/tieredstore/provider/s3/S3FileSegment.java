package org.apache.rocketmq.tieredstore.provider.s3;

import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.provider.stream.FileSegmentInputStream;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class S3FileSegment extends TieredFileSegment {
    public S3FileSegment(TieredMessageStoreConfig storeConfig, FileSegmentType fileType, String filePath, long baseOffset) {
        super(storeConfig, fileType, filePath, baseOffset);
    }

    @Override
    public String getPath() {
        return null;
    }

    @Override
    public long getSize() {
        return 0;
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public void createFile() {

    }

    @Override
    public void destroyFile() {

    }

    @Override
    public CompletableFuture<ByteBuffer> read0(long position, int length) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> commit0(FileSegmentInputStream inputStream, long position, int length, boolean append) {
        return null;
    }
}
