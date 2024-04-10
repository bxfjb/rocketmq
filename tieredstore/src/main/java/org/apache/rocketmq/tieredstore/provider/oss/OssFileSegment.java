/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.tieredstore.provider.oss;

import com.aliyuncs.exceptions.ClientException;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.aliyun.oss.model.AppendObjectResult;
import com.aliyun.oss.model.PutObjectResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.provider.stream.FileSegmentInputStream;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

public class OssFileSegment extends TieredFileSegment {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);
    private final String bucketName;
    private final String objectName;
    private final OssAccess access;
    private int size;

    /**
     * filePath: broker/topic/queueId
     * bucketName: rocketmq-tiered-test
     * objectName: cluster/broker/topic/queueId/fileType/baseOffset
     * */
    public OssFileSegment(TieredMessageStoreConfig storeConfig,
        FileSegmentType fileType,
        String filePath,
        long baseOffset) throws ClientException, NoSuchFieldException, IllegalAccessException {
        super(storeConfig, fileType, filePath, baseOffset);
        this.objectName = Paths.get(storeConfig.getTieredStoreFilePath(), storeConfig.getBrokerClusterName(),
                filePath, fileType.toString(), TieredStoreUtil.offset2FileName(baseOffset)).toString();
        this.bucketName = storeConfig.getObjectStoreBucket();
        this.access = OssAccess.getInstance(storeConfig);
        OssUtil.init(storeConfig);
    }

    @Override
    public String getPath() {
        return objectName;
    }

    @Override
    public long getSize() {
        try {
            if (size == -1 && exists()) {
                size = access.getSize(bucketName, objectName);
            }
        } catch (Exception e) {
            logger.error("OssFileSegment#getSize Exception: ", e);
        }
        return size;
    }

    @Override
    public boolean exists() {
        boolean exist = false;
        try {
            exist = access.isObjectExist(bucketName, objectName);
        } catch (Exception e) {
            logger.error("OssFileSegment#exists Exception: ", e);
        }
        return exist;
    }

    @Override
    public void createFile() {
        try {
            if (fileType != FileSegmentType.INDEX) {
                access.appendObject(new ByteArrayInputStream(new byte[0]), bucketName, objectName, 0);
            } else {
                access.putObject(new ByteArrayInputStream(new byte[0]), bucketName, objectName);
            }
            size = 0;
        } catch (Exception e) {
            logger.error("OssFileSegment#createFile Exception: ", e);
        }
    }

    @Override
    public void destroyFile() {
        try {
            access.deleteObject(bucketName, objectName);
            size = -1;
        } catch (Exception e) {
            logger.error("OssFileSegment#destroyFile Exception: ", e);
        }
    }

    @Override
    public CompletableFuture<ByteBuffer> read0(long position, int length) {
        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        try {
            future.complete(access.getRangeObject(bucketName, objectName, position, length));
        } catch (Exception e) {
            logger.error("OssFileSegment#read0 Exception: ", e);
        }
        return future;
    }

    @Override
    public CompletableFuture<Boolean> commit0(FileSegmentInputStream inputStream, long position, int length, boolean append) {
        if (inputStream.getContentLength() != length) {
            logger.error("OssFileSegment commit0 failed for incorrect variable length :{}, actual length:{}", length, inputStream.getContentLength());
        }
        return CompletableFuture.supplyAsync(() ->{
            try {
                if (append) {
                    AppendObjectResult result = access.appendObject(inputStream, bucketName, objectName, position);
                    if (result.getNextPosition() != size + length) {
                        logger.error("OssFileSegment commit0 error: commit data position[{}] is not equal with size[{}]+length[{}]", result.getNextPosition(), size, length);
                    }
                    size += length;
                } else {
                    PutObjectResult result = access.putObject(inputStream, bucketName, objectName);
                    size = length;
                }
                return true;
            } catch (Exception e) {
                logger.error("OssFileSegment#commit0 Exception: ", e);
            }
            return false;
        }, TieredStoreExecutor.commitExecutor);
    }

    @VisibleForTesting
    public void shutdown() {
        access.shutdown();
    }
}
