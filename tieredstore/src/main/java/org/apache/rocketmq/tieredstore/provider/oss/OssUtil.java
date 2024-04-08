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

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider;
import com.aliyuncs.exceptions.ClientException;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

import java.lang.reflect.Field;
import java.util.Map;

public class OssUtil {
    private static final OSSClientBuilder BUILDER = new OSSClientBuilder();
    private static volatile boolean init = false;
    private static EnvironmentVariableCredentialsProvider credentialsProvider;

    public static void init(TieredMessageStoreConfig config) throws NoSuchFieldException, IllegalAccessException, ClientException {
        if (!init) {
            synchronized (OssUtil.class) { // get ak/sk from environmental parameters, make sure user config them before run the program
                if (!init) {
                    OssUtil.setEnv(TieredStoreUtil.OSS_ACCESS_KEY_ID_NAME, config.getObjectStoreAccessKey());
                    OssUtil.setEnv(TieredStoreUtil.OSS_ACCESS_KEY_SECRET_NAME, config.getObjectStoreSecretKey());
                    credentialsProvider = CredentialsProviderFactory.newEnvironmentVariableCredentialsProvider();
                    init = true;
                }
            }
        }
    }

    public static OSS buildOssClient(String endpoint) throws ClientException {
        return BUILDER.build(endpoint, credentialsProvider);
    }

    private static void setEnv(String key, String value) throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> env = System.getenv();
        Class<?> clazz = env.getClass();
        Field field = clazz.getDeclaredField("m");
        field.setAccessible(true);
        Map<String, String> writableEnv = (Map<String, String>) field.get(env);
        writableEnv.put(key, value);
    }
}
