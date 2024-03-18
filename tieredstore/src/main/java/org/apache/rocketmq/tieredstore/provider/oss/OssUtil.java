package org.apache.rocketmq.tieredstore.provider.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider;
import com.aliyuncs.exceptions.ClientException;

import java.lang.reflect.Field;
import java.util.Map;

public class OssUtil {
    private static final OSSClientBuilder builder = new OSSClientBuilder();
    private static boolean init = false;
    private static EnvironmentVariableCredentialsProvider credentialsProvider;

    public static void init(OssConfig config) throws NoSuchFieldException, IllegalAccessException, ClientException {
        if (!init) {
            // 从环境变量中获取访问凭证。运行本代码示例之前，请确保已设置环境变量OSS_ACCESS_KEY_ID和OSS_ACCESS_KEY_SECRET。
            OssUtil.setEnv(OssConstant.oss_access_key_id_name, config.getKeyId());
            OssUtil.setEnv(OssConstant.oss_access_key_secret_name, config.getKeySecret());
            init = true;
        }
        credentialsProvider = CredentialsProviderFactory.newEnvironmentVariableCredentialsProvider();
    }

    public static OSS buildOssClient(String endpoint) throws ClientException {
        // 创建OSSClient实例。
        return builder.build(endpoint, credentialsProvider);
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
