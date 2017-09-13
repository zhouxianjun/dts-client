package com.alone.dts.client.failstore;

import java.util.Map;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 17-9-5 下午4:42
 */
public interface FailStore {
    /**
     * 获取所有数据 key,value键值对
     * @return
     */
    Map<String, String> datas();

    /**
     * 存放数据
     * @param key 键
     * @param value 值
     */
    void put(String key, Object value);

    /**
     * 删除数据
     * @param key 键
     */
    void del(String key);

    /**
     * 初始化
     */
    void init() throws Exception;

    /**
     * 结束
     */
    void shutdown();
}
