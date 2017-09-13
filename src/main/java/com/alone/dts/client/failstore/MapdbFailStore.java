package com.alone.dts.client.failstore;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.springframework.util.Assert;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapdbFailStore extends AbstractFailStore {
    private DB db;
    private BTreeMap<String, String> map;
    @Override
    public String getName() {
        return "mapdb";
    }

    /**
     * 获取所有数据 key,value键值对
     *
     * @return
     */
    @Override
    public Map<String, String> datas() {
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : map.getEntries()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * 存放数据
     *
     * @param key   键
     * @param value 值
     */
    @Override
    public void put(String key, Object value) {
        Assert.notNull(value);
        Assert.notNull(key);
        map.put(key, JSON.toJSONString(value, SerializerFeature.WriteDateUseDateFormat));
        db.commit();
    }

    /**
     * 删除数据
     *
     * @param key 键
     */
    @Override
    public void del(String key) {
        map.remove(key);
        db.commit();
    }

    /**
     * 结束
     */
    @Override
    public void shutdown() {
        db.close();
    }

    @Override
    public void init() throws Exception {
        super.init();
        db = DBMaker.fileDB(new File(this.dir, "dts.db"))
                .closeOnJvmShutdown()
                .make();
        map = db.treeMap("dts").keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).createOrOpen();
        db.commit();
    }
}
