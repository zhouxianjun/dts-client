package com.alone.dts.client.failstore;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 17-9-5 下午4:52
 */
public class LevelFailStore extends AbstractFailStore {
    private DB db;
    private Options options;
    /**
     * 获取所有数据 key,value键值对
     *
     * @return
     */
    @Override
    public Map<String, String> datas() {
        ReadOptions ro = new ReadOptions();
        ro.snapshot(db.getSnapshot());
        DBIterator iterator = null;
        Map<String, String> map = new LinkedHashMap<>();
        try {
            iterator = db.iterator(ro);
            for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                String key = asString(iterator.peekNext().getKey());
                String value = asString(iterator.peekNext().getValue());
                map.put(key, value);
            }
        } finally {
            if (iterator != null) {
                try {
                    iterator.close();
                } catch (IOException ignored) {
                }
            }
            try {
                ro.snapshot().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return map;
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
        db.put(bytes(key), bytes(JSON.toJSONString(value, SerializerFeature.WriteDateUseDateFormat)));
    }

    /**
     * 删除数据
     *
     * @param key 键
     */
    @Override
    public void del(String key) {
        Assert.notNull(key);
        db.delete(bytes(key));
    }

    /**
     * 结束
     */
    @Override
    public void shutdown() {
        try {
            db.close();
            JniDBFactory.factory.destroy(this.dir, options);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init() throws Exception {
        super.init();
        options = new Options();
        options.createIfMissing(true);
        options.cacheSize(100 * 1024 * 1024);   // 100M
        options.maxOpenFiles(500);

        JniDBFactory.factory.repair(this.dir, options);
        db = JniDBFactory.factory.open(this.dir, options);
    }

    @Override
    public String getName() {
        return "leveldb";
    }
}
