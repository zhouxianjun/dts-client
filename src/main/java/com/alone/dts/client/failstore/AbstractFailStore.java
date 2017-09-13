package com.alone.dts.client.failstore;

import lombok.Setter;
import org.springframework.util.Assert;

import java.io.File;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 17-9-5 下午5:07
 */
public abstract class AbstractFailStore implements FailStore {
    protected File dir;
    @Setter
    private String root = System.getProperty("user.home");
    @Setter
    private String group;

    @Override
    public void init() throws Exception {
        String path = String.format("%s/.dts/CLIENT/%s/failstore/%s", root, group, getName());
        dir = new File(path);
        if (!dir.exists()) {
            Assert.isTrue(dir.mkdirs(), "create failstore path " + path);
        }
    }

    public abstract String getName();
}
