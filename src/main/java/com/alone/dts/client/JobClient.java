package com.alone.dts.client;

import com.alibaba.fastjson.JSON;
import com.alone.dts.client.failstore.AbstractFailStore;
import com.alone.dts.client.failstore.FailStore;
import com.alone.dts.thrift.service.JobService;
import com.alone.dts.thrift.struct.JobStruct;
import com.gary.trc.annotation.ThriftReference;
import com.gary.trc.util.NetworkUtil;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;

import java.lang.management.ManagementFactory;
import java.util.*;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 17-9-5 下午2:35
 */
@Slf4j
public class JobClient implements InitializingBean {
    @ThriftReference
    private JobService.Iface jobService;

    @Setter
    private AbstractFailStore failStore;
    @Setter
    private long failInterval = 1000;
    @Setter
    private String nodeGroup = "all";
    @Setter
    private String host = NetworkUtil.getLocalHost();

    private Timer failTimer;

    public void addJob(JobStruct job) {
        parseJob(job);
        try {
            jobService.add(job);
        } catch (Throwable e) {
            if (failStore != null) {
                failStore.put(job.getTaskId(), job);
                log.error("job {} add fail, save local store", job.getTaskId(), e);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public void addJob(Collection<JobStruct> jobs) {
        if (jobs.size() == 1) {
            addJob(jobs.iterator().next());
            return;
        }
        StringBuilder ids = new StringBuilder();
        for (JobStruct job : jobs) {
            parseJob(job);
            ids.append(job.getTaskId());
        }
        try {
            jobService.addList(new ArrayList<>(jobs));
        } catch (Throwable e) {
            if (failStore != null) {
                failStore.put(DigestUtils.md5Hex(ids.toString()), jobs);
                log.error("jobs add fail, save local store", e);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (failStore != null) {
            failStore.setGroup(nodeGroup);
            failStore.init();
            failTimer = new Timer("FAIL_STORE_TIMER");
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    log.info("fail store timer shutdown...");
                    failTimer.cancel();
                    failStore.shutdown();
                }
            }));
            failTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        Map<String, String> map = failStore.datas();
                        for (Map.Entry<String, String> entry : map.entrySet()) {
                            log.info("delete fail store job " + entry.getKey());
                            failStore.del(entry.getKey());
                            String value = entry.getValue();
                            if (value.startsWith("[") && value.endsWith("]")) {
                                addJob(JSON.parseArray(value, JobStruct.class));
                                continue;
                            }
                            addJob(JSON.parseObject(value, JobStruct.class));
                        }
                    } catch (Throwable e) {
                        log.error("fail store retry error", e);
                    }
                }
            }, 1000, failInterval);
        }
    }

    private void parseJob(JobStruct job) {
        if (StringUtils.isEmpty(job.getNodeGroup())) job.setNodeGroup(nodeGroup);
        if (StringUtils.isEmpty(job.getSubmitHost())) job.setSubmitHost(host);
        if (!job.isSetSubmitPid()) {
            String name = ManagementFactory.getRuntimeMXBean().getName();
            job.setSubmitPid(Integer.parseInt(name.split("@")[0]));
        }
    }
}
