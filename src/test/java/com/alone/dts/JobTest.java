package com.alone.dts;

import com.alone.dts.client.JobClient;
import com.alone.dts.thrift.struct.JobStruct;
import com.alone.dts.thrift.struct.PublicStructConstants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.concurrent.locks.LockSupport;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:spring.xml"})
public class JobTest {

    @Resource
    private JobClient jobClient;


    @Test
    public void add() {
        JobStruct job = new JobStruct();
        job.setTaskId("task-java-1");
        job.setAction("say");
        job.setType(PublicStructConstants.REAL_TIME);
        jobClient.addJob(job);
        LockSupport.park();
    }
}
