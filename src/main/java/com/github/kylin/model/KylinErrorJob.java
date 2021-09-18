package com.github.kylin.model;

import lombok.Data;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-15 16:23
 * @description kylin error job.
 */
@Data
public class KylinErrorJob {
    private String jobID;
    private String jobName;
    private String lastModifiedTime;
    private String jobCubeName;
    private String execStartTime;
    private String execEndTime;
}
