package com.github.kylin.model;

import lombok.Data;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-15 15:09
 * @description kylin segment.
 */
@Data
public class KylinSegment {
    // 段id
    private String uuid;

    // 段名称
    private String name;

    // 存储表名
    private String table;

    // 段状态
    private String status;

    // 段大小(KB)
    private String sizeKB;

    // 段输入记录数
    private String inputRecords;

    // 段开始日期
    private String dateRangeStart;

    // 段结束日期
    private String dateRangeEnd;

    // 段最后构建时间
    private String lastBuildTime;

    // 段最后构建任务id
    private String lastBuildJobId;
}
