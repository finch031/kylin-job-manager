package com.github.kylin.model;

import lombok.Data;
import java.util.List;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-15 15:09
 * @description kylin cube
 */
@Data
public class KylinCube {
    // cube id
    private String cubeID;

    // cube名称
    private String cubeName;

    // cube状态
    private String status;

    // cube最近修改时间
    private String lastModifiedTime;

    // cube的段数据
    private List<KylinSegment> segments;
}
