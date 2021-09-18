package com.github.kylin.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.kylin.model.KylinCube;
import com.github.kylin.model.KylinErrorJob;
import com.github.kylin.model.KylinSegment;

import java.util.List;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-04-22 14:36
 * @description kylin rest service interface
 */
public interface IKylinRestService {

    /**
     * 获取指定cube的所有segment.
     * @param cube cube名称.
     * */
    List<KylinSegment> getKylinCubeSegments(String cube);

    /**
     * 获取所有cubes信息.
     * */
    List<KylinCube> getKylinCubes();

    /**
     * 获取指定cube错误任务.
     * @param cube cube名称
     * @param jobStatus 任务状态,默认为8,即错误任务
     * @param timeFilter 时间过滤范围
     * */
    List<KylinErrorJob> getKylinErrorJobs(String cube, int jobStatus, int timeFilter);

    /**
     * cube任务恢复重跑.
     * @param jobID 任务id
     * */
    boolean cubeJobResume(String jobID);

    /**
     * cube任务构建
     * @param cube cube名称
     * @param buildStartTs 构建开始时间戳(ms)
     * @param buildStopTs 构建结束时间戳(ms)
     * */
    JSONObject cubeBuild(String cube, long buildStartTs, long buildStopTs);

    /**
     * cube段删除
     * @param cube cube名称
     * @param segment cube段名称(如:20210502000000_20210503000000)
     * */
    JSONArray cubeSegmentDelete(String cube,String segment);
}
