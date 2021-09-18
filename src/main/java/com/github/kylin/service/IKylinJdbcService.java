package com.github.kylin.service;

import java.util.List;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-16 17:03
 * @description kylin jdbc service.
 */
public interface IKylinJdbcService {
    /**
     * 获取cube所有已构建segment日期(通过sql查询日期字段获取)
     * @param cube cube名称
     * */
    List<String> getCubeAllSegmentDate(String cube);

    /**
     * 判断指定日期的cube segment是否存在.
     * @param cube cube名称
     * @param date 日期(格式:yyyy-MM-dd)
     * @return true: 存在, false: 不存在
     * */
    boolean isCubeSegmentDateExists(String cube,String date);

    /**
     * 查询指定日期的cube segment记录条数.
     * @param cube cube名称
     * @param date 日期(格式:yyyy-MM-dd)
     * @return segment record count.
     * */
    int cubeSegmentRecordCount(String cube,String date);
}
