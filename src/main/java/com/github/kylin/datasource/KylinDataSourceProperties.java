package com.github.kylin.datasource;

import lombok.Data;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-04-19 14:47
 * @description kylin data source properties.
 */
@Data
public class KylinDataSourceProperties {
     // 数据源名称
    private String name;

    // 数据源类型
    private String type;

    // 数据库连接url
    private String jdbcUrl;

    // 数据库用户名
    private String username;

    // 数据库密码
    private String password;

    // 驱动
    private String driverClassName;

    // 获取连接时最大等待时间,毫秒
    private long maxWaitTime;

    // 连接池大小
    private int poolSize;
}