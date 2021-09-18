package com.github.kylin.model;

import lombok.Data;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-15 14:36
 * @description kylin api
 */
@Data
public class KylinApi {
    private String project;
    private String server;
    private String port;
    private String user;
    private String password;

    private String restBaseApi(){
        return "http://" + server + ":" + port + "/kylin/api";
    }

    public String restCubesApi(){
        return restBaseApi() + "/cubes";
    }

    public String restJobsApi(){
        return restBaseApi() + "/jobs";
    }
}
