package com.github.kylin.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.kylin.model.KylinCube;
import com.github.kylin.model.KylinErrorJob;
import com.github.kylin.service.IKylinRestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-04-22 15:28
 * @description kylin rest controller
 */
@RestController
public class KylinRestController {
    @Autowired
    private IKylinRestService kylinRestService;

    @RequestMapping("/cubes")
    public String kylinCubes(){
        List<KylinCube> kylinCubes = kylinRestService.getKylinCubes();
        return JSONArray.toJSONString(kylinCubes);
    }

    @RequestMapping("/error_jobs")
    public String kylinJobs(){
        List<KylinErrorJob> errorJobs = kylinRestService.getKylinErrorJobs("motor_con_temp_detail_cube",8,4);
        return JSONArray.toJSONString(errorJobs);
    }

    @RequestMapping("/job_resume")
    public String kylinCubeJobResume(){
        return kylinRestService.cubeJobResume("9c28f8d7-7835-ea69-1008-129d8a97ec23") + "";
    }

    @RequestMapping(value = "/build")
    public String kylinBuild(){
        JSONObject response = kylinRestService.cubeBuild("driving_speed_hour_detail_cube",1621180800000L,1621267200000L);
        return response.toJSONString();
    }

    @RequestMapping(value = "/segment_delete")
    public String kylinCubeSegmentDelete(){
        JSONArray response = kylinRestService.cubeSegmentDelete("driving_speed_hour_detail_cube","20210517000000_20210518000000");
        return response.toJSONString();
    }
}
