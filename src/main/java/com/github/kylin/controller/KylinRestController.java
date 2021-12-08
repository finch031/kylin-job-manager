package com.github.kylin.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.kylin.model.KylinCube;
import com.github.kylin.model.KylinErrorJob;
import com.github.kylin.service.IKylinRestService;
import com.github.kylin.task.KylinCubeDailyJob;
import com.github.kylin.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
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

    @Autowired
    private KylinCubeDailyJob kylinCubeDailyJob;

    @RequestMapping("/cubes")
    public String kylinCubes(){
        List<KylinCube> kylinCubes = kylinRestService.getKylinCubes();
        return JSONArray.toJSONString(kylinCubes);
    }

    @RequestMapping("/error_jobs")
    public String kylinJobs(String cube){
        if(cube == null || cube.isEmpty()){
            return "无效cube参数!";
        }
        List<KylinErrorJob> errorJobs = kylinRestService.getKylinErrorJobs(cube,8,4);
        return JSONArray.toJSONString(errorJobs);
    }

    @RequestMapping("/job_resume")
    public String kylinCubeJobResume(String jobId){
        if(jobId == null || jobId.isEmpty()){
            return "无效job id参数!";
        }
        return kylinRestService.cubeJobResume(jobId) + "";
    }

    @RequestMapping(value = "/build")
    public String kylinBuild(String cube,long startTs,long stopTs){
        if(cube == null || cube.isEmpty() || startTs >= stopTs || startTs <= 0){
            return "无效cube或时间参数!";
        }
        JSONObject response = kylinRestService.cubeBuild(cube,startTs,stopTs);
        return response.toJSONString();
    }

    @RequestMapping(value = "/segment_delete")
    public String kylinCubeSegmentDelete(String cube,String segment){
        if(cube == null || cube.isEmpty() || segment == null || segment.isEmpty()){
            return "无效cube或segment参数!";
        }
        JSONArray response = kylinRestService.cubeSegmentDelete(cube,segment);
        return response.toJSONString();
    }

    @PostMapping("/missing_cube_build")
    public void missingCubeBuild(String date){
        if(date != null && !date.isEmpty() && Utils.isValidDate(date)){
            kylinCubeDailyJob.missingCubeJobRunner(date);
        }
    }
}
