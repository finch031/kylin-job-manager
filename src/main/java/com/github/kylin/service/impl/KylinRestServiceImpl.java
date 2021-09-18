package com.github.kylin.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.kylin.model.KylinApi;
import com.github.kylin.model.KylinCube;
import com.github.kylin.model.KylinErrorJob;
import com.github.kylin.model.KylinSegment;
import com.github.kylin.service.IKylinRestService;
import com.github.kylin.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.Base64Utils;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-04-22 14:42
 * @description kylin rest service impl
 */
@Slf4j
@Service
public class KylinRestServiceImpl implements IKylinRestService {
    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private KylinApi kylinApi;

    @Override
    public List<KylinSegment> getKylinCubeSegments(String cube) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_ATOM_XML);
        String userPassword = kylinApi.getUser() + ":" + kylinApi.getPassword();
        String authorization = new String(Base64Utils.encode(userPassword.getBytes()));
        httpHeaders.add("Authorization","Basic " + authorization);

        HttpEntity<String> entity = new HttpEntity<>(httpHeaders);
        String url = kylinApi.restCubesApi() + "/" + cube + "/hbase" ;

        ResponseEntity<JSONArray> responseEntity = restTemplate.exchange(url, HttpMethod.GET,entity, JSONArray.class);
        List<KylinSegment> segments = new ArrayList<>();

        if(responseEntity.getStatusCode().is2xxSuccessful()){
            JSONArray body = responseEntity.getBody();
            if(body != null){
                int responseJsonSize = body.size();
                for(int i = 0; i < responseJsonSize; i++){
                    JSONObject jsonObject = body.getJSONObject(i);
                    String segmentName = jsonObject.getString("segmentName");
                    String segmentStatus = jsonObject.getString("segmentStatus");
                    String tableName = jsonObject.getString("tableName");
                    String tableSize = jsonObject.getString("tableSize");
                    String dateRangeStart = jsonObject.getString("dateRangeStart");
                    String dateRangeEnd = jsonObject.getString("dateRangeEnd");
                    String sourceCount = jsonObject.getString("sourceCount");

                    KylinSegment kylinSegment = new KylinSegment();
                    kylinSegment.setName(segmentName);
                    kylinSegment.setStatus(segmentStatus);
                    kylinSegment.setTable(tableName);
                    kylinSegment.setSizeKB(tableSize);
                    kylinSegment.setDateRangeStart(dateRangeStart);
                    kylinSegment.setDateRangeEnd(dateRangeEnd);
                    kylinSegment.setInputRecords(sourceCount);

                    segments.add(kylinSegment);
                }
            }
        }
        return segments;
    }

    @Override
    public List<KylinCube> getKylinCubes() {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_ATOM_XML);
        String userPassword = kylinApi.getUser() + ":" + kylinApi.getPassword();
        String authorization = new String(Base64Utils.encode(userPassword.getBytes()));
        httpHeaders.add("Authorization","Basic " + authorization);

        HttpEntity<String> entity = new HttpEntity<>(httpHeaders);
        ResponseEntity<JSONArray> responseEntity = restTemplate.exchange(kylinApi.restCubesApi(), HttpMethod.GET,entity, JSONArray.class);
        List<KylinCube> cubes = new ArrayList<>();
        if(responseEntity.getStatusCode().is2xxSuccessful()){
            JSONArray body = responseEntity.getBody();
            if(body != null){
                int responseJsonSize = body.size();
                for(int i = 0; i < responseJsonSize; i++){
                    JSONObject jsonObject = body.getJSONObject(i);

                    KylinCube kylinCube = new KylinCube();
                    String cubeID = jsonObject.getString("uuid");
                    String cubeName = jsonObject.getString("name");
                    String status = jsonObject.getString("status");
                    long lastModifiedTime = Long.parseLong(jsonObject.getString("last_modified"));

                    List<KylinSegment> kylinSegments = new ArrayList<>();

                    JSONArray segments = jsonObject.getJSONArray("segments");
                    for(int j = 0; j < segments.size(); j++){
                        JSONObject segment = segments.getJSONObject(j);
                        KylinSegment kylinSegment = new KylinSegment();

                        kylinSegment.setUuid(segment.getString("uuid"));
                        String[] segmentNameArr = segment.getString("name").split("_");
                        kylinSegment.setName(segmentNameArr[0].substring(0,8) + "-" + segmentNameArr[1].substring(0,8));
                        kylinSegment.setTable(segment.getString("storage_location_identifier"));
                        kylinSegment.setStatus(segment.getString("status"));
                        kylinSegment.setSizeKB(segment.getString("size_kb"));
                        kylinSegment.setInputRecords(segment.getString("input_records"));

                        long dateRangeStart = Long.parseLong(segment.getString("date_range_start"));
                        long dateRangeEnd = Long.parseLong(segment.getString("date_range_end"));
                        kylinSegment.setDateRangeStart(Utils.formatDateTime(dateRangeStart,"yyyy-MM-dd"));
                        kylinSegment.setDateRangeEnd(Utils.formatDateTime(dateRangeEnd,"yyyy-MM-dd"));

                        long lastBuildTime = Long.parseLong(segment.getString("last_build_time"));
                        kylinSegment.setLastBuildTime(Utils.formatDateTime(lastBuildTime,"yyyy-MM-dd HH:mm:ss"));
                        kylinSegment.setLastBuildJobId(segment.getString("last_build_job_id"));

                        kylinSegments.add(kylinSegment);
                    }

                    kylinCube.setCubeID(cubeID);
                    kylinCube.setCubeName(cubeName);
                    kylinCube.setStatus(status);
                    kylinCube.setLastModifiedTime(Utils.formatDateTime(lastModifiedTime,"yyyy-MM-dd HH:mm:ss"));
                    kylinCube.setSegments(kylinSegments);

                    cubes.add(kylinCube);
                }
            }
        }
        return cubes;
    }

    private JSONArray getKylinJobs(String cube, int jobStatus, int timeFilter) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_ATOM_XML);
        String userPassword = kylinApi.getUser() + ":" + kylinApi.getPassword();
        String authorization = new String(Base64Utils.encode(userPassword.getBytes()));
        httpHeaders.add("Authorization","Basic " + authorization);

        HttpEntity<String> entity = new HttpEntity<>(httpHeaders);

        String url = kylinApi.restJobsApi() + "?cubeName=" + cube + "&status=" +
                     jobStatus + "&projectName=" + kylinApi.getProject() + "&timeFilter=" + timeFilter;
        ResponseEntity<JSONArray> responseEntity = restTemplate.exchange(url, HttpMethod.GET,entity, JSONArray.class);

        if(responseEntity.getStatusCode().is2xxSuccessful()){
            return responseEntity.getBody();
        }
        return null;
    }

    @Override
    public List<KylinErrorJob> getKylinErrorJobs(String cube,int jobStatus, int timeFilter){
        JSONArray body = getKylinJobs(cube,jobStatus,timeFilter);
        List<KylinErrorJob> errorJobs = new ArrayList<>();
        if(body == null){
            return errorJobs;
        }

        for(int i = 0; i < body.size(); i++){
            JSONObject jsonObject = body.getJSONObject(i);

            KylinErrorJob errorJob = new KylinErrorJob();

            errorJob.setJobID(jsonObject.getString("uuid"));
            errorJob.setJobName(jsonObject.getString("name"));
            long lastModifiedTime = Long.parseLong(jsonObject.getString("last_modified"));
            errorJob.setLastModifiedTime(Utils.formatDateTime(lastModifiedTime,"yyyy-MM-dd HH:mm:ss"));
            errorJob.setJobCubeName(jsonObject.getString("related_cube"));
            long execStartTime = Long.parseLong(jsonObject.getString("exec_start_time"));
            long execEndTime = Long.parseLong(jsonObject.getString("exec_end_time"));
            errorJob.setExecStartTime(Utils.formatDateTime(execStartTime,"yyyy-MM-dd HH:mm:ss"));
            errorJob.setExecEndTime(Utils.formatDateTime(execEndTime,"yyyy-MM-dd HH:mm:ss"));

            errorJobs.add(errorJob);
        }
        return errorJobs;
    }

    @Override
    public JSONArray cubeSegmentDelete(String cube,String segment){
        HttpHeaders httpHeaders = new HttpHeaders();
        String userPassword = kylinApi.getUser() + ":" + kylinApi.getPassword();
        String authorization = new String(Base64Utils.encode(userPassword.getBytes()));
        httpHeaders.add("Authorization","Basic " + authorization);
        httpHeaders.add("Content-Type","application/json;charset=utf-8");

        HttpEntity<String> entity = new HttpEntity<>(httpHeaders);
        String url = kylinApi.restCubesApi() + "/" + cube + "/segs/" + segment;
        ResponseEntity<JSONArray> responseEntity = restTemplate.exchange(url, HttpMethod.DELETE,entity, JSONArray.class);
        return responseEntity.getBody();
    }

    @Override
    public boolean cubeJobResume(String jobID) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_ATOM_XML);
        String userPassword = kylinApi.getUser() + ":" + kylinApi.getPassword();
        String authorization = new String(Base64Utils.encode(userPassword.getBytes()));
        httpHeaders.add("Authorization","Basic " + authorization);

        HttpEntity<String> entity = new HttpEntity<>(httpHeaders);
        String url = kylinApi.restJobsApi() + "/" + jobID + "/resume";
        ResponseEntity<JSONArray> responseEntity = restTemplate.exchange(url, HttpMethod.PUT,entity, JSONArray.class);

        return responseEntity.getStatusCode().is2xxSuccessful();
    }

    @Override
    public JSONObject cubeBuild(String cube, long buildStartTs,long buildStopTs) {
        // kylin服务时区配置问题,+8小时
        long realBuildStartTs = buildStartTs + 8 * 3600000;
        long realBuildStopTs = buildStopTs + 8 * 3600000;

        String url = kylinApi.restCubesApi() + "/" + cube + "/build";

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        String userPassword = kylinApi.getUser() + ":" + kylinApi.getPassword();
        String authorization = new String(Base64Utils.encode(userPassword.getBytes()));
        httpHeaders.add("Authorization","Basic " + authorization);
        httpHeaders.add("Content-Type","application/json;charset=utf-8");

        // post请求体
        JSONObject jsonBody = new JSONObject();
        // 构建开始时间戳(ms)
        jsonBody.put("startTime",realBuildStartTs);
        // 构建结束时间戳(ms)
        jsonBody.put("endTime",realBuildStopTs);
        jsonBody.put("buildType","BUILD");

        HttpEntity<JSONObject> entity = new HttpEntity<>(jsonBody,httpHeaders);
        ResponseEntity<JSONObject> responseEntity = restTemplate.exchange(url,HttpMethod.PUT,entity,JSONObject.class);

        return responseEntity.getBody();
    }
}

