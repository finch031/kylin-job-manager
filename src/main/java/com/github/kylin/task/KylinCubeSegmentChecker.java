package com.github.kylin.task;

import com.github.kylin.service.IKylinJdbcService;
import com.github.kylin.service.IKylinRestService;
import com.github.kylin.utils.Tuple;
import com.github.kylin.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-16 18:13
 * @description kylin cube段定期检查
 */
@Component
public class KylinCubeSegmentChecker {
    private static final List<String> defaultCubes = Utils.readResourceAsLines("default-cubes.txt");

    @Autowired
    private IKylinJdbcService kylinJdbcService;

    @Autowired
    private IKylinRestService kylinRestService;

    public List<String> getDefaultCubes(){
        return defaultCubes;
    }

    /**
     * 查询cube指定日期的segment info.
     * @param cube cube名称
     * @param date 日期(yyyy-MM-dd)
     * @return segmentInfo
     * */
    private Tuple<String,Integer> cubeDateSegmentCheck(String cube, String date){
        int count = -1;

        int hasTriedTimes = 0;
        boolean checkSuccess = false;
        do{
            // 先执行一遍循环操作
            try{
                count = kylinJdbcService.cubeSegmentRecordCount(cube,date);
            }catch (Exception ex){
                // ignores
            }
            hasTriedTimes++;

            if(count >= 0){
                checkSuccess = true;
            }
        } while (hasTriedTimes <= 5 && !checkSuccess);
          // 符合条件,循环继续执行,否则,循环退出.

        return new Tuple<>(cube,count);
    }

    /**
     *  查询所有cube指定日期的segment info.
     * @param date 查询日期(yyyy-MM-dd)
     * @return all cube segment info.
     * */
    public List<Tuple<String,Integer>> allCubeDateSegmentCheck(String date){
        List<Tuple<String,Integer>> segmentInfoList = new ArrayList<>();
        for (String cube : defaultCubes) {
            segmentInfoList.add(cubeDateSegmentCheck(cube,date));
        }
        return segmentInfoList;
    }
}
