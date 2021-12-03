package com.github.kylin.task;

import com.alibaba.fastjson.JSONObject;
import com.github.kylin.model.KylinCube;
import com.github.kylin.model.KylinSegment;
import com.github.kylin.service.IKylinJdbcService;
import com.github.kylin.service.IKylinRestService;
import com.github.kylin.utils.Tuple;
import com.github.kylin.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-17 10:11
 * @description kylin cube daily job.
 */
@Component
@EnableScheduling
@PropertySource("classpath:application.properties")
public class KylinCubeDailyJob {
    private static final Logger LOG = LogManager.getLogger(KylinCubeDailyJob.class);

    @Value("${spring.kylin.cube.report.dir}")
    private String cubeReportDir;

    @Value("${spring.kylin.cube.report.count}")
    private boolean cubeReportCount;

    @Autowired
    private KylinCubeSegmentChecker kylinCubeSegmentChecker;

    @Autowired
    private IKylinRestService kylinRestService;

    @Autowired
    private IKylinJdbcService kylinJdbcService;

    /**
     * 每日缺失cube任务构建.
     * 默认每天上午10点30分触发一次.
     * */
    @Scheduled(cron = "0 30 10 ? * *")
    private void dailyMissingCubeJobRunner(){
        String now = Utils.formatDateTime(System.currentTimeMillis(),"yyyy-MM-dd");
        String buildDate = Utils.dateShift(now,"yyyy-MM-dd",-1);

        List<Tuple<String,Integer>> cubeSegmentInfoList =  kylinCubeSegmentChecker.allCubeDateSegmentCheck(buildDate);
        for (Tuple<String, Integer> segmentInfo : cubeSegmentInfoList) {
            // 当前时间点segment仍然无数据,判定脚本调度构建任务异常.
            if(segmentInfo.v2() == 0){
               String cube = segmentInfo.v1();
                List<KylinSegment> cubeSegments = kylinRestService.getKylinCubeSegments(cube);
                boolean segmentOverlappedFlag = false;
                for (KylinSegment segment : cubeSegments) {
                    String dateRangeStart = segment.getDateRangeStart();
                    String dateRangeEnd = segment.getDateRangeEnd();
                    // 判断构建任务的时间范围上是否段重叠
                    if(Utils.isSegmentOverlapped(buildDate,"yyyy-MM-dd",Long.parseLong(dateRangeStart),Long.parseLong(dateRangeEnd))){
                        // 重叠段的大小或记录数大于0才最终确认为段重叠
                        if(Integer.parseInt(segment.getSizeKB()) > 0 || Integer.parseInt(segment.getInputRecords()) > 0){
                            segmentOverlappedFlag = true;
                        }
                    }
                }

                if(!segmentOverlappedFlag){
                    LOG.warn(cube + " T-1日:" + buildDate + " segment无数据,启动缺失cube任务构建...");
                    long buildStartTs = Utils.startTimeStampOfDate(buildDate,"yyyy-MM-dd");
                    long buildStopTs = buildStartTs + 86400000L;
                    JSONObject buildResponse = kylinRestService.cubeBuild(cube,buildStartTs,buildStopTs);
                    LOG.warn(cube + "任务构建完成:");
                    LOG.warn(buildResponse.toJSONString());
                }else {
                    LOG.warn(cube + " T-日:" + buildDate + " segment和已有segment发生重叠,任务构建终止!");
                }

                Utils.sleepQuietly(10 * 1000);
            }
        }
    }

    /**
     * 每日cube报告.
     * 默认每天上午10点30分触发一次.
     * */
    // @Scheduled(cron = "0 30 10 ? * *")
    private void dailyCubeReporter(){
        long startTs = System.currentTimeMillis();
        String reportName = "cube_report_" + Utils.formatDateTime(System.currentTimeMillis(),"yyyyMMdd") + ".csv";
        String reportFile = cubeReportDir + "/" + reportName;

        List<String> reportLines = new ArrayList<>();

        List<KylinCube> kylinCubes = kylinRestService.getKylinCubes();
        Map<String,KylinCube> cubeMap = new HashMap<>();
        for (KylinCube kylinCube : kylinCubes) {
            cubeMap.put(kylinCube.getCubeName(),kylinCube);
        }

        int cubeIdx = 0;
        for (String defaultCube : kylinCubeSegmentChecker.getDefaultCubes()) {
            cubeIdx++;
            KylinCube kylinCube = cubeMap.get(defaultCube);
            List<KylinSegment> segments = kylinCube.getSegments();

            if(cubeIdx == 1){
                reportLines.add("   cube编号:" + cubeIdx);
            }else{
                reportLines.add("cube编号:" + cubeIdx);
            }

            // cube总大小.
            long cubeSegmentTotalSize = segments.stream().mapToLong(x -> 1000 * Long.parseLong(x.getSizeKB())).sum();
            reportLines.add("cube名称,segment总个数,segment总大小,cube状态,cube最后修改时间");
            String cubeHeader = defaultCube + "," + kylinCube.getSegments().size() + "," + Utils.formatBytes(cubeSegmentTotalSize) + "," + kylinCube.getStatus() + "," + kylinCube.getLastModifiedTime();
            reportLines.add(cubeHeader);

            segments.sort(new Comparator<KylinSegment>() {
                @Override
                public int compare(KylinSegment o1, KylinSegment o2) {
                    // 按segment日期降序
                    return -o1.getName().compareToIgnoreCase(o2.getName());
                }
            });

            String segmentHeader = "segment名称,segment大小KB,segment输入记录数,HBase表名,segment最后构建时间";
            reportLines.add(segmentHeader);

            for (KylinSegment segment : segments) {
                StringBuilder sb = new StringBuilder();
                // 段名称20210818-20210819
                sb.append(segment.getName());
                sb.append(",");
                // 段的大小
                sb.append(Utils.formatBytes(Long.parseLong(segment.getSizeKB())));
                sb.append(",");
                // 段的原始输入记录数
                sb.append(segment.getInputRecords());
                sb.append(",");
                // 段的HBase表名
                sb.append(segment.getTable());
                sb.append(",");
                // 段的最后构建时间
                sb.append(segment.getLastBuildTime());
                reportLines.add(sb.toString());
            }

            // kylin cube中所有segment的记录数统计(资源消耗操作,默认不开启).
            if(cubeReportCount){
                // cube所有segment记录数.
                int cubeTotalSegmentRecordCount = kylinJdbcService.cubeSegmentTotalRecordCount(defaultCube);

                String now = Utils.formatDateTime(System.currentTimeMillis(),"yyyy-MM-dd");
                String t_1Date = Utils.dateShift(now,"yyyy-MM-dd",-1);
                String t_2Date = Utils.dateShift(now,"yyyy-MM-dd",-2);
                String t_3Date = Utils.dateShift(now,"yyyy-MM-dd",-3);

                // t-1日segment记录数
                int t_1Count = kylinJdbcService.cubeSegmentRecordCount(defaultCube,t_1Date);
                // t-2日segment记录数
                int t_2Count = kylinJdbcService.cubeSegmentRecordCount(defaultCube,t_2Date);
                // t-3日segment记录数
                int t_3Count = kylinJdbcService.cubeSegmentRecordCount(defaultCube,t_3Date);

                reportLines.add("cube总记录数,t-1日记录数,t-2日记录数,t-3日记录数");
                reportLines.add(cubeTotalSegmentRecordCount + "," + t_1Count + "," + t_2Count + "," + t_3Count);
            }

            reportLines.add(Utils.LINE_SEPARATOR);
        }

        // 报告数据写csv文件
        Utils.writeCsvFile(reportFile,reportLines);

        String reportTime = Utils.appendPosixTime(System.currentTimeMillis() - startTs);
        LOG.info("报告生成总耗时:" + reportTime);

        reportLines.forEach(System.out::println);
    }
}
