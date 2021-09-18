package com.github.kylin.service.impl;

import com.github.kylin.service.IKylinJdbcService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-16 17:05
 * @description kylin jdbc service impl
 */
@Slf4j
@Service
public class KylinJdbcServiceImpl implements IKylinJdbcService {
    @Autowired
    @Qualifier("kylinTemplate")
    private JdbcTemplate kylinTemplate;

    @Override
    public List<String> getCubeAllSegmentDate(String cube) {
        String tableName = cube.substring(0,cube.length() - 5);
        String sql = "select distinct(send_date) as dt from " + tableName + " order by dt desc";
        List<String> cubeDates = new ArrayList<>();

        kylinTemplate.query(sql, new ResultSetExtractor<String>() {
            @Override
            public String extractData(ResultSet rs) throws SQLException, DataAccessException {
                while(rs.next()){
                    cubeDates.add(rs.getString(1));
                }
                return "";
            }
        });
        return cubeDates;
    }

    @Override
    public boolean isCubeSegmentDateExists(String cube, String date) {
        String tableName = cube.substring(0,cube.length() - 5);
        String sql = "select count(1) from " + tableName + " where send_date = '" + date + "'";

        Integer count = kylinTemplate.query(sql, new ResultSetExtractor<Integer>() {
            @Override
            public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
                int recordNum = 0;
                while(rs.next()){
                    recordNum = rs.getInt(1);
                }
                return recordNum;
            }
        });
        return count != null && count> 0;
    }

    @Override
    public int cubeSegmentRecordCount(String cube, String date) {
        String tableName = cube.substring(0,cube.length() - 5);
        String sql = "select count(1) from " + tableName + " where send_date = '" + date + "'";

        Integer count = kylinTemplate.query(sql, new ResultSetExtractor<Integer>() {
            @Override
            public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
                int recordNum = 0;
                while(rs.next()){
                    recordNum = rs.getInt(1);
                }
                return recordNum;
            }
        });
        return count != null ? count : 0;
    }
}
