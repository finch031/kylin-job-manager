package com.github.kylin.controller;

import com.github.kylin.service.IKylinJdbcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-04-19 14:08
 * @description
 */
@Controller
public class KylinController {
    @Autowired
    @Qualifier("kylinTemplate")
    private JdbcTemplate kylinTemplate;

    @Autowired
    private IKylinJdbcService kylinJdbcService;

    @RequestMapping("/kylin")
    public String kylin(){
       kylinJdbcService.getCubeAllSegmentDate("driving_current_hour_detail_cube");
       return "";
    }
}

