package com.github.kylin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-15 11:02
 * @description kylin job manager.
 */
@SpringBootApplication
public class KylinJobManager {
    private static final Logger LOG = LogManager.getLogger(KylinJobManager.class);

    public static void main(String[] args){
        SpringApplication.run(KylinJobManager.class, args);
        LOG.info("kylin job manager start up!");
    }
}
