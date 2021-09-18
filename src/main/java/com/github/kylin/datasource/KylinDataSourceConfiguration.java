package com.github.kylin.datasource;

import com.github.kylin.model.KylinApi;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-04-19 15:06
 * @description kylin datasource configuration.
 */
@Slf4j
@Configuration
public class KylinDataSourceConfiguration {
    @Bean(name = "kylinDataSourceProperties")
    @ConfigurationProperties(prefix = "spring.datasource.kylin")
    public KylinDataSourceProperties createKylinDataSourceProperties() {
        return new KylinDataSourceProperties();
    }

    @Bean(name = "kylinDataSource")
    public DataSource KylinDataSource(@Qualifier("kylinDataSourceProperties") KylinDataSourceProperties kylinDataSourceProperties) {
        return new KylinDataSource(kylinDataSourceProperties);
    }

    @Bean(name = "kylinTemplate")
    public JdbcTemplate kylinJdbcTemplate(@Qualifier("kylinDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "kylinApi")
    public KylinApi kylinApi(@Qualifier("kylinDataSourceProperties") KylinDataSourceProperties kylinDataSourceProperties){
        KylinApi kylinApi = new KylinApi();
        // jdbc:kylin://10.91.101.48:7070/sandian
        String jdbcUrl = kylinDataSourceProperties.getJdbcUrl();
        String[] tempArr = jdbcUrl.split("/");
        String hostPort = tempArr[2];
        String project = tempArr[3];

        String[] hostPortArr = hostPort.split(":");
        kylinApi.setProject(project);
        kylinApi.setServer(hostPortArr[0]);
        kylinApi.setPort(hostPortArr[1]);
        kylinApi.setUser(kylinDataSourceProperties.getUsername());
        kylinApi.setPassword(kylinDataSourceProperties.getPassword());

        return kylinApi;
    }
}

