package com.wujunshen.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * @author frank woo(吴峻申) <br>
 * email:<a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/18 09:32<br>
 */
@Configuration
public class ElasticSearchConfig {
    private final ElasticSearchConfigProperties elasticSearchConfigProperties;

    public ElasticSearchConfig(ElasticSearchConfigProperties elasticSearchConfigProperties) {
        this.elasticSearchConfigProperties = elasticSearchConfigProperties;
    }

    @Bean
    public RestClient restClient() {
        // 拆分地址
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] hostArray = elasticSearchConfigProperties.getAddress().split(",");
        for (String element : hostArray) {
            String host = element.split(":")[0];
            String port = element.split(":")[1];
            httpHostList.add(new HttpHost(host, Integer.parseInt(port),
                    elasticSearchConfigProperties.getSchema()));
        }

        // 转换成 HttpHost 数组
        HttpHost[] httpHostArray = httpHostList.toArray(new HttpHost[]{});
        // 构建连接对象
        RestClientBuilder builder = RestClient.builder(httpHostArray);
        // 异步连接延时配置
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(elasticSearchConfigProperties.getConnectTimeout());
            requestConfigBuilder.setSocketTimeout(elasticSearchConfigProperties.getSocketTimeout());
            requestConfigBuilder.setConnectionRequestTimeout(elasticSearchConfigProperties.getConnectionRequestTimeout());
            return requestConfigBuilder;
        });

        // 异步连接数配置
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(elasticSearchConfigProperties.getMaxConnectNum());
            httpClientBuilder.setMaxConnPerRoute(elasticSearchConfigProperties.getMaxConnectPerRoute());
            return httpClientBuilder;
        });

        return builder.build();
    }

    @Bean
    public ElasticsearchTransport elasticsearchTransport(RestClient restClient) {
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    @Bean
    public ElasticsearchClient elasticsearchClient(ElasticsearchTransport transport) {
        return new ElasticsearchClient(transport);
    }
}
