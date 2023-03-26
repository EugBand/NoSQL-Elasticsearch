package com.epam.elasticsearch.configuration;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class ElasticSearchConfiguration {

    @Value("${application.elasticsearch.server}")
    private String elasticServerUrl;

    @Value("${application.elasticsearch.port}")
    private Integer elasticServerPort;

    @Value("${application.elasticsearch.protocol}")
    private String elasticServerProtocol;

    @Bean
    RestClient restClient() throws IOException {
        return RestClient.builder(
                        new HttpHost(elasticServerUrl, elasticServerPort, elasticServerProtocol))
                .build();
    }

    @Bean
    ElasticsearchClient elasticsearchClient() throws IOException {
        ElasticsearchTransport transport = new RestClientTransport(
                restClient(), new JacksonJsonpMapper());

        return new ElasticsearchClient(transport);
    }

}
