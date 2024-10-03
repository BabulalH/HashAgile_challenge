package com.elasticsearch;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;

public class ElasticsearchConnector {
    private ElasticsearchClient client;

    // Constructor: Initialize the connection to Elasticsearch
    public ElasticsearchConnector() {
        // Create the low-level REST client
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                .setRequestConfigCallback(requestConfigBuilder ->
                        requestConfigBuilder
                                .setConnectTimeout(5000)  // Connection timeout
                                .setSocketTimeout(120000)) // Socket timeout (increased to 120 seconds)
                .build();



        // Create the transport layer for the Java client
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        // Initialize the Elasticsearch client
        client = new ElasticsearchClient(transport);
    }

    // Method to retrieve the Elasticsearch client object
    public ElasticsearchClient getClient() {
        return client;
    }
}
