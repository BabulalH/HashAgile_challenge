package com.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.entity.ContentType;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyElasticFunctions {

    public void createCollection(String collectionName) {
        ElasticsearchClient client = new ElasticsearchConnector().getClient();

        try {
            // Check if the index already exists
            boolean exists = client.indices().exists(ExistsRequest.of(e -> e.index(collectionName))).value();

            if (!exists) {
                // Create the index if it does not exist
                client.indices().create(c -> c.index(collectionName));
                System.out.println("Collection created: " + collectionName);
            } else {
                System.out.println("Collection " + collectionName + " already exists.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void indexDataLowLevel(String collectionName, String excludeColumn) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            List<String[]> employees = EmployeeDataLoader.loadEmployeeData("src/main/resources/employee_sample_data.csv");

            for (String[] employee : employees) {
                Map<String, Object> employeeMap = new HashMap<>();
                employeeMap.put("id", employee[0]);
                employeeMap.put("name", employee[1]);
                employeeMap.put("department", employee[2]);
                employeeMap.put("role", employee[3]);
                employeeMap.put("hireDate", employee[4]);
                employeeMap.put("salary", employee[5]);

                // Exclude the provided column
                employeeMap.remove(excludeColumn.toLowerCase());

                try {
                    // Convert the employeeMap to JSON string
                    String jsonString = new ObjectMapper().writeValueAsString(employeeMap);

                    // Trim and encode the employee ID
                    String employeeId = URLEncoder.encode(employee[0].trim(), StandardCharsets.UTF_8.toString());

                    // Construct a POST request to index the document
                    HttpPost postRequest = new HttpPost("http://localhost:9200/" + collectionName + "/_doc/" + employeeId);
                    postRequest.setEntity(new StringEntity(jsonString, ContentType.APPLICATION_JSON));

                    // Execute the request
                    try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
                        if (response.getStatusLine().getStatusCode() == 201) {
                            System.out.println("Indexed employee ID: " + employeeId);
                        } else {
                            System.out.println("Failed to index employee ID: " + employeeId + ", Status: " + response.getStatusLine());
                        }
                    }
                } catch (IOException e) {
                    System.out.println("Error indexing employee ID: " + employee[0]);
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void indexData(String collectionName, String excludeColumn) {
        List<String[]> employees = EmployeeDataLoader.loadEmployeeData("src/main/resources/employee_sample_data.csv");
        ElasticsearchClient client = new ElasticsearchConnector().getClient();

        for (String[] employee : employees) {
            Map<String, Object> employeeMap = new HashMap<>();
            employeeMap.put("id", employee[0]);
            employeeMap.put("name", employee[1]);
            employeeMap.put("department", employee[2]);
            employeeMap.put("role", employee[3]);
            employeeMap.put("hireDate", employee[4]);
            employeeMap.put("salary", employee[5]);

            // Exclude the provided column
            employeeMap.remove(excludeColumn.toLowerCase());

            try {
                // Create the request without specifying the ID
                IndexRequest<Map<String, Object>> request = IndexRequest.of(i -> i
                        .index(collectionName)   // The collection name (index)
                        .document(employeeMap)   // The employee document (data) to be indexed
                );

                // Execute the request
                client.index(request);
                System.out.println("Indexed employee ID: " + employee[0]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public void searchByColumn(String collectionName, String columnName, String columnValue) {
        ElasticsearchClient client = new ElasticsearchConnector().getClient();

        try {
            SearchResponse<Map> response = client.search(s -> s
                            .index(collectionName)
                            .query(q -> q.term(t -> t.field(columnName).value(columnValue)))
                    , Map.class);

            response.hits().hits().forEach(hit -> System.out.println(hit.source()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getEmpCount(String collectionName) {
        ElasticsearchClient client = new ElasticsearchConnector().getClient();
        try {
            CountResponse countResponse = client.count(c -> c.index(collectionName));
            System.out.println("Employee count in " + collectionName + ": " + countResponse.count());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void delEmpById(String collectionName, String employeeId) {
        ElasticsearchClient client = new ElasticsearchConnector().getClient();
        try {
            client.delete(d -> d.index(collectionName).id(employeeId));
            System.out.println("Deleted employee with ID: " + employeeId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getDepFacet(String collectionName) {
        ElasticsearchClient client = new ElasticsearchConnector().getClient();
        try {
            SearchResponse<Map> response = client.search(s -> s
                            .index(collectionName)
                            .size(0)
                            .aggregations("departments", a -> a.terms(t -> t.field("department.keyword")))
                    , Map.class);

            response.aggregations().get("departments").sterms().buckets().array().forEach(bucket -> {
                System.out.println(bucket.key() + ": " + bucket.docCount());
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String nameCollection = "Babulal" + System.currentTimeMillis();
        String phoneCollection = "1234"; // Last 4 digits of your phone

        MyElasticFunctions elastic = new MyElasticFunctions();

        elastic.createCollection(nameCollection);
        elastic.createCollection(phoneCollection);
        elastic.getEmpCount(nameCollection);

        // Call the low-level indexing method
        elastic.indexDataLowLevel(nameCollection, "Department");
        elastic.indexDataLowLevel(phoneCollection, "Gender");

        elastic.delEmpById(nameCollection, "E02003");
        elastic.getEmpCount(nameCollection);
        elastic.searchByColumn(nameCollection, "Department", "IT");
        elastic.searchByColumn(nameCollection, "Gender", "Male");
        elastic.searchByColumn(phoneCollection, "Department", "IT");
        elastic.getDepFacet(nameCollection);
        elastic.getDepFacet(phoneCollection);
    }
}
