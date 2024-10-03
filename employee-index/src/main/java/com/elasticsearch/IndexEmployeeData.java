package com.elasticsearch;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import java.io.IOException;
import java.util.List;

public class IndexEmployeeData {
    public static void main(String[] args) {
        // Initialize connection to Elasticsearch
        ElasticsearchConnector connector = new ElasticsearchConnector();
        ElasticsearchClient client = connector.getClient();
        String filePath ="D:/java/employee-index/src/main/resources/employee_sample_data.csv";  // Adjust the path accordingly


        // Load employee data from CSV file
        List<String[]> employees = EmployeeDataLoader.loadEmployeeData(filePath);

        // Index each employee record into Elasticsearch
        for (String[] employee : employees) {
            String employeeId = employee[0];
            String employeeName = employee[1];
            String department = employee[2];
            String role = employee[3];
            String hireDate = employee[4];
            String salary = employee[5];

            // Create an Employee object to represent the employee data
            Employee employeeDocument = new Employee(employeeId, employeeName, department, role, hireDate, salary);

            try {
                // Create an IndexRequest to store the employee document in Elasticsearch
                IndexRequest<Employee> request = IndexRequest.of(i -> i
                        .index("employees")  // The index name
                        .id(employeeId)      // Document ID
                        .document(employeeDocument)  // The employee data to be indexed
                );

                // Execute the indexing request
                IndexResponse response = client.index(request);
                System.out.println("Indexed employee ID: " + response.id());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
