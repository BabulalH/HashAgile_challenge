package com.elasticsearch;
import com.opencsv.CSVReader;
import java.io.FileReader;
import java.util.List;

public class EmployeeDataLoader {
    // Method to load employee data from CSV file
    public static List<String[]> loadEmployeeData(String filePath) {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            return reader.readAll();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
