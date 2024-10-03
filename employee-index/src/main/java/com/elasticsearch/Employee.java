package com.elasticsearch;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Employee {
    @JsonProperty("employeename")
    private String employeeName;

    @JsonProperty("employeeId")
    private String employeeId;

    @JsonProperty("department")
    private String department;

    @JsonProperty("role")
    private String role;

    @JsonProperty("hireDate")
    private String hireDate;

    @JsonProperty("salary")
    private String salary;

    // Default constructor (required by Jackson)
    public Employee() {
    }

    // Constructor with parameters
    public Employee(String employeeId, String employeeName, String department, String role, String hireDate, String salary) {
        this.employeeId = employeeId;
        this.employeeName = employeeName;
        this.department = department;
        this.role = role;
        this.hireDate = hireDate;
        this.salary = salary;
    }

    // Getters and Setters
    public String getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(String employeeId) {
        this.employeeId = employeeId;
    }

    public String getEmployeeName() {
        return employeeName;
    }

    public void setEmployeeName(String employeeName) {
        this.employeeName = employeeName;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getHireDate() {
        return hireDate;
    }

    public void setHireDate(String hireDate) {
        this.hireDate = hireDate;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }
}
