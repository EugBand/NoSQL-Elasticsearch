package com.epam.elasticsearch.service;

import com.epam.elasticsearch.dto.Employee;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface EmployeeService {

    List<Employee> getAll() throws IOException;

    Optional<Employee> getById(String id) throws IOException;

    void createWithId(Employee employee, String id) throws IOException;

    void deleteById(String id) throws IOException;

    List<Employee> search(MultiValueMap<String, String> params) throws IOException;

    String aggregate(Map<String, String> params) throws IOException;

}
