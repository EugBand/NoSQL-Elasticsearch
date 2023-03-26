package com.epam.elasticsearch.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.epam.elasticsearch.dto.Employee;
import com.epam.elasticsearch.service.EmployeeApiServiceImpl;

@RestController
@RequestMapping(value = "/api/v2/employees", produces = "application/json")
public class EmployeeApiController {

    @Qualifier("api-service")
    private final EmployeeApiServiceImpl service;

    public EmployeeApiController(EmployeeApiServiceImpl service) {
        this.service = service;
    }

    @GetMapping
    ResponseEntity<List<Employee>> getAll() {
        try {
            List<Employee> employees = service.getAll();
            return new ResponseEntity<>(employees, HttpStatus.OK);
        } catch (IOException e) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/{id}")
    ResponseEntity<Employee> getById(
            @PathVariable
            String id) throws IOException {
        Optional<Employee> employee = service.getById(id);
        return employee.map(value -> new ResponseEntity<>(value, HttpStatus.OK))
                .orElseGet(() -> new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    @PutMapping("/{id}")
    ResponseEntity<Void> createWithId(
            @RequestBody
            Employee employee,
            @PathVariable
            String id) {
        try {
            service.createWithId(employee, id);
            return new ResponseEntity<>(HttpStatus.CREATED);
        } catch (IOException e) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }

    @DeleteMapping("/{id}")
    ResponseEntity<Void> deleteById(
            @PathVariable
            String id) {
        try {
            service.deleteById(id);
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } catch (IOException e) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }

    @PostMapping("/search")
    ResponseEntity<List<Employee>> search(
            @RequestParam
            MultiValueMap<String, String> params) throws IOException {
        List<Employee> employees = service.search(params);
        return new ResponseEntity<>(employees, HttpStatus.OK);
    }

    @PostMapping("/agg")
    ResponseEntity<String> aggregation(
            @RequestParam
            Map<String, String> params) {
        try {
            String response = service.aggregate(params);
            return new ResponseEntity<>(response, HttpStatus.OK);
        } catch (IOException e) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }

}
