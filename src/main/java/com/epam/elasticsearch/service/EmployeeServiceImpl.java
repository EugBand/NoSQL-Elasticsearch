package com.epam.elasticsearch.service;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import com.epam.elasticsearch.dto.Employee;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service("service")
@RequiredArgsConstructor
@Slf4j
public class EmployeeServiceImpl implements EmployeeService {

    private final RestClient restClient;

    private final ObjectMapper mapper;

    private static String getResponseBody(Response response) throws IOException {
        return EntityUtils.toString(response.getEntity());
    }

    @Override
    public List<Employee> getAll() throws IOException {
        Request request = new Request(
                "GET",
                "/employees/_search"
        );

        Response response = restClient.performRequest(request);
        String responseBody = getResponseBody(response);

        return getEmployeesFromResponse(responseBody);
    }

    @Override
    public Optional<Employee> getById(String id) throws IOException {
        String path = String.format("/employees/_doc/%s", id);
        Request request = new Request(
                "GET",
                path
        );

        Response response;
        try {
            response = restClient.performRequest(request);
        } catch (ResponseException e) {
            log.warn("Employee not found by index: " + id);
            return Optional.empty();
        }

        String responseBody = getResponseBody(response);

        return getEmployeeFromResponse(responseBody);
    }

    @Override
    public void createWithId(Employee employee, String id) throws IOException {
        assert id.isEmpty() : "Id shouldn't be empty or null";
        String path = String.format("/employees/_doc/%s", id);
        Request request = new Request(
                "PUT",
                path
        );
        String jsonEntity = mapper.writeValueAsString(employee);
        request.setJsonEntity(jsonEntity);

        restClient.performRequest(request);
    }

    @Override
    public void deleteById(String id) throws IOException {
        String path = String.format("/employees/_doc/%s", id);
        Request request = new Request(
                "DELETE",
                path
        );

        restClient.performRequest(request);
    }

    @Override
    public List<Employee> search(MultiValueMap<String, String> params) throws IOException {
        StringBuilder query = new StringBuilder();
        query.append("""
                {
                \t"query": {
                \t\t"bool": {
                \t\t\t"must": [""");

        Iterator<Map.Entry<String, List<String>>> keyIterator = params.entrySet().iterator();
        while (keyIterator.hasNext()) {
            Map.Entry<String, List<String>> map = keyIterator.next();
            List<String> values = map.getValue();
            // Check whether value(s) provided
            if (!values.isEmpty()) {
                query.append(" {")
                        .append("\n")
                        .append("\t\t\t\t\"terms\" : {")
                        .append("\n")
                        .append("          \"")
                        .append(map.getKey())
                        .append("\": [ ");
                Iterator<String> valuesIterator = values.iterator();
                while (valuesIterator.hasNext()) {
                    query.append("\"")
                            .append(valuesIterator.next())
                            .append("\"");
                    if (valuesIterator.hasNext()) {
                        query.append(", ");
                    }
                }
                query.append(" ]")
                        .append("\n")
                        .append("\t\t\t\t}")
                        .append("\n")
                        .append("\t\t\t}");
                if (keyIterator.hasNext()) {
                    query.append(",");
                } else {
                    query.append(" ]\n");
                }
            } else {
                log.error("Bad request: Value(s) is empty");
                throw new IllegalArgumentException("Bad request: Value(s) is empty");
            }
        }
        query.append("""
                        }
                    }
                }
                """);

        Request request = new Request(
                "GET",
                "/employees/_search"
        );
        request.setJsonEntity(query.toString());

        Response response = restClient.performRequest(request);
        String responseBody = getResponseBody(response);
        return getEmployeesFromResponse(responseBody);
    }

    @Override
    public String aggregate(Map<String, String> params) throws IOException {
        String aggField = params.get("agg_field");
        String metricType = params.get("metric_type");
        String metricField = params.get("metric_field");
        String sortOrder = params.get("sort_order");
        String query = String.format("""
                {
                    "size": 0,
                    "aggs": {
                        "%s": {
                            "terms": {\s
                                "field": "%s.keyword",
                                "order": {
                                    "rating_stats.%s": "%s"
                                }
                            },
                            "aggs": {
                                "rating_stats": {\s
                                    "stats": {\s
                                        "field": "%s"\s
                                    }\s
                                }
                            }
                        }
                    }
                }
                """, aggField, aggField, metricType, sortOrder, metricField);

        Request request = new Request(
                "POST",
                "/employees/_search"
        );
        request.setJsonEntity(query);

        Response response = restClient.performRequest(request);
        String responseBody = getResponseBody(response);

        return mapper.readTree(responseBody)
                .findValues("buckets")
                .toString();
    }

    private Optional<Employee> getEmployeeFromResponse(String responseBody) throws JsonProcessingException {
        JsonNode node = mapper.readTree(responseBody)
                .findPath("_source");

        return getEmployeeFromNode(node);
    }

    private Optional<Employee> getEmployeeFromNode(JsonNode node) {
        ObjectReader reader = mapper.readerFor(new TypeReference<Employee>() {
        });

        try {
            Employee employee = reader.readValue(node);
            return Optional.of(employee);
        } catch (IOException e) {
            log.error("Node can't be parsed to Employee.class : " + node.asText());
            return Optional.empty();
        }
    }

    private List<Employee> getEmployeesFromResponse(String responseBody) throws IOException {
        List<JsonNode> nodes = mapper.readTree(responseBody)
                .findValue("hits")
                .findPath("hits")
                .findValues("_source");

        return nodes.stream()
                .map(this::getEmployeeFromNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }
}
