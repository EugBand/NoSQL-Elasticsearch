package com.epam.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import co.elastic.clients.elasticsearch._types.aggregations.AverageAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.HistogramBucket;
import co.elastic.clients.elasticsearch._types.aggregations.MultiBucketBase;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import com.epam.elasticsearch.dto.Employee;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service("api-service")
@RequiredArgsConstructor
@Slf4j
public class EmployeeApiServiceImpl implements EmployeeService {

    private final ElasticsearchClient client;

    @Override
    public List<Employee> getAll() throws IOException {
        SearchResponse<Employee> searchResponse = client.search(s -> s
                        .index("employees"),
                Employee.class);

        return searchResponse.hits().hits().stream()
                .map(Hit::source)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Employee> getById(String id) throws IOException {
        GetResponse<Employee> response = client.get(g -> g
                        .index("employees")
                        .id(id),
                Employee.class);

        if (response.found()) {
            Employee employee = response.source();
            assert employee != null;
            return Optional.of(employee);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void createWithId(Employee employee, String id) throws IOException {
        IndexRequest<Employee> request = IndexRequest.of(i -> i
                .index("employees")
                .id(id)
                .document(employee));

        client.index(request);
    }

    @Override
    public void deleteById(String id) throws IOException {
        DeleteRequest request = DeleteRequest.of(i -> i
                .index("employees")
                .id(id));

        client.delete(request);
    }

    @Override
    public List<Employee> search(MultiValueMap<String, String> params) throws IOException {
        Iterator<Map.Entry<String, List<String>>> keyIterator = params.entrySet().iterator();
        if (!keyIterator.hasNext()) {
            throw new IllegalArgumentException("Bad request");
        }
        Map.Entry<String, List<String>> map = keyIterator.next();
        if (!map.getValue().iterator().hasNext()) {
            throw new IllegalArgumentException("Bad request");
        }
        String value = map.getValue().iterator().next();

        SearchResponse<Employee> response = client.search(s -> s
                        .index("employees")
                        .query(q -> q
                                .term(t -> t
                                        .field(map.getKey())
                                        .value(value))),
                Employee.class);

        TotalHits totalHits = response.hits().total();
        assert totalHits != null;

        return response.hits().hits().stream()
                .map(Hit::source)
                .collect(Collectors.toList());
    }

    @Override
    public String aggregate(Map<String, String> params) throws IOException {
        String aggField = params.get("agg_field");
        String metricType = params.get("metric_type");
        String metricField = params.get("metric_field");

        Map<String, Aggregation> map = new HashMap<>();
        Aggregation subAggregation = new Aggregation.Builder()
                .avg(new AverageAggregation.Builder().field(metricField).build())
                .build();

        Aggregation aggregation = new Aggregation.Builder()
                .terms(new TermsAggregation.Builder().field(aggField + ".keyword").build())
                .aggregations(new HashMap<>() {{
                    put(metricField, subAggregation);
                }}).build();

        map.put(metricField, aggregation);

        SearchRequest request = new SearchRequest.Builder()
                .index("employees")
                .size(0)
                .aggregations(map)
                .build();

        SearchResponse<Void> response = client.search(request, Void.class);

        List<StringTermsBucket> buckets = response.aggregations()
                .get(metricField)
                .sterms()
                .buckets()
                .array();

        return buckets.stream()
                .toList()
                .toString();
    }
}
