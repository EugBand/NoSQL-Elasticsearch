package com.epam.elasticsearch.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;

import java.time.LocalDate;
import java.util.List;

public record Employee(String name,
                       @JsonDeserialize(using = LocalDateDeserializer.class)
                       @JsonSerialize(using = LocalDateSerializer.class)
                       LocalDate dob,
                       Address address,
                       String email,
                       List<String> skills,
                       Integer experience,
                       Float rating,
                       String description,
                       Boolean verified,
                       Integer salary) {
}
