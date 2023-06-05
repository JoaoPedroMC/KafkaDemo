package com.kafkaproject.model

import com.fasterxml.jackson.annotation.JsonProperty

data class Product(
        @JsonProperty("name")
        val name: String,
        @JsonProperty("description")
        val description: String?
)