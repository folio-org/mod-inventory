package org.folio.inventory.domain.items;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * DTO for fetching inventory items record by POST request
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "offset",
    "limit",
    "query"
})
public class CQLQueryRequestDto {

    /**
     * Skip over a number of elements by specifying an offset value for the query
     * 
     */
    @JsonProperty("offset")
    @JsonPropertyDescription("Skip over a number of elements by specifying an offset value for the query")
    @DecimalMin("0")
    @DecimalMax("2147483647")
    private Integer offset = 0;
    /**
     * Limit the number of elements returned in the response
     * 
     */
    @JsonProperty("limit")
    @JsonPropertyDescription("Limit the number of elements returned in the response")
    @DecimalMin("0")
    @DecimalMax("2147483647")
    private Integer limit = 10;
    /**
     * A query expressed as a CQL string
     * 
     */
    @JsonProperty("query")
    @JsonPropertyDescription("A query expressed as a CQL string")
    private String query;

    /**
     * Skip over a number of elements by specifying an offset value for the query
     * 
     */
    @JsonProperty("offset")
    public Integer getOffset() {
        return offset;
    }

    /**
     * Skip over a number of elements by specifying an offset value for the query
     * 
     */
    @JsonProperty("offset")
    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    /**
     * Limit the number of elements returned in the response
     * 
     */
    @JsonProperty("limit")
    public Integer getLimit() {
        return limit;
    }

    /**
     * Limit the number of elements returned in the response
     * 
     */
    @JsonProperty("limit")
    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    /**
     * A query expressed as a CQL string
     * 
     */
    @JsonProperty("query")
    public String getQuery() {
        return query;
    }

    /**
     * A query expressed as a CQL string
     * 
     */
    @JsonProperty("query")
    public void setQuery(String query) {
        this.query = query;
    }

    public CQLQueryRequestDto withQuery(String query) {
        this.query = query;
        return this;
    }
}