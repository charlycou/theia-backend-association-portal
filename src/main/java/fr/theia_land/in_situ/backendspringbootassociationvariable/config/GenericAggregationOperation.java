/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.CustomConfig;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;

/**
 * @author coussotc
 */
public class GenericAggregationOperation implements AggregationOperation {

    private String operator;
    private Document query;

    /**
     * Default constructor.
     *
     * @param operator MongoDB operator ($group, $sort, $project, etc..)
     * @param query    MongoDB aggregation query step string
     */
    public GenericAggregationOperation(String operator, String query) {
        this(operator, Document.parse(query));
    }

    /**
     * Default constructor.
     *
     * @param operator MongoDB operator ($group, $sort, $project, etc..)
     * @param query    MongoDB aggregation query step DBObject
     */
    public GenericAggregationOperation(String operator, Document query) {
        this.operator = operator;
        this.query = query;
    }

    @Override
    public Document toDocument(AggregationOperationContext context) {
        return new Document(operator, query);
    }

}

