/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.Utils;

import com.mongodb.client.result.UpdateResult;
import fr.theia_land.in_situ.backendspringbootassociationvariable.CustomConfig.GenericAggregationOperation;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators;
import org.springframework.data.mongodb.core.aggregation.ComparisonOperators;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Cond;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.OutOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

/**
 *
 * @author coussotc
 */
@Component
public class MongoDbUtils {

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * Delete all document in a MongoDB collection for a given producerId
     *
     * @param producerId String - the producerId used to identify the Document to be removed
     * @param collectionName
     */
    public void deleteDocumentsUsingProducerId(String producerId, String collectionName) {
        Query query = Query.query(where("producer.producerId").is(producerId));
        mongoTemplate.remove(query, collectionName);
    }

    /**
     * Group observations from a collection using producerId, datasetId, and location. Store the result in a new
     * collection
     *
     * @param inputCollectionName name of the input collection
     * @param outputCollectionName name of the output collection
     * @param producerId producerId
     */
    public void groupDocumentsByLocationAndInsertInOtherCollection(
            String inputCollectionName, String outputCollectionName, String producerId) {

        deleteDocumentsUsingProducerId(producerId, outputCollectionName);

        MatchOperation m1 = Aggregation.match(where("producer.producerId").is(producerId));

        ProjectionOperation p1 = Aggregation.project()
                .and("producer.producerId").as("producerId")
                .and("dataset.datasetId").as("datasetId")
                .and("observation.featureOfInterest.samplingFeature").as("samplingFeature")
                .and("documentId").as("documentId");

        UnwindOperation u1 = Aggregation.unwind("documentId");

        GroupOperation g1 = group(
                "producerId",
                "datasetId",
                "samplingFeature"
        ).push("documentId").as("documentId");

        ProjectionOperation p2 = Aggregation.project("documentId")
                .and("_id.producerId").as("producerId")
                .and("_id.samplingFeature").as("samplingFeature")
                .andExclude("_id");

        //OutOperation o1 = Aggregation.out(outputCollectionName);

        AggregationOptions options = AggregationOptions.builder().allowDiskUse(true).build();
        List<AggregationOperation> aggs = Arrays.asList(m1, p1, u1, g1, p2);
        List<Document> documents = mongoTemplate.aggregate(Aggregation.newAggregation(aggs).withOptions(options), inputCollectionName, Document.class)
                .getMappedResults();
        mongoTemplate.insert(documents, outputCollectionName);

    }

    /**
     * Group the Document of a collection by variable at a given location for a given dataset. The resulting document
     * are inserted in a new collection
     *
     * @param inputCollectionName String - the collection name from which document are grouped
     * @param outputCollectionName String - the collection name used to store resulting document of the grouping
     * operation
     * @param producerId String - the producerId of the prodcuer inserting the Document. it is used to remove the
     * relative document previously inserted before update.
     */
    public void groupDocumentsByVariableAtGivenLocationAndInsertInOtherCollection(
            String inputCollectionName, String outputCollectionName, String producerId) {

        deleteDocumentsUsingProducerId(producerId, outputCollectionName);

        /**
         * All observation matching the producer ID
         */
        MatchOperation m1 = Aggregation.match(where("producer.producerId").is(producerId));

        /**
         * Project the fields used in group operation
         */
        ProjectionOperation p1 = Aggregation.project()
                .and("documentId").as("documentId")
                .and("producer").as("producer")
                .and("dataset").as("dataset")
                .and("observation.featureOfInterest.samplingFeature").as("samplingFeature")
                .and("observation.observedProperty").as("observedProperty")
                .and("observation.observedProperty.theiaVariable.uri").as("uri")
                .and(ArrayOperators.Filter.filter("observation.observedProperty.name").as("item").by(ComparisonOperators.Eq.valueOf("item.lang").equalToValue("en"))).as("producerVariableName");

        /**
         * Unwind the producerVariableName array that contain only one element
         */
        UnwindOperation u1 = Aggregation.unwind("producerVariableName");
        /**
         * Group all the observation by producer, dataset, sampling feature and theiaVariable or producerVariableName
         * when theiaVariable does not exist. The documentId of the observations grouped are push in an array The
         * observedPorperty of the observation grouped are pushed in an array in order to keep the information
         * theiaVariable / theiaCategories for each observation
         *
         * This group operation is complex and not supported by Spring data mongodb. AggregationOperation.class is
         * extended to support Aggregation operation building using Document.class
         */

        String groupJson = "{ '_id' : { \n"
                + "                        'producerId' : '$producer.producerId' ,\n"
                + "                        'datasetId' : '$dataset.datasetId' ,\n"
                + "                        'theiaVariableUri' : {$cond:[{$not: ['$uri']},null, '$uri']},\n"
                + "                        'producerVariableName' : {$cond:[{$not: ['$uri']},'$producerVariableName.text', null]},\n"
                + "                        'samplingFeature' : '$samplingFeature'}\n"
                + "                , 'documentId' : { '$push' : '$documentId'}\n"
                + "                , 'observedProperty' : { '$push' : '$observedProperty'}\n"
                + "                , 'samplingFeature' : { '$first' : '$samplingFeature'}\n"
                + "                , 'producer' : { '$first' : '$producer'}\n"
                + "                , 'dataset' : { '$first' : '$dataset'}}";
        AggregationOperation g1 = new GenericAggregationOperation("$group", groupJson);

        /**
         * Project the result of the group operation before to be inserted in collection
         */
        ProjectionOperation p2 = Aggregation.project()
                .and("documentId").as("documentId")
                .and("producer.producerId").as("producer.producerId")
                .and("producer.name").as("producer.name")
                .and("producer.title").as("producer.title")
                .and("producer.fundings").as("producer.fundings")
                .and("dataset.datasetId").as("dataset.datasetId")
                .and("dataset.metadata.title").as("dataset.metadata.title")
                .and("dataset.metadata.portalSearchCriteria").as("dataset.metadata.portalSearchCriteria")
                .and("dataset.metadata.keywords").as("dataset.metadata.keywords")
                .and("dataset.metadata.description").as("dataset.metadata.description")
                .and("observedProperty").as("observation.observedProperties")
                .and("samplingFeature").as("observation.featureOfInterest.samplingFeature")
                .andExclude("_id");

        /**
         * Insert the result of the aggregation pipeline in the collection
         */
        //OutOperation o1 = Aggregation.out(outputCollectionName);

        AggregationOptions options = AggregationOptions.builder().allowDiskUse(true).build();
        List<AggregationOperation> aggs = Arrays.asList(m1, p1, u1, g1, p2);

        List<Document> documents = mongoTemplate.aggregate(Aggregation.newAggregation(aggs).withOptions(options), inputCollectionName, Document.class)
                .getMappedResults();
        mongoTemplate.insert(documents, outputCollectionName);

    }

    /**
     * Store the different variable association made for a given producer in a collection. For a given producer, an
     * associaiton is concidered different if for an identical variable name the categories are different.
     *
     * @param outputCollectionName String - the name of the output collection (variableAssociationss)
     * @param producerId String - the producerId
     */
    public void storeAssociation(String outputCollectionName, String producerId) {
        MatchOperation m1 = Aggregation.match(where("producer.producerId").is(producerId));
        MatchOperation m2 = Aggregation.match(where("observation.observedProperty.theiaVariable").exists(true));
        ProjectionOperation p1 = Aggregation.project()
                .and("producer").as("producer")
                .and("observation.observedProperty.theiaCategories").as("theiaCategories")
                .and("observation.observedProperty.name").as("producerVariableName")
                .and("observation.observedProperty.theiaVariable").as("theiaVariable");

        GroupOperation g1 = Aggregation.group("producer.producerId", "theiaCategories", "theiaVariable.uri")
                .first("theiaVariable").as("theiaVariable")
                .first("producer.producerId").as("producerId")
                .first("producerVariableName").as("producerVariableName")
                .first("theiaCategories").as("theiaCategories")
                .addToSet(true).as("isActive");

        UnwindOperation u1 = Aggregation.unwind("isActive");
        ProjectionOperation p2 = Aggregation.project().andExclude("_id");

        // OutOperation o1 = Aggregation.out(outputCollectionName);
        List<Document> associations = mongoTemplate.aggregate(Aggregation.newAggregation(m1, m2, p1, g1, u1, p2), "observations", Document.class).getMappedResults();
        refreshAssociationSubmited(outputCollectionName, producerId, associations);
    }

    public void refreshAssociationSubmited(String collectionName, String producerId, List<Document> associations) {
        Update up1 = Update.update("isActive", false);
        Query query = Query.query(where("producerId").is(producerId));
        mongoTemplate.updateMulti(query, up1, collectionName);
        mongoTemplate.insert(associations, collectionName);

        GroupOperation g1 = Aggregation.group("producer.producerId", "theiaCategories", "theiaVariable.uri", "producerVariableName")
                .first("theiaVariable").as("theiaVariable")
                .first("producerId").as("producerId")
                .first("theiaCategories").as("theiaCategories")
                .first("producerVariableName").as("producerVariableName")
                .addToSet(true).as("isActive");

        Cond condOperation = ConditionalOperators.when(Criteria.where("isActive").ne(false))
                .then(true)
                .otherwise(false);

        ProjectionOperation p1 = Aggregation.project()
                .and("theiaVariable").as("theiaVariable")
                .and("producerId").as("producerId")
                .and("theiaCategories").as("theiaCategories")
                .and("producerVariableName").as("producerVariableName")
                .andExclude("_id")
                .and(condOperation).as("isActive");

        List<Document> docs = mongoTemplate.aggregate(Aggregation.newAggregation(g1, p1), "variableAssociations", Document.class).getMappedResults();

        mongoTemplate.remove(query, collectionName);
        mongoTemplate.insert(docs, collectionName);

    }


}
