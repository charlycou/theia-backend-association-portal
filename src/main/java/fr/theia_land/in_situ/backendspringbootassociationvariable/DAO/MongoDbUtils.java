/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.DAO;

import fr.theia_land.in_situ.backendspringbootassociationvariable.CustomConfig.GenericAggregationOperation;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ObservedProperty;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ProducerStat;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.TheiaVariable;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import org.bson.Document;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author coussotc
 */
@Component
public class MongoDbUtils {

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * For each producer, query the number producer variable associated to theia variable and the number of diffrenet
     * producer variable. In order to print the state of association work for each producer.
     *
     * @return List of ProducerStat - one for each prodcuer
     */
    public List<ProducerStat> setProducerStats() {
        /**
         * Option to be used in case the group operation take too much RAM
         */
        AggregationOptions options = AggregationOptions.builder().allowDiskUse(true).build();

        /**
         * Group operation to get the number of producerVariable per producer in collection "observations"
         */
        ProjectionOperation po1 = Aggregation.project().and("producer.producerId").as("producerId")
                .and(ArrayOperators.Filter.filter("observation.observedProperty.name").as("item").by(ComparisonOperators.Eq.valueOf("item.lang").equalToValue("en"))).as("producerVariableName")
                .and("observation.observedProperty.unit").as("unit")
                .and("observation.observedProperty.theiaCategories").as("theiaCategories");
        GroupOperation go1 = Aggregation.group("producerId", "producerVariableName", "unit", "theiaCategories");
        GroupOperation go2 = Aggregation.group("producerId").count().as("variableCount");
        List<Map> numberOfProducerVariables = mongoTemplate.aggregate(Aggregation.newAggregation(po1, go1, go2).withOptions(options), "observations", Map.class)
                .getMappedResults();

        /**
         * Group operation to get the number of variable associated per producer in collection "observations"
         */
        MatchOperation m1 = Aggregation.match(Criteria.where("observation.observedProperty.theiaVariable").exists(true));
        List<Map> numberOfAssociatedProducerVariables = mongoTemplate.aggregate(Aggregation.newAggregation(m1, po1, go1, go2).withOptions(options), "observations", Map.class)
                .getMappedResults();

        /**
         * Create the response object containing the stats for each producer
         */
        List<ProducerStat> producerStats = new ArrayList<>();
        numberOfProducerVariables.forEach(item -> {
            ProducerStat producerStat = new ProducerStat();
            producerStat.setName(item.get("_id").toString());
            producerStat.setTotal((Integer) item.get("variableCount"));

            List<Map> tmp = numberOfAssociatedProducerVariables.stream()
                    .filter((t) -> {
                        Map associatedProducerVariable = (Map) t;
                        return associatedProducerVariable.get("_id").toString().equals(item.get("_id").toString());
                    }).collect(Collectors.toList());
            if (!tmp.isEmpty()) {
                producerStat.setAssociated((Integer) tmp.get(0).get("variableCount"));
            } else {
                producerStat.setAssociated(0);
            }
            producerStats.add(producerStat);
        });
        return producerStats;
    }

    /**
     * For a given producer, query the non-associated producer variable name and the associated producer variable name.
     * A producer variables are concidered different if for a same 'name' they have different 'unit' or
     * 'theiaCateogries'
     *
     * @param producerId String value of the producer ID
     * @return List of List of ObservedProperty.class - Containing
     * "producerVariableName","unit","theiaCategories","theiaVariable"
     */
    public List<List<ObservedProperty>> getDifferentProducerVariableSorted(String producerId) {
        /**
         * Match the producer Id
         */
        MatchOperation m1 = Aggregation.match(where("producer.producerId").is(producerId));
        /**
         * Match the producer variable already associated
         */
        MatchOperation m2 = Aggregation.match(where("observation.observedProperty.theiaVariable").exists(true));
        /**
         * Match the producer variable not associated yet
         */
        MatchOperation m3 = Aggregation.match(where("observation.observedProperty.theiaVariable").exists(false));
        /**
         * Group the same producer variable. Producer variables are concidered different if for a same 'name' they have
         * different 'unit' or 'theiaCateogries'
         */
        ProjectionOperation p1 = Aggregation.project().and("observation.observedProperty.name").as("name")
                .and("observation.observedProperty.unit").as("unit")
                .and("observation.observedProperty.theiaCategories").as("theiaCategories")
                .and("observation.observedProperty.theiaVariable").as("theiaVariable");
        GroupOperation g1 = Aggregation.group("name", "unit", "theiaCategories", "theiaVariable");
        ReplaceRootOperation r1 = Aggregation.replaceRoot("_id");
        /**
         * Sort by alphabetical order
         */
        SortOperation s1 = Aggregation.sort(Sort.Direction.ASC, "name.0.text");

        /**
         * Execute the aggregation pipeline
         */
        List<List<ObservedProperty>> response = new ArrayList<>();
        response.add(mongoTemplate.aggregate(Aggregation.newAggregation(m1, m2, p1, g1, r1, s1), "observations", ObservedProperty.class)
                .getMappedResults());
        response.add(mongoTemplate.aggregate(Aggregation.newAggregation(m1, m3, p1, g1, r1, s1), "observations", ObservedProperty.class)
                .getMappedResults());
        return response;
    }

    /**
     * Find the Theia Varaiable already associated to one or several cateogories. In order to suggest Theia variable to
     * be associated with for a given producer variable.
     *
     * @param categories List of category uri ex: "["https://w3id.org/ozcar-theia/atmosphericRadiation"]"
     * @return List of Document containing the theia variables
     */
    public List<TheiaVariable> getVariablesAlreadyAssociatedToCategories(
            @ApiParam(required = true,
                    value = "Example (quotes inside brackets can be badly escaped by UI...):\n [\"https://w3id.org/ozcar-theia/atmosphericRadiation\"]",
                    examples = @Example(value = {
                            @ExampleProperty(value = "[\"https://w3id.org/ozcar-theia/atmosphericRadiation\"]")
                    }))
            @RequestBody List<String> categories) {
        List<Criteria> orCriteriasList = new ArrayList<>();
        for (String category : categories) {
            orCriteriasList.add(Criteria.where("observation.observedProperty.theiaCategories").is(category));
        }
        Criteria[] orCriterias = new Criteria[orCriteriasList.size()];
        for (int i = 0; i < orCriteriasList.size(); i++) {
            orCriterias[i] = orCriteriasList.get(i);
        }

        MatchOperation m1 = Aggregation.match(where("observation.observedProperty.theiaVariable").exists(true));
        UnwindOperation u1 = Aggregation.unwind("observation.observedProperty.theiaCategories");
        MatchOperation m2 = Aggregation.match(new Criteria().orOperator(orCriterias));
        ProjectionOperation p1 = Aggregation.project().and("observation.observedProperty.theiaVariable").as("theiaVariable");
        GroupOperation g1 = Aggregation.group("theiaVariable");
        ReplaceRootOperation r1 = Aggregation.replaceRoot("_id");

        return mongoTemplate.aggregate(Aggregation.newAggregation(m1, u1, m2, p1, g1, r1), "observations", TheiaVariable.class)
                .getMappedResults();
    }

    /**
     * Find each observation that have the uri in observation.observedProperty.theiaVariable.uri. If the
     * observation.observedProperty.theiaVariable.prefLabel.*.text is not equal to prefLabel for a given language the
     * prefLabel field is updated and the corresponding document in the "variableAssociations" collection is updated.
     *
     * @param uri       uri of the theiaVariable
     * @param prefLabel prefLabel of the theiaVariable for a given laguage
     * @param lang      language - sould always be "en"
     */
    public void checkPrefLabelForOtherObservations(String uri, String prefLabel, String lang) {
        /**
         * Aggregation operations finding all the observations with theiaVariable prefLabel different to prefLabel
         * method param. The aggregation pipeline return a list of Document containing the documentId of the
         * observations and the index corresponding to the lang parameter in the prefLabel array - example of prefLabel
         * (prefLabel: [{"lang":"en", "text": "Air pressure"}, {"lang":"fr", "text": "Pression de l'air"}]
         */
        MatchOperation m1 = Aggregation.match(where("observation.observedProperty.theiaVariable.uri").is(uri));
        MatchOperation m2 = Aggregation.match(new Criteria().andOperator(
                where("observation.observedProperty.theiaVariable.prefLabel.lang").is(lang),
                where("observation.observedProperty.theiaVariable.prefLabel.text").ne(prefLabel)
        ));
        ProjectionOperation p1 = Aggregation.project("documentId").and(ArrayOperators.IndexOfArray.arrayOf("observation.observedProperty.theiaVariable.prefLabel.lang").indexOf(lang)).as("index");
        List<Document> responses = mongoTemplate.aggregate(Aggregation.newAggregation(m1, m2, p1), "observations", Document.class).getMappedResults();

        /**
         * If the aggregation pipeline find observation satisflying the match operation, those observations are updated
         * with the new prefLabel for the given language
         */
        if (responses.size() > 0) {
            for (Document obs : responses) {
                Query query = Query.query(new Criteria("documentId").is(obs.getString("documentId")));
                Update update = Update.update("observation.observedProperty.theiaVariable.prefLabel." + obs.getInteger("index") + ".text", prefLabel);
                mongoTemplate.updateFirst(query, update, "observations");
            }
            /**
             * The "variableAssociations" document saving association of the "observations" collection matching document
             * are also updated.
             */
            MatchOperation m3 = Aggregation.match(new Criteria("theiaVariable.uri").is(uri));
            ProjectionOperation p2 = Aggregation.project("_id").and(ArrayOperators.IndexOfArray.arrayOf("theiaVariable.prefLabel.lang").indexOf(lang)).as("index");
            List<Document> responsesVariableAssociations = mongoTemplate.aggregate(Aggregation.newAggregation(m3, p2), "variableAssociations", Document.class).getMappedResults();
            for (Document asso : responsesVariableAssociations) {

                Query query = Query.query(new Criteria("_id").is(asso.getObjectId("_id")));
                Update update = Update.update("theiaVariable.prefLabel." + asso.getInteger("index") + ".text", prefLabel);
                mongoTemplate.updateFirst(query, update, "variableAssociations");
            }
        }
    }

    /**
     * Delete all document in a MongoDB collection for a given producerId
     *
     * @param producerId     String - the producerId used to identify the Document to be removed
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
     * @param inputCollectionName  name of the input collection
     * @param outputCollectionName name of the output collection
     * @param producerId           producerId
     */
    public void groupDocumentsByLocationAndInsertInOtherCollection(
            String inputCollectionName, String outputCollectionName, String producerId) {

        //deleteDocumentsUsingProducerId(producerId, outputCollectionName);
        mongoTemplate.remove(Query.query(where("producerId").is(producerId)), outputCollectionName);
        MatchOperation m1 = Aggregation.match(where("producer.producerId").is(producerId));

        UnwindOperation u1 = Aggregation.unwind("observations");

        ProjectionOperation p1 = Aggregation.project()
                .and("producer.producerId").as("producerId")
                .and("dataset.datasetId").as("datasetId")
                .and("observations.featureOfInterest.samplingFeature").as("samplingFeature")
                .and("observations.observationId").as("observationId");

        //UnwindOperation u1 = Aggregation.unwind("documentIds");
        GroupOperation g1 = group(
                "producerId",
                "datasetId",
                "samplingFeature"
        ).push("observationId").as("observationIds");

        ProjectionOperation p2 = Aggregation.project("observationIds")
                .and("_id.producerId").as("producerId")
                .and("_id.samplingFeature").as("samplingFeature")
                .andExclude("_id");

        //OutOperation o1 = Aggregation.out(outputCollectionName);
        AggregationOptions options = AggregationOptions.builder().allowDiskUse(true).build();
        //List<AggregationOperation> aggs = Arrays.asList(m1, p1, u1, g1, p2);
        List<AggregationOperation> aggs = Arrays.asList(m1, u1, p1, g1, p2);
        List<Document> docs = mongoTemplate.aggregate(Aggregation.newAggregation(aggs).withOptions(options), inputCollectionName, Document.class).getMappedResults();
        mongoTemplate.insert(docs, outputCollectionName);

    }

    /**
     * Group the Document of a collection by variable at a given location for a given dataset. The resulting document
     * are inserted in a new collection
     *
     * @param inputCollectionName  String - the collection name from which document are grouped
     * @param outputCollectionName String - the collection name used to store resulting document of the grouping
     *                             operation
     * @param producerId           String - the producerId of the prodcuer inserting the Document. it is used to remove the
     *                             relative document previously inserted before update.
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
                //.and("documentId").as("observation.documentId")
                .and("observation.observationId").as("observation.observationId")
                .and(DateOperators.DateFromString.fromStringOf("observation.temporalExtent.dateBeg")).as("observation.temporalExtent.dateBeg")
                .and(DateOperators.DateFromString.fromStringOf("observation.temporalExtent.dateEnd")).as("observation.temporalExtent.dateEnd")
                .and("observation.observedProperty").as("observation.observedProperty")
                .and("observation.featureOfInterest").as("observation.featureOfInterest")
                .and("producer").as("producer")
                .and("dataset").as("dataset")
                .and("observation.featureOfInterest.samplingFeature").as("samplingFeature")
                //                .and("observation.temporalExtent").as("temporalExtent")
                //                .and("observation.observedProperty").as("observedProperty")
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

        String groupJson = "{\n"
                + "	\"_id\": {\n"
                + "		\"producerId\": \"$producer.producerId\",\n"
                + "		\"datasetId\": \"$dataset.datasetId\",\n"
                + "		\"theiaVariableUri\": {\n"
                + "			\"$cond\": [{\n"
                + "				\"$not\": [\"$uri\"]\n"
                + "			}, null, \"$uri\"]\n"
                + "		},\n"
                + "		\"producerVariableName\": {\n"
                + "			\"$cond\": [{\n"
                + "				\"$not\": [\"$uri\"]\n"
                + "			}, \"$producerVariableName.text\", null]\n"
                + "		},\n"
                + "		\"samplingFeature\": \"$samplingFeature\"\n"
                + "	},\n"
                + "	\"producer\": {\n"
                + "		\"$first\": \"$producer\"\n"
                + "	},\n"
                + "	\"dataset\": {\n"
                + "		\"$first\": \"$dataset\"\n"
                + "	},\n"
                + "	\"observations\": {\n"
                + "		\"$push\": \"$observation\"\n"
                + "	}\n"
                + "}";
        AggregationOperation g1 = new GenericAggregationOperation("$group", groupJson);

        /**
         * Project the result of the group operation before to be inserted in collection
         */
        String projectJson = "{\n"
                + "	\"producer.producerId\": 1,\n"
                + "	\"producer.name\": 1,\n"
                + "	\"producer.title\": 1,\n"
                + "	\"producer.fundings\": 1,\n"
                + "	\"dataset.datasetId\": 1,\n"
                + "	\"dataset.metadata.portalSearchCriteria\": 1,\n"
                + "	\"dataset.metadata.title\": 1,\n"
                + "	\"dataset.metadata.keywords\": 1,\n"
                + "	\"dataset.metadata.description\": 1,\n"
                + "     \"observations\": 1,\n"
                + "	\"_id\": 0\n"
                + "}";
        AggregationOperation p2 = new GenericAggregationOperation("$project", projectJson);

        /**
         * Insert the result of the aggregation pipeline in the collection
         */
        AggregationOptions options = AggregationOptions.builder().allowDiskUse(true).build();
        List<Document> docs = mongoTemplate.aggregate(Aggregation.newAggregation(m1, p1, u1, g1, p2).withOptions(options), inputCollectionName, Document.class).getMappedResults();
        mongoTemplate.insert(docs, outputCollectionName);

    }

    /**
     * update or insert or remove a new association in "variableAssociations" collection
     *
     * @param outputCollectionName name of the collection ("variableAssociations")
     * @param producerId           id of the producer
     * @param asso                 JSONObject describing the association. if "asso.prefLabel" is empty and "asso.uri" is null, the
     *                             association is removed from the collection
     */
    public void updateOneVariableAssociation(String outputCollectionName, String producerId, JSONObject asso) {

        /**
         * Storing matching value into lists to find corresponding observation
         */
        List<String> variableNames = new ArrayList<>();
        // List<String> unitName = new ArrayList<>();
        List<String> theiaCategoryUri = new ArrayList<>();
        asso.getJSONObject("variable").getJSONArray("name").forEach(item -> {
            JSONObject tmp = (JSONObject) item;
            if ("en".equals(tmp.getString("lang"))) {
                variableNames.add(tmp.getString("text"));
            }
        });

        asso.getJSONObject("variable").getJSONArray("theiaCategories").forEach(item -> {
            theiaCategoryUri.add((String) item);
        });
        String producerVariableNameEn = variableNames.get(0);

        Query query = Query.query(Criteria.where("producerId").is(producerId)
                .and("producerVariableNameEn").is(producerVariableNameEn)
                .and("theiaCategories").in(theiaCategoryUri));
        if (asso.getJSONArray("prefLabel").isEmpty() && asso.isNull("uri")) {
            mongoTemplate.remove(query, outputCollectionName);
        } else {
            Document theiaVariablePrefLabel = new Document("lang", "en").append("text", asso.getJSONArray("prefLabel").getJSONObject(0).getString("text"));
            Update update = Update.update("isActive", true)
                    .set("theiaVariable", new Document("uri", asso.getString("uri"))
                            .append("prefLabel", Arrays.asList(theiaVariablePrefLabel)))
                    .set("producerId", producerId)
                    .set("producerVariableNameEn", producerVariableNameEn)
                    .set("theiaCategories", theiaCategoryUri);
            mongoTemplate.upsert(query, update, outputCollectionName);
        }
    }
}
