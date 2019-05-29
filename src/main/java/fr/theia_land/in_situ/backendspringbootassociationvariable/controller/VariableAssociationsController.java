/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.controller;

import com.mongodb.MongoClient;
import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.TheiaVariableRepository;
import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.Utils.RDFUtils;
import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.VariableAssociationsRepository;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.Entities.TheiaVariableName;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ObservedProperty;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.text.CaseUtils;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.sparql.ARQException;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.OutOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author coussotc
 */
@RestController
@RequestMapping("/association")
//@CrossOrigin(origins = {"http://localhost"})
public class VariableAssociationsController {

    private static final Logger logger = LoggerFactory.getLogger(VariableAssociationsController.class);

    /**
     * Inject the repository to be queried
     */
    @Autowired
    private VariableAssociationsRepository associationsRepository;
    @Autowired
    private TheiaVariableRepository variableRepository;
    //Indicate that mongoTemplate must be injected by Spring IoC
    @Autowired
    private MongoTemplate mongoTemplate;

    @GetMapping("/setProducerStats")
    private List<ProducerStat> setProducerStats() {
        /**
         * Option to be used in case the group operation take too much RAM
         */
        AggregationOptions options = AggregationOptions.builder().allowDiskUse(true).build();

        /**
         * Group operation to get the number of producerVariable per producer in collection "observations"
         */
        ProjectionOperation po1 = Aggregation.project().and("producer.producerId").as("producerId")
                .and("observation.observedProperty.name").as("name")
                .and("observation.observedProperty.unit").as("unit")
                .and("observation.observedProperty.theiaCategories").as("theiaCategories");
//        ProjectionOperation po2 = Aggregation.project().and("observation.observedProperty.name").as("name")
//                .and("observedProperty.unit").as("unit")
//                .and("observedProperty.theiaCategories").as("theiaCategories");
        GroupOperation go1 = Aggregation.group("producerId", "name", "unit", "theiaCategories");
        GroupOperation go2 = Aggregation.group("producerId").count().as("variableCount");
        List<Map> numberOfProducerVariables = mongoTemplate.aggregate(Aggregation.newAggregation(po1, go1, go2).withOptions(options), "observations", Map.class)
                .getMappedResults();

        /**
         * Group operation to get the number of variable associated per producer in collection "variableAssocation"
         */
        GroupOperation go3 = group("producerId").count().as("associatedVariablesCount");
        List<Map> numberOfAssociatedProducerVariables = mongoTemplate.aggregate(Aggregation.newAggregation(go3).withOptions(options), "variableAssociations", Map.class)
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
                        Map AssociatedProducerVariable = (Map) t;
                        return AssociatedProducerVariable.get("producerId").toString().equals(item.get("_id").toString());
                    }).collect(Collectors.toList());
            if (!tmp.isEmpty()) {
                producerStat.setAssociated((Integer) tmp.get(0).get("associatedVariablesCount"));
            } else {
                producerStat.setAssociated(0);
            }
            producerStats.add(producerStat);
        });
        return producerStats;
    }

    private class ProducerStat {

        private String name;
        private int associated;
        private int total;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAssociated() {
            return associated;
        }

        public void setAssociated(int associated) {
            this.associated = associated;
        }

        public int getTotal() {
            return total;
        }

        public void setTotal(int total) {
            this.total = total;
        }
    }

    @GetMapping("/setProducerVariables")
    private List<List<ObservedProperty>> setProducerVariables(@RequestParam("producerId") String producerId) {
        MatchOperation m1 = Aggregation.match(where("producer.producerId").is(producerId));
        MatchOperation m2 = Aggregation.match(where("observation.observedProperty.theiaVariable").exists(true));
        MatchOperation m3 = Aggregation.match(where("observation.observedProperty.theiaVariable").exists(false));
        ProjectionOperation p1 = Aggregation.project().and("observation.observedProperty.name").as("name")
                .and("observation.observedProperty.unit").as("unit")
                .and("observation.observedProperty.theiaCategories").as("theiaCategories");
        GroupOperation g1 = Aggregation.group("name", "unit", "theiaCategories");
        ReplaceRootOperation r1 = Aggregation.replaceRoot("_id");
        SortOperation s1 = Aggregation.sort(Sort.Direction.ASC, "name.0.text");

        List<List<ObservedProperty>> response = new ArrayList<>();
        response.add(mongoTemplate.aggregate(Aggregation.newAggregation(m1, m2, p1, g1, s1), "observations", ObservedProperty.class)
                .getMappedResults());
        response.add(mongoTemplate.aggregate(Aggregation.newAggregation(m1, m3, p1, g1, r1, s1), "observations", ObservedProperty.class)
                .getMappedResults());
        return response;
    }

    @PostMapping("/setVariablesAlreadyAssociatedToCategories")
    private List<TheiaVariableName> setVariablesAlreadyAssociatedToCategories(@RequestBody String categories) {
        JSONArray json = new JSONArray(categories);
        List<String> uriCategories = (List<String>) (List<?>) json.toList();
        List<Criteria> orCriteriasList = new ArrayList<>();
        for (String category : uriCategories) {
            orCriteriasList.add(Criteria.where("observation.observedProperty.theiaCategories").is(category));
        }
        Criteria[] orCriterias = new Criteria[orCriteriasList.size()];
        for (int i = 0; i < orCriteriasList.size(); i++) {
            orCriterias[i] = orCriteriasList.get(i);
        }

        MatchOperation m1 = Aggregation.match(where("observation.observedProperty.theiaVariable").exists(true));
        UnwindOperation u1 = Aggregation.unwind("observation.observedProperty.theiaCategories");
        MatchOperation m2 = Aggregation.match(new Criteria().orOperator(orCriterias));
        GroupOperation g1 = Aggregation.group("observation.observedProperty.theiaVariable");
        ReplaceRootOperation r1 = Aggregation.replaceRoot("_id");

        return mongoTemplate.aggregate(Aggregation.newAggregation(m1, u1, m2), "observations", TheiaVariableName.class)
                .getMappedResults();
    }

    @PostMapping("/createANewTheiaVariable")
    private ResponseEntity<Map<String, String>> createANewTheiaVariable(@RequestBody String info) {
        Map<String, String> response = new HashMap();
        JSONObject json = new JSONObject(info);
        String prefLabel = json.getString("prefLabel");
        List<String> categories = (List<String>) (List<?>) (json.getJSONArray("broaders").toList());

        String uri = Normalizer.normalize(prefLabel, java.text.Normalizer.Form.NFD)
                .replaceAll("[^\\p{ASCII}]", "")
                .replaceAll("[^a-zA-Z0-9 ]", "");
        uri = "https://w3id.org/ozcar-theia/variables/" + CaseUtils.toCamelCase(uri, false, new char[]{' '});

        try (RDFConnection conn = RDFConnectionFactory.connect("http://in-situ.theia-land.fr:3030/theia_vocabulary/")) {

            /**
             * Created to avoid circular reference (broader and narrower), if the uri is already used in categories
             * "Variable" is added at the end of the uri.
             */
            if (RDFUtils.existSkosVariable(uri)) {
                String uriTmp = new String(uri);
                uri = uriTmp + "Variable";
            }
            try {
                RDFUtils.insertSkosVariable(uri, prefLabel, categories);
            } catch (ARQException ex) {
                logger.error(ex.getMessage());
                response.put("error", ex.getMessage());
                return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
                response.put("error", ex.getMessage());
                return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
            }
        }

        response.put("uri", uri);
        response.put("prefLabel", prefLabel);
        return new ResponseEntity<>(response, HttpStatus.ACCEPTED);
    }

    @PostMapping("/submitAssociation")
    private boolean submitAssociation(@RequestBody String associationInfo) {
        JSONObject json = new JSONObject(associationInfo);
        String producerId = json.getString("producerId");

        /**
         * For each assocation, Aggregation operations finding all the observations having
         * "observation.observedProperty.name" equal to the association variable name, and
         * "observation.observedProperty.theiaCategories" being a subset of the assoation variable theia categories
         */
        json.getJSONArray("associations").forEach(association -> {
            JSONObject asso = (JSONObject) association;

            /**
             * Storing matching value into lists to find corresponding observation
             */
            List<String> variableName = new ArrayList<>();
            List<String> unitName = new ArrayList<>();
            List<String> theiaCategoryUri = new ArrayList<>();
            asso.getJSONObject("variable").getJSONArray("name").forEach(item -> {
                JSONObject tmp = (JSONObject) item;
                variableName.add(tmp.getString("text"));
            });
            asso.getJSONObject("variable").getJSONArray("unit").forEach(item -> {
                JSONObject tmp = (JSONObject) item;
                unitName.add(tmp.getString("text"));
            });
            asso.getJSONObject("variable").getJSONArray("theiaCategories").forEach(item -> {
                theiaCategoryUri.add((String) item);
            });
            /**
             * Set match operation for producerId, variable name, unit , theia categories
             */
            MatchOperation m1 = Aggregation.match(where("producer.producerId").is(producerId));
            MatchOperation m2 = Aggregation.match(where("observation.observedProperty.name.text").in(variableName));
            MatchOperation m3 = Aggregation.match(where("observation.observedProperty.unit.text").in(unitName));
            MatchOperation m4 = Aggregation.match(where("observation.observedProperty.theiaCategories").in(theiaCategoryUri));
            List<Document> documents = mongoTemplate.aggregate(Aggregation.newAggregation(m1, m2, m3, m4), "observations", Document.class).getMappedResults();

            /**
             * Each observation is updated by adding "observation.observaProperty.theiaVariables" object
             */
            for (Document doc : documents) {
                Document theiaVariable = new Document("lang", "en").append("text", asso.getString("prefLabel"));

                Query query = Query.query(new Criteria("documentId").is(doc.getString("documentId")));
                Update update = Update.update("observation.observedProperty.theiaVariable",
                        new Document("uri", asso.getString("uri"))
                                .append("prefLabel", Arrays.asList(theiaVariable)));
                mongoTemplate.updateFirst(query, update, "observations");
            }
            checkPrefLabelForOtherObservations(asso.getString("uri"), asso.getString("prefLabel"), "en");
        });
        groupDocumentsByVariableAtGivenLocationAndInsertInOtherCollection("observations", "observationsLite", producerId);
        groupDocumentsByLocationAndInsertInOtherCollection("observationsLite", "mapItems", producerId);
        return true;
    }

    /**
     * Find each observation that have the uri in observation.observedProperty.theiaVariable.uri. If the
     * observation.observedProperty.theiaVariable.prefLabel.*.text is not equal to prefLabel for a given language the
     * field is updated.
     *
     * @param uri uri of the theiaVariable
     * @param prefLabel prefLabel of the theiaVariable for a given laguage
     * @param lang language - sould always be "en"
     */
    private void checkPrefLabelForOtherObservations(String uri, String prefLabel, String lang) {
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
        ProjectionOperation p1 = Aggregation.project("documentId").and(ArrayOperators.IndexOfArray.arrayOf("observation.observedProperty.theiaVariable.prefLabel.lang").indexOf("en")).as("index");
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
        }
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
    private void groupDocumentsByVariableAtGivenLocationAndInsertInOtherCollection(
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
                .and("observation.observedProperty.theiaCategories").as("theiaCategories");

        /**
         * Group all the observation by producer, dataset, sampling feature et theia variable. The documentId of the
         * observations grouped are push in an array The observedPorperty of the observation grouped are pushed in an
         * array in order to keep the information theiaVariable / theiaCategories for each observation
         */
        GroupOperation g1 = Aggregation.group(
                "producer.producerId",
                "dataset.datasetId",
                "uri",
                "theiaCategories",
                "samplingFeature"
        )
                .push("documentId").as("documentId")
                .push("observedProperty").as("observedProperty")
                .first("producer").as("producer")
                .first("dataset").as("dataset");

        /**
         * Project the result of the group operation before to be inserted in collection
         */
        ProjectionOperation p2 = Aggregation.project()
                .and("documentId").as("documentId")
                .and("producer").as("producer")
                .and("dataset").as("dataset")
                .and("observedProperty").as("observation.observedProperty")
                .and("_id.samplingFeature").as("observation.featureOfInterest.samplingFeature")
                .andExclude("_id");

        /**
         * Insert the result of the aggregation pipeline in the collection
         */
        OutOperation o1 = Aggregation.out(outputCollectionName);

        AggregationOptions options = AggregationOptions.builder().allowDiskUse(true).build();
        List<AggregationOperation> aggs = Arrays.asList(m1, p1, g1, p2, o1);
        mongoTemplate.aggregate(Aggregation.newAggregation(aggs).withOptions(options), inputCollectionName, Document.class);

    }

    /**
     * Delete all document in a MongoDB collection for a given producerId
     *
     * @param collection MongoCollection - the collection where the documents need to be removed
     * @param producerId String - the producerId used to identify the Document to be removed
     */
    private void deleteDocumentsUsingProducerId(String producerId, String collectionName) {
        Query query = Query.query(where("producer.producerId").is(producerId));
        mongoTemplate.remove(query, collectionName);
    }

    
    /**
     * Group observations from a collection using producerId, datasetId, and location. Store the result in a new collection
     * @param inputCollectionName name of the input collection 
     * @param outputCollectionName name of the output collection 
     * @param producerId producerId
     */
    private void groupDocumentsByLocationAndInsertInOtherCollection(
            String inputCollectionName, String outputCollectionName, String producerId) {
        
        deleteDocumentsUsingProducerId(producerId, outputCollectionName);

        MatchOperation m1 = Aggregation.match(where("producer.producerId").is(producerId));
        
        ProjectionOperation p1 = Aggregation.project()
                .and("producer.producerId").as("producerId")
                .and("dataset.datasetId").as("datasetId")
                .and("observation.featureOfInterest.samplingFeature").as("samplingFeature")
                .and("documentId").as("documentId");

        GroupOperation g1 = group(
                "producerId",
                "datasetId",
                "samplingFeature"
        ).push("documentId").as("documentId");
        
        ProjectionOperation p2 = Aggregation.project("documentId")
                .and("_id.producerId").as("producerId")
                .and("_id.samplingFeature").as("samplingFeature")
                .andExclude("_id");
        
        OutOperation o1 = Aggregation.out(outputCollectionName);
        AggregationOptions options = AggregationOptions.builder().allowDiskUse(true).build();
        List<AggregationOperation> aggs = Arrays.asList(m1, p1, g1, p2, o1);
        mongoTemplate.aggregate(Aggregation.newAggregation(aggs).withOptions(options), inputCollectionName, Document.class);

    }

}
