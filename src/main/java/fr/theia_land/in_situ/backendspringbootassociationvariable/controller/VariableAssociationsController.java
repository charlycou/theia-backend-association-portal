/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.controller;

import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.TheiaVariableRepository;
import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.Utils.RDFUtils;
import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.VariableAssociationsRepository;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.Entities.TheiaVariableName;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ObservedProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.text.CaseUtils;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.sparql.ARQException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.ReplaceRootOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import sun.text.Normalizer;

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

        String uri = Normalizer.normalize(prefLabel, java.text.Normalizer.Form.NFD, 0)
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
}
