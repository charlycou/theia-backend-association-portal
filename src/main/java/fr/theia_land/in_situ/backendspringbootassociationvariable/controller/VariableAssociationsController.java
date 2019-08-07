/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.controller;

import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.Utils.MongoDbUtils;
import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.Utils.RDFUtils;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ProducerStat;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.I18n;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ObservedProperty;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.TheiaVariable;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
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

    //Indicate that mongoTemplate must be injected by Spring IoC
    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MongoDbUtils mongoDbUtils;

    @Autowired
    private RDFUtils rDFUtils;

    /**
     * For each producer, query the number producer variable associated to theia variable and the number of diffrenet
     * producer variable. In order to print the state of association work for each producer.
     *
     * @return List of ProducerStat - one for each prodcuer
     */
    @ApiOperation(value = "For each producer, query the number producer variable associated to theia variable and the number of diffrenet producer \n"
            + "     * variable.",
            notes = "In order to print the state of association work for each producer.",
            response = ProducerStat.class,
            responseContainer = "List")
    @GetMapping("/setProducerStats")
    private List<ProducerStat> setProducerStats() {
        return mongoDbUtils.setProducerStats();
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
    @ApiOperation(value = "For a given producer, query the non-associated producer variable name and the associated producer variable name",
            notes = "A producer variables are concidered different if for a same 'name' they have different 'unit' or 'theiaCateogries'",
            response = List.class,
            responseContainer = "List")
    @GetMapping("/setProducerVariables")
    private List<List<ObservedProperty>> setProducerVariables(
            @ApiParam(required = true,
                    value = "Producer ID",
                    example = "MSEC")
            @RequestParam("producerId") String producerId) {
        return mongoDbUtils.getDifferentProducerVariableSorted(producerId);
    }

    /**
     * Find the Theia Varaiable already associated to one or several cateogories. In order to suggest Theia variable to
     * be associated with for a given producer variable.
     *
     * @param categories List of category uri ex: "["https://w3id.org/ozcar-theia/atmosphericRadiation"]"
     * @return List of Document containing the theia variables
     */
    @ApiOperation(value = "Find the Theia Varaiable already associated to one or several cateogories",
            notes = "Documents are queried using the json array of Theia category field: [\"https://w3id.org/ozcar-theia/atmosphericRadiation\"]",
            response = Document.class,
            responseContainer = "List")
    @PostMapping("/setVariablesAlreadyAssociatedToCategories")
    private List<TheiaVariable> setVariablesAlreadyAssociatedToCategories(
            @ApiParam(required = true,
                    value = "Example (quotes inside brackets can be badly escaped by UI...):\n [\"https://w3id.org/ozcar-theia/atmosphericRadiation\"]",
                    examples = @Example(value = {
                @ExampleProperty(value = "[\"https://w3id.org/ozcar-theia/atmosphericRadiation\"]")
            }))
            @RequestBody List<String> categories) {
        return mongoDbUtils.getVariablesAlreadyAssociatedToCategories(categories);
    }

    /**
     * Create in new Theia variable in the Theia OZCAR thesaurus. A new SKOS Concept is pushed in the thesaurus.
     *
     * @param info PrefLabel and URI of the concept to be added. Optionally contains exact match concept from other
     * thesaurus. ex
     * "{"uri":"https://w3id.org/ozcar-theia/variables/conductivity","prefLabel":[{"lang":"en","text":"Conductivity"}]}"
     * @return ResponseEntity HttpStatus code according to the success or failure of the request. On Success it also
     * return the TheiaVariable added to the thesaurus.
     */
    @ApiOperation(value = "Create in new Theia variable in the Theia OZCAR thesaurus. A new SKOS Concept is pushed in the thesaurus",
            response = TheiaVariable.class,
            responseContainer = "ResponseEntity")
    @PostMapping("/createANewTheiaVariable")
    private ResponseEntity<TheiaVariable> createANewTheiaVariable(
            @ApiParam(required = true,
                    value = "Example (quotes inside brackets can be badly escaped by UI...):\n "
                    + "{\"uri\":\"https://w3id.org/ozcar-theia/variables/conductivity\",\"prefLabel\":[{\"lang\":\"en\",\"text\":\"Conductivity\"}]}",
                    examples = @Example(value = {
                @ExampleProperty(value = "{\"uri\":\"https://w3id.org/ozcar-theia/variables/conductivity\",\"prefLabel\":[{\"lang\":\"en\",\"text\":\"Conductivity\"}]}")
            }))
            @RequestBody String info) {

        /**
         * Parse info request body into a JSON object
         */
        JSONObject json = new JSONObject(info);
        String prefLabel = json.getString("prefLabel");
        List<String> categories = (List<String>) (List<?>) (json.getJSONArray("broaders").toList());
        List<String> exactMatches = (List<String>) (List<?>) (json.getJSONArray("exactMatches").toList());

        /**
         * Create uri from
         */
        String uri = Normalizer.normalize(prefLabel, java.text.Normalizer.Form.NFD)
                .replaceAll("[^\\p{ASCII}]", "")
                .replaceAll("[^a-zA-Z0-9 ]", "");
        uri = "https://w3id.org/ozcar-theia/variables/" + CaseUtils.toCamelCase(uri, false, new char[]{' '});

        /**
         * Connect to the triple store
         */
        try (RDFConnection conn = RDFConnectionFactory.connect("http://in-situ.theia-land.fr:3030/theia_vocabulary/")) {

            /**
             * Created to avoid circular reference (broader and narrower), if the uri is already used in categories
             * vocabulary "Variable" is added at the end of the uri.
             */
            if (rDFUtils.existSkosVariable(uri)) {
                String uriTmp = new String(uri);
                uri = uriTmp + "Variable";
            }
            /**
             * Insert the skos concept in the triple store
             */
            try {
                rDFUtils.insertSkosVariable(uri, prefLabel, exactMatches);
            } catch (ARQException ex) {
                logger.error(ex.getMessage());
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
                return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
            }
        }

        /**
         * Create the TheiaVariable to be returned if the insert of the skos concpet has been sucessful
         */
        TheiaVariable theiaVariable = new TheiaVariable();
        theiaVariable.setUri(uri);

        List<I18n> prefLabels = new ArrayList<>();
        I18n i18n = new I18n();
        i18n.setLang("en");
        i18n.setText(prefLabel);
        prefLabels.add(i18n);
        theiaVariable.setPrefLabel(prefLabels);
        return new ResponseEntity<>(theiaVariable, HttpStatus.ACCEPTED);
    }

    /**
     * Save association between one or several producer variables and theia variables using the "variableAssociations"
     * collection
     *
     * @param associationInfo a String that can be parsed into json ex:
     * {"producerId":"CATC","associations":[{"variable":{"name":[{"lang":"en","text":"Air
     * Pressure"}],"unit":[{"lang":"en","text":"mbar"}],"theiaVariable":null,"theiaCategories":["https://w3id.org/ozcar-theia/atmosphericPressure"],"oldIndex":4},"uri":"https://w3id.org/ozcar-theia/atmosphericPressure","prefLabel":[{"lang":"en","text":"Atmospheric
     * pressure"}]}]}
     */
    @PostMapping("/submitAssociation")
    @ApiOperation(value = "Save association between one or several producer variables and theia variables using the \"variableAssociations\" collection")
    private void submitAssociation(
            @ApiParam(required = true,
                    value = "Example (quotes inside brackets can be badly escaped by UI...):\n "
                    + "{\"producerId\":\"CATC\",\"associations\":[{\"variable\":{\"name\":[{\"lang\":\"en\",\"text\":\"Air Pressure\"}],\"unit\":[{\"lang\":\"en\",\"text\":\"mbar\"}],\"theiaVariable\":null,\"theiaCategories\":[\"https://w3id.org/ozcar-theia/atmosphericPressure\"],\"oldIndex\":4},\"uri\":\"https://w3id.org/ozcar-theia/atmosphericPressure\",\"prefLabel\":[{\"lang\":\"en\",\"text\":\"Atmospheric pressure\"}]}]}",
                    examples = @Example(value = {
                @ExampleProperty(value = "{\"producerId\":\"CATC\",\"associations\":[{\"variable\":{\"name\":[{\"lang\":\"en\",\"text\":\"Air Pressure\"}],\"unit\":[{\"lang\":\"en\",\"text\":\"mbar\"}],\"theiaVariable\":null,\"theiaCategories\":[\"https://w3id.org/ozcar-theia/atmosphericPressure\"],\"oldIndex\":4},\"uri\":\"https://w3id.org/ozcar-theia/atmosphericPressure\",\"prefLabel\":[{\"lang\":\"en\",\"text\":\"Atmospheric pressure\"}]}]}")
            }))
            @RequestBody String associationInfo) {
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
             * Mongodb "observations" collection update. The corresponding documents are updated with
             * "observations.observedProperty.theiaVariable" field.
             */
            /**
             * Storing matching value into lists to find corresponding observation
             */
            List<String> variableNames = new ArrayList<>();
            List<String> unitName = new ArrayList<>();
            List<String> theiaCategoryUri = new ArrayList<>();
            /**
             * Store all english producer variable name
             */
            asso.getJSONObject("variable").getJSONArray("name").forEach(item -> {
                JSONObject tmp = (JSONObject) item;
                if ("en".equals(tmp.getString("lang"))) {
                    variableNames.add(tmp.getString("text"));
                }
            });
            /**
             * Store all non null unit name
             */
            asso.getJSONObject("variable").getJSONArray("unit").forEach(item -> {
                JSONObject tmp = (JSONObject) item;
                if (!tmp.isNull("text")) {
                    unitName.add(tmp.getString("text"));
                }
            });
            /**
             * Store all category uri
             */
            asso.getJSONObject("variable").getJSONArray("theiaCategories").forEach(item -> {
                theiaCategoryUri.add((String) item);
            });
            /**
             * Set match operation for producerId, variable name, unit , theia categories
             */
            List<AggregationOperation> aggregationOperations = new ArrayList();
            aggregationOperations.add(Aggregation.match(where("producer.producerId").is(producerId)));
            aggregationOperations.add(Aggregation.match(where("observation.observedProperty.name.text").in(variableNames)));
            if (unitName.size() > 0) {
                aggregationOperations.add(Aggregation.match(where("observation.observedProperty.unit.text").in(unitName)));
            }
            aggregationOperations.add(Aggregation.match(where("observation.observedProperty.theiaCategories").in(theiaCategoryUri)));
            List<Document> documents = mongoTemplate.aggregate(Aggregation.newAggregation(aggregationOperations), "observations", Document.class).getMappedResults();

            /**
             * if "prefLabel" and "uri" fields are not null the association is made and the documents from the data base
             * are updated
             */
            if (asso.getJSONArray("prefLabel").isEmpty() && asso.isNull("uri")) {
                for (Document doc : documents) {
                    Query query = Query.query(new Criteria("documentId").is(doc.getString("documentId")));
                    Update update = new Update();
                    update.unset("observation.observedProperty.theiaVariable");
                    // Update update = Update.update("observation.observedProperty.theiaVariable");
                    mongoTemplate.updateFirst(query, update, "observations");
                }
                mongoDbUtils.updateOneVariableAssociation("variableAssociations", producerId, asso);
            } else {

                /**
                 * Each observation is updated by adding "observation.observaProperty.theiaVariables" object
                 */
                String prefLabelEn = null;
                for (Document doc : documents) {
                    JSONArray prefLabelArray = asso.getJSONArray("prefLabel");

                    for (int i = 0; i < prefLabelArray.length(); i++) {
                        JSONObject jo = prefLabelArray.getJSONObject(i);
                        if (jo.getString("lang").equals("en")) {
                            prefLabelEn = jo.getString("text");
                        }
                    }

                    Document theiaVariable = new Document("lang", "en").append("text", prefLabelEn);
                    Query query = Query.query(new Criteria("documentId").is(doc.getString("documentId")));
                    Update update = Update.update("observation.observedProperty.theiaVariable",
                            new Document("uri", asso.getString("uri"))
                                    .append("prefLabel", Arrays.asList(theiaVariable)));
                    mongoTemplate.updateFirst(query, update, "observations");
                }
                /**
                 * If the theia variable correspond to an existing uri with an updated prefLabel, other previously
                 * associated document need to be updated
                 */
                mongoDbUtils.checkPrefLabelForOtherObservations(asso.getString("uri"), prefLabelEn, "en");

                /**
                 * Update the "variableAssociation" collection with one association
                 */
                mongoDbUtils.updateOneVariableAssociation("variableAssociations", producerId, asso);
                /**
                 * for the given association the semantic link "skos:broaders" need to be made with the observation
                 * categories
                 */
                rDFUtils.instertSkosBroaders(asso.getString("uri"), theiaCategoryUri);
            }

        });

        /**
         * the documents of the collection "observations" are grouped to "observationsLite" and "mapItems" collection
         * for the given producer.
         */
        mongoDbUtils.groupDocumentsByVariableAtGivenLocationAndInsertInOtherCollection("observations", "observationsLite", producerId);
        mongoDbUtils.groupDocumentsByLocationAndInsertInOtherCollection("observationsLite", "mapItems", producerId);
        // mongoDbUtils.storeAssociation("variableAssociations", producerId);

    }

    /**
     * Get the prefLabel of a skos concept using the uri of the concept
     *
     * @param uri String - uri of the concept
     * @return ResponseEntity<Map<String, String>>
     */
    @ApiOperation(value = " Get the prefLabel of a skos concept using the uri of the concept",
            response = Map.class,
            responseContainer = "ResponseEntity")
    @GetMapping("/getPrefLabelUsingURI")
    private ResponseEntity<Map<String, String>> getPrefLabelUsingURI(
            @ApiParam(required = true,
                    example = "https://w3id.org/ozcar-theia/atmosphericTemperature")
            @RequestParam("URI") String uri) {
        Map<String, String> response = new HashMap();
        try {
            List<I18n> prefLabels = new ArrayList<>();
            I18n i18n = new I18n();
            i18n.setLang("en");
            i18n.setText(rDFUtils.getPrefLabel(uri));
            prefLabels.add(i18n);
            response.put("prefLabel", new JSONArray(prefLabels).toString());
            return new ResponseEntity<>(response, HttpStatus.ACCEPTED);
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
}
