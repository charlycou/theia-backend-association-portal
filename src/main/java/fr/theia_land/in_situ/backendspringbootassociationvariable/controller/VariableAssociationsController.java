/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.controller;

import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ProducerStat;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.I18n;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ObservedProperty;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.TheiaVariable;
import fr.theia_land.in_situ.backendspringbootassociationvariable.service.AssociationService;
import fr.theia_land.in_situ.backendspringbootassociationvariable.service.ProducerListService;
import fr.theia_land.in_situ.backendspringbootassociationvariable.service.TheiaVariableCreationService;
import fr.theia_land.in_situ.backendspringbootassociationvariable.service.VariableListService;
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
import org.apache.jena.sparql.ARQException;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;

import static org.springframework.data.mongodb.core.query.Criteria.where;

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author coussotc
 */
@RestController
@RequestMapping("/association")
@CrossOrigin(origins = "${app.api_host}")
public class VariableAssociationsController {

    private final AssociationService associationService;
    private final ProducerListService producerListService;
    private final TheiaVariableCreationService theiaVariableCreationService;
    private final VariableListService variableListService;

    @Autowired
    public VariableAssociationsController(AssociationService associationService, ProducerListService producerListService, TheiaVariableCreationService theiaVariableCreationService, VariableListService variableListService) {
        this.associationService = associationService;
        this.producerListService = producerListService;
        this.theiaVariableCreationService = theiaVariableCreationService;
        this.variableListService = variableListService;
    }


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
        return producerListService.setProducerStats();
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
        return producerListService.setProducerVariables(producerId);
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
        return variableListService.setVariablesAlreadyAssociatedToCategories(categories);
    }

    /**
     * Create in new Theia variable in the Theia OZCAR thesaurus. A new SKOS Concept is pushed in the thesaurus.
     *
     * @param info PrefLabel and URI of the concept to be added. Optionally contains exact match concept from other
     *             thesaurus. ex
     *             "{"uri":"https://w3id.org/ozcar-theia/variables/conductivity","prefLabel":[{"lang":"en","text":"Conductivity"}]}"
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
        return theiaVariableCreationService.createANewTheiaVariable(info);
    }

    /**
     * Save association between one or several producer variables and theia variables using the "variableAssociations"
     * collection
     *
     * @param associationInfo a String that can be parsed into json ex:
     *                        {"producerId":"CATC","associations":[{"variable":{"name":[{"lang":"en","text":"Air
     *                        Pressure"}],"unit":[{"lang":"en","text":"mbar"}],"theiaVariable":null,"theiaCategories":["https://w3id.org/ozcar-theia/atmosphericPressure"],"oldIndex":4},"uri":"https://w3id.org/ozcar-theia/atmosphericPressure","prefLabel":[{"lang":"en","text":"Atmospheric
     *                        pressure"}]}]}
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
        associationService.submitAssociation(associationInfo);
    }

    /**
     * Get the prefLabel of a skos concept using the uri of the concept
     *
     * @param uri String - uri of the concept
     * @return ResponseEntity<Map < String, String>>
     */
    @ApiOperation(value = " Get the prefLabel of a skos concept using the uri of the concept",
            response = Map.class,
            responseContainer = "ResponseEntity")
    @GetMapping("/getPrefLabelUsingURI")
    private ResponseEntity<Map<String, String>> getPrefLabelUsingURI(
            @ApiParam(required = true,
                    example = "https://w3id.org/ozcar-theia/atmosphericTemperature")
            @RequestParam("URI") String uri) {
        return variableListService.getPrefLabelUsingURI(uri);
    }
}
