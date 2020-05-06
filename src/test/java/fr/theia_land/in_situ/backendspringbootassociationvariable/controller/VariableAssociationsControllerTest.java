package fr.theia_land.in_situ.backendspringbootassociationvariable.controller;

import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ObservedProperty;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ProducerStat;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.TheiaVariable;
import fr.theia_land.in_situ.backendspringbootassociationvariable.service.AssociationService;
import fr.theia_land.in_situ.backendspringbootassociationvariable.service.ProducerListService;
import fr.theia_land.in_situ.backendspringbootassociationvariable.service.TheiaVariableCreationService;
import fr.theia_land.in_situ.backendspringbootassociationvariable.service.VariableListService;
import org.hamcrest.Matcher;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = VariableAssociationsController.class)
@TestPropertySource(locations = "/test.properties")
class VariableAssociationsControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    AssociationService associationService;
    @MockBean
    ProducerListService producerListService;
    @MockBean
    TheiaVariableCreationService theiaVariableCreationService;
    @MockBean
    VariableListService variableListService;

    @WithMockUser("user")
    @org.junit.jupiter.api.Test
    void setProducerStats() throws Exception {
        List<ProducerStat> producerStats = new ArrayList<>();
        producerStats.add(new ProducerStat());
        when(producerListService.setProducerStats()).thenReturn(producerStats);
        this.mockMvc.perform(MockMvcRequestBuilders.get("/association/setProducerStats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)));
        verify(producerListService, times(1)).setProducerStats();
    }

    @WithMockUser("user")
    @org.junit.jupiter.api.Test
    void setProducerVariables() throws Exception {
        List<List<ObservedProperty>> obsLists = new ArrayList<>();
        obsLists.add(new ArrayList<ObservedProperty>() {{
            add(new ObservedProperty());
        }});
        when(producerListService.setProducerVariables(anyString())).thenReturn(obsLists);
        this.mockMvc.perform(MockMvcRequestBuilders.get("/association/setProducerVariables?producerId=TEST"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)));
        verify(producerListService, times(1)).setProducerVariables(anyString());
    }

    @WithMockUser("user")
    @org.junit.jupiter.api.Test
    void setVariablesAlreadyAssociatedToCategories() throws Exception {
        JSONArray json = new JSONArray("[\"https://w3id.org/ozcar-theia/atmosphericRadiation\"]");
        List<TheiaVariable> list = new ArrayList();
        list.add(new TheiaVariable());
        when(variableListService.setVariablesAlreadyAssociatedToCategories(anyList())).thenReturn(list);
        this.mockMvc.perform(MockMvcRequestBuilders.post("/association/setVariablesAlreadyAssociatedToCategories")
                .contentType("application/json")
                .content(json.toString())
        ).andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)));
        verify(variableListService, times(1)).setVariablesAlreadyAssociatedToCategories(anyList());
    }

    @WithMockUser("user")
    @org.junit.jupiter.api.Test
    void createANewTheiaVariable() throws Exception {
        ResponseEntity<TheiaVariable> theiaVariableResponseEntity = new ResponseEntity<TheiaVariable>(new TheiaVariable(), HttpStatus.ACCEPTED);
        when(theiaVariableCreationService.createANewTheiaVariable(anyString())).thenReturn(theiaVariableResponseEntity);
        this.mockMvc.perform(MockMvcRequestBuilders.post("/association/createANewTheiaVariable")
        .content("{\"uri\":\"https://w3id.org/ozcar-theia/variables/conductivity\",\"prefLabel\":[{\"lang\":\"en\",\"text\":\"Conductivity\"}]}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$",hasKey("uri")))
                .andExpect(jsonPath("$",hasKey("prefLabel")));
        verify(theiaVariableCreationService, times(1)).createANewTheiaVariable(anyString());
    }

    @WithMockUser("user")
    @Test
    void submitAssociation() throws Exception {
        this.mockMvc.perform(MockMvcRequestBuilders.post("/association/submitAssociation")
                .content("{\"producerId\":\"CATC\",\"associations\":[{\"variable\":{\"name\":[{\"lang\":\"en\",\"text\":\"Air Pressure\"}],\"unit\":[{\"lang\":\"en\",\"text\":\"mbar\"}],\"theiaVariable\":null,\"theiaCategories\":[\"https://w3id.org/ozcar-theia/atmosphericPressure\"],\"oldIndex\":4},\"uri\":\"https://w3id.org/ozcar-theia/atmosphericPressure\",\"prefLabel\":[{\"lang\":\"en\",\"text\":\"Atmospheric pressure\"}]}]}"))
                .andExpect(status().isOk());
        verify(associationService,times(1)).submitAssociation(anyString());
    }

    @WithMockUser("user")
    @Test
    void getPrefLabelUsingURI() throws Exception {
        HashMap<String, String> map = new HashMap<>();
        map.put("test", "test");
        ResponseEntity<Map<String, String>> response = new ResponseEntity<Map<String, String>>(map, HttpStatus.ACCEPTED);
        when(variableListService.getPrefLabelUsingURI(anyString())).thenReturn(response);
        this.mockMvc.perform(MockMvcRequestBuilders.get("/association/getPrefLabelUsingURI?URI=test"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$", hasKey("test")))
                .andExpect(jsonPath("$.test",is("test")));
    }

}