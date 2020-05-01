package fr.theia_land.in_situ.backendspringbootassociationvariable.service;

import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.TheiaVariable;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

public interface VariableListService {
    List<TheiaVariable> setVariablesAlreadyAssociatedToCategories(List<String> categories);
    ResponseEntity<Map<String, String>> getPrefLabelUsingURI(String uri);
}
