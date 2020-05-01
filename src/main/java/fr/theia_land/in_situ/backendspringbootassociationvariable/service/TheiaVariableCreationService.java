package fr.theia_land.in_situ.backendspringbootassociationvariable.service;

import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.TheiaVariable;
import org.springframework.http.ResponseEntity;

public interface TheiaVariableCreationService {
    ResponseEntity<TheiaVariable> createANewTheiaVariable(String payload);
}

