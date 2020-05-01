package fr.theia_land.in_situ.backendspringbootassociationvariable.service;

import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.MongoDbUtils;
import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.RDFUtils;
import fr.theia_land.in_situ.backendspringbootassociationvariable.controller.VariableAssociationsController;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.I18n;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.TheiaVariable;
import org.apache.jena.sparql.ARQException;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class VariableListServiceImpl implements VariableListService {

    private static final Logger logger = LoggerFactory.getLogger(VariableListService.class);

    private final MongoDbUtils mongoDbUtils;
    private final RDFUtils rDFUtils;

    @Autowired
    public VariableListServiceImpl(MongoDbUtils mongoDbUtils, RDFUtils rDFUtils) {
        this.mongoDbUtils = mongoDbUtils;
        this.rDFUtils = rDFUtils;
    }


    @Override
    public List<TheiaVariable> setVariablesAlreadyAssociatedToCategories(List<String> categories) {
        return mongoDbUtils.getVariablesAlreadyAssociatedToCategories(categories);
    }

    @Override
    public ResponseEntity<Map<String, String>> getPrefLabelUsingURI(String uri) {
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
