package fr.theia_land.in_situ.backendspringbootassociationvariable.service;

import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.RDFUtils;
import fr.theia_land.in_situ.backendspringbootassociationvariable.controller.VariableAssociationsController;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.I18n;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.TheiaVariable;
import org.apache.commons.text.CaseUtils;
import org.apache.jena.sparql.ARQException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

public class TheiaVariableCreationServiceImpl implements TheiaVariableCreationService {
    private static final Logger logger = LoggerFactory.getLogger(TheiaVariableCreationServiceImpl.class);

    private final RDFUtils rDFUtils;

    @Autowired
    public TheiaVariableCreationServiceImpl(RDFUtils rDFUtils) {
        this.rDFUtils = rDFUtils;
    }

    @Override
    public ResponseEntity<TheiaVariable> createANewTheiaVariable(String payload) {
        /**
         * Parse info request body into a JSON object
         */
        JSONObject json = new JSONObject(payload);
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
        // try (RDFConnection conn = RDFConnectionFactory.connect("http://in-situ.theia-land.fr:3030/theia_vocabulary/")) {

        /**
         * Created to avoid circular reference (broader and narrower), if the uri is already used in categories
         * vocabulary "Variable" is added at the end of the uri.
         */
        if (rDFUtils.existSkosCategoryConcept(uri)) {
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
        //   }

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
}
