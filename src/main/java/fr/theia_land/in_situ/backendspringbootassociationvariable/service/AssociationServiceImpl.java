package fr.theia_land.in_situ.backendspringbootassociationvariable.service;

import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.MongoDbUtils;
import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.RDFUtils;
import org.bson.Document;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;

@Service
public class AssociationServiceImpl implements AssociationService {
    private final MongoDbUtils mongoDbUtils;
    private final RDFUtils rDFUtils;
    private final MongoTemplate mongoTemplate;

    @Autowired
    public AssociationServiceImpl(MongoDbUtils mongoDbUtils, RDFUtils rDFUtils, MongoTemplate mongoTemplate) {
        this.mongoDbUtils = mongoDbUtils;
        this.rDFUtils = rDFUtils;
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void submitAssociation(String associationInfo) {
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
            List<String> producerVariableNames = new ArrayList<>();
            List<String> unitName = new ArrayList<>();
            List<String> theiaCategoryUri = new ArrayList<>();
            /**
             * Store all english producer variable name
             */
            asso.getJSONObject("variable").getJSONArray("name").forEach(item -> {
                JSONObject tmp = (JSONObject) item;
                if ("en".equals(tmp.getString("lang"))) {
                    producerVariableNames.add(tmp.getString("text"));
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

            List<Document> documents = mongoDbUtils.getObservedPropertyMatchingDocuments(producerId,producerVariableNames,unitName,theiaCategoryUri);
//            /**
//             * Set match operation for producerId, variable name, unit , theia categories
//             */
//            List<Document> documents = new ArrayList();
//            MatchOperation m1 = Aggregation.match(where("producer.producerId").is(producerId));
//            MatchOperation m2 = Aggregation.match(where("observation.observedProperty.name.text").in(producerVariableNames));
//            MatchOperation m4 = Aggregation.match(where("observation.observedProperty.theiaCategories").in(theiaCategoryUri));
//            if (unitName.size() > 0) {
//                MatchOperation m3 = Aggregation.match(where("observation.observedProperty.unit.text").in(unitName));
//                documents = mongoTemplate.aggregate(Aggregation.newAggregation(m1, m2, m3, m4), "observations", Document.class).getMappedResults();
//            } else {
//                documents = mongoTemplate.aggregate(Aggregation.newAggregation(m1, m2, m4), "observations", Document.class).getMappedResults();
//            }

            /**
             * if "prefLabel" and "uri" fields are null the operation is a "delete association". Mongodb "observations"
             * and "variableAssociation" collections are updated. Semantic links are removed if necessary.
             */
            if (asso.getJSONArray("prefLabel").isEmpty() && asso.isNull("uri")) {
                for (Document doc : documents) {
                    mongoDbUtils.deleteTheiaVariableKey(doc.getString("documentId"));
                }
                /**
                 * After deleting the theiaVariable key from documents of the "observations" collection,
                 * the association is removed from the variableAssociations collection if this association does not
                 * correspond to other observation for the same producer (i.e. the unit can change for a same
                 * producerVariable/TheiaCategorie couple).
                 */
                if (mongoDbUtils.getTheiaVariableMatchingDocument(producerId,producerVariableNames,theiaCategoryUri).size() == 0) {
                    mongoDbUtils.updateOneVariableAssociation("variableAssociations", producerId, asso);
                }

                /**
                 * If the theiaVariable/TheiaCategory association don't exists any more in other associations of the
                 * variableAssociations collection, the semantic links are removed from the thesaurus for each
                 * categories of the deleted association. Otherwise, if other associations exist, wee check that each
                 * categories of the deleted association exists among the remaining association of the
                 * variableAssociations collection. If one theia categories does not exist any more among the remaining
                 * association, the semantic link theiaVariable/TheiaCategory is removed
                 */
                String theiaVariableUri = asso.getJSONObject("variable").getJSONObject("theiaVariable").getString("uri");

                for (String uri : theiaCategoryUri) {
                    if (mongoDbUtils.isAssociationExisting(theiaVariableUri,uri)) {
                        rDFUtils.removeSkosBroaders(asso.getJSONObject("variable").getJSONObject("theiaVariable").getString("uri"), uri);
                    }
                }

                /**
                 * The operation made is an "association creation" or "association update"
                 */
            } else {
                /**
                 * if the operation is an association update, we store the theiaVariable uri to be updated. This uri is
                 * used to remove semantic links variableUri/TheiaCateories for each categories of the association if
                 * they don't exist any more.
                 */
                String theiaVariableUri = null; //will be null if it is an creation - not null if it is an update
                if (documents.get(0).get("observation", Document.class).get("observedProperty", Document.class).containsKey("theiaVariable")) {
                    theiaVariableUri = documents.get(0).get("observation", Document.class).get("observedProperty", Document.class)
                            .get("theiaVariable", Document.class).getString("uri");
                }

//                /**
//                 * Each observation is updated by adding "observation.observedProperty.theiaVariables" object
//                 */
//                String prefLabelEn = null;
//                for (Document doc : documents) {
//                    JSONArray prefLabelArray = asso.getJSONArray("prefLabel");
//
//                    for (int i = 0; i < prefLabelArray.length(); i++) {
//                        JSONObject jo = prefLabelArray.getJSONObject(i);
//                        if (jo.getString("lang").equals("en")) {
//                            prefLabelEn = jo.getString("text");
//                        }
//                    }
//
//                    Document theiaVariable = new Document("lang", "en").append("text", prefLabelEn);
//                    Query query = Query.query(new Criteria("documentId").is(doc.getString("documentId")));
//                    Update update = Update.update("observation.observedProperty.theiaVariable",
//                            new Document("uri", asso.getString("uri"))
//                                    .append("prefLabel", Arrays.asList(theiaVariable)));
//                    mongoTemplate.updateFirst(query, update, "observations");
//                }
                String prefLabelEn =mongoDbUtils.addTheiaVariable(asso,documents);
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

                /**
                 * If the old theiaVariable/TheiaCategory association don't exists any more in other associations of the
                 * variableAssociations collection, the semantic links are removed from the thesaurus for each
                 * categories of the deleted association. Otherwise, if other associations exist, wee check that each
                 * categories of the deleted association exists among the remaining association of the
                 * variableAssociations collection. If one theia categories does not exist any more among the remaining
                 * association, the semantic link theiaVariable/TheiaCategory is removed
                 */
                if (theiaVariableUri != null) {
                    MatchOperation m8 = Aggregation.match(where("theiaVariable.uri").is(
                            theiaVariableUri));
                    for (String uri : theiaCategoryUri) {
                        MatchOperation m9 = Aggregation.match(where("theiaCategories").in(uri));
                        if (mongoDbUtils.isAssociationExisting(theiaVariableUri,uri)) {
                            rDFUtils.removeSkosBroaders(asso.getJSONObject("variable").getJSONObject("theiaVariable").getString("uri"), uri);
                        }
                    }
                }

            }

        });

        /**
         * the documents of the collection "observations" are grouped to "observationsLite" and "mapItems" collection
         * for the given producer.
         */
        mongoDbUtils.groupDocumentsByVariableAtGivenLocationAndInsertInOtherCollection("observations", "observationsLite", producerId);
        mongoDbUtils.groupDocumentsByLocationAndInsertInOtherCollection("observationsLite", "mapItems", producerId);

    }
}
