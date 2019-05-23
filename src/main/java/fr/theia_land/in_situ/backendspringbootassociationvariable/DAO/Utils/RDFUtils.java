/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;

/**
 *
 * @author coussotc
 */
public class RDFUtils {

    public static boolean existSkosVariable(String uri) {
        String queryString = "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n"
                + "SELECT *\n"
                + "FROM <https://w3id.org/ozcar-theia/>\n"
                + "WHERE {\n"
                + "   <https://w3id.org/ozcar-theia/variableCategories> skos:member <" + uri + ">\n"
                + "}";
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qExec = QueryExecutionFactory.createServiceRequest("http://in-situ.theia-land.fr:3030/theia_vocabulary/", query);) {

            ResultSet rs = qExec.execSelect();
            return rs.hasNext();
        }
    }

    /**
     * Insert a new variable in the triple store.
     *
     * @param uri String - uri of the concept variable
     * @param prefLabel String - prefLabel of the concept
     * @param categories List\<String\> - lit of categories uri associated to the variables
     */
    public static void insertSkosVariable(String uri, String prefLabel, List<String> categories) {

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        Credentials credentials = new UsernamePasswordCredentials("admin", "pw");
        credsProvider.setCredentials(AuthScope.ANY, credentials);
        HttpClient httpclient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();
        //HttpOp.setDefaultHttpClient(httpclient);

        List<String> updateString = new ArrayList<>();
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> a skos:Concept }}");
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> skos:inScheme <https://w3id.org/ozcar-theia/ozcarTheiaThesaurus> }}");
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA{ GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> skos:prefLabel '" + prefLabel + "'@en} }");
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA{ GRAPH <https://w3id.org/ozcar-theia/> {<https://w3id.org/ozcar-theia/Variables> skos:member <" + uri + "> } }");
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA{ GRAPH <https://w3id.org/ozcar-theia/> {<https://w3id.org/ozcar-theia/Variables> skos:narrower <" + uri + "> } }");
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> skos:broader <https://w3id.org/ozcar-theia/Variables>}}");

        for (String categoryUri : categories) {
            updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#>  INSERT DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> skos:broader <" + categoryUri + ">}}");
        }

        UpdateRequest update;
        UpdateProcessor uExec;
        for (int i = 0; i < updateString.size(); i++) {

                update = UpdateFactory.create(updateString.get(i));
                uExec = UpdateExecutionFactory.createRemote(update, "http://in-situ.theia-land.fr:3030/theia_vocabulary/", httpclient);
                uExec.execute();

        }
    }
}
