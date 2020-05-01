/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.DAO;

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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author coussotc
 */
@Component
public class RDFUtils {

    private String sparqlUser;

    @Value("${sparql.endpoint.user}")
    private void setSparqlUser(String user) {
        sparqlUser = user;
    }

    private String sparqlPassword;

    @Value("${sparql.endpoint.password}")
    private void setSparqlPassword(String password) {
        sparqlPassword = password;
    }

    private String sparqlUrl;

    @Value("${sparql.endpoint.url}")
    private void setSparqlUrl(String url) {
        sparqlUrl = url;
    }

    public boolean existSkosCategoryConcept(String uri) {
        String queryString = "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n"
                + "SELECT *\n"
                + "FROM <https://w3id.org/ozcar-theia/>\n"
                + "WHERE {\n"
                + "   <https://w3id.org/ozcar-theia/variableCategoriesGroup> skos:member <" + uri + ">\n"
                + "}";
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qExec = QueryExecutionFactory.createServiceRequest(sparqlUrl, query);) {

            ResultSet rs = qExec.execSelect();
            return rs.hasNext();
        }
    }

    /**
     * Insert a new variable in the triple store.
     *
     * @param uri        String - uri of the concept variable
     * @param prefLabel  String - prefLabel of the concept
     * @param exactMatches List\<String\> - list of SKOS exactMatches concept uri
     */
    public void insertSkosVariable(String uri, String prefLabel, List<String> exactMatches) {

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        Credentials credentials = new UsernamePasswordCredentials(sparqlUser, sparqlPassword);
        credsProvider.setCredentials(AuthScope.ANY, credentials);
        HttpClient httpclient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();
        //HttpOp.setDefaultHttpClient(httpclient);

        List<String> updateString = new ArrayList<>();
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> a skos:Concept }}");
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> skos:inScheme <https://w3id.org/ozcar-theia/ozcarTheiaThesaurus> }}");
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA{ GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> skos:prefLabel '" + prefLabel + "'@en} }");
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA{ GRAPH <https://w3id.org/ozcar-theia/> {<https://w3id.org/ozcar-theia/variableGroup> skos:member <" + uri + "> } }");
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA{ GRAPH <https://w3id.org/ozcar-theia/> {<https://w3id.org/ozcar-theia/variables> skos:narrower <" + uri + "> } }");
        updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#> INSERT DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> skos:broader <https://w3id.org/ozcar-theia/variables>}}");

//        for (String categoryUri : categories) {
//            updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#>  INSERT DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> skos:broader <" + categoryUri + ">}}");
//        }
        for (String exactMatchUri : exactMatches) {
            updateString.add("PREFIX skos: <http://www.w3.org/2004/02/skos/core#>  INSERT DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> skos:exactMatch <" + exactMatchUri + ">}}");
        }

        for (String s : updateString) {
            UpdateExecutionFactory.createRemote(UpdateFactory.create(s), sparqlUrl, httpclient).execute();
        }
    }

    public void instertSkosBroaders(String uri, List<String> categories) {
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        Credentials credentials = new UsernamePasswordCredentials(sparqlUser, sparqlPassword);
        credsProvider.setCredentials(AuthScope.ANY, credentials);
        HttpClient httpclient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();

        String update;

        for (String categoryUri : categories) {
            update = "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>  INSERT DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uri + "> skos:broader <" + categoryUri + ">}}";
            UpdateExecutionFactory.createRemote(UpdateFactory.create(update), sparqlUrl, httpclient).execute();
        }
    }

    public void removeSkosBroaders(String uriVariable, String uriCategory) {
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        Credentials credentials = new UsernamePasswordCredentials(sparqlUser, sparqlPassword);
        credsProvider.setCredentials(AuthScope.ANY, credentials);
        HttpClient httpclient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();
        String update = "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>  DELETE DATA { GRAPH <https://w3id.org/ozcar-theia/> {<" + uriVariable + "> skos:broader <" + uriCategory + ">}}";
        UpdateExecutionFactory.createRemote(UpdateFactory.create(update), sparqlUrl, httpclient).execute();
    }

    public String getPrefLabel(String uri) {
        String queryString = "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
                + "SELECT ?o FROM <https://w3id.org/ozcar-theia/> WHERE { ?s ?p ?o . FILTER(?s = <" + uri + "> && ?p = skos:prefLabel)}";
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qExec = QueryExecutionFactory.createServiceRequest(sparqlUrl, query);) {
            ResultSet rs = qExec.execSelect();
            return rs.next().get("o").asLiteral().getString();
        }
    }
}
