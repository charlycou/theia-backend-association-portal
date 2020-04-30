/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO;

import java.util.List;

/**
 * @author coussotc
 */
public class TheiaVariable {
    private String uri;

    private List<I18n> prefLabel;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public List<I18n> getPrefLabel() {
        return prefLabel;
    }

    public void setPrefLabel(List<I18n> prefLabel) {
        this.prefLabel = prefLabel;
    }


}
