/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO;

import java.util.List;

/**
 * The observed property is a characteristic of the feature of interest. It identifies or describes the phenomenon for
 * which the observation is made.
 *
 * @author coussotc
 */
public class ObservedProperty {

    /**
     * Name of the variable
     */
    private List<I18n> name;
    /**
     * Unit of the variable
     */
    private List<I18n> unit;
    /**
     * Short description of the variable
     */
    private List<I18n> description;
    /**
     * Gcmd Keywords that the scientist wants to associate to the variable. All the association made by the Theia/OZCAR
     * team will be associated later.
     */
    private List<GcmdKeyword> gcmdKeywords;

    /**
     * A list of URI of Theia categories to be associated
     */
    private List<String> theiaCategories;

    public List<I18n> getName() {
        return name;
    }

    public void setName(List<I18n> name) {
        this.name = name;
    }

    public List<I18n> getUnit() {
        return unit;
    }

    public void setUnit(List<I18n> unit) {
        this.unit = unit;
    }

    public List<I18n> getDescription() {
        return description;
    }

    public void setDescription(List<I18n> description) {
        this.description = description;
    }

    public List<GcmdKeyword> getGcmdKeywords() {
        return gcmdKeywords;
    }

    public void setGcmdKeywords(List<GcmdKeyword> gcmdKeywords) {
        this.gcmdKeywords = gcmdKeywords;
    }

    public List<String> getTheiaCategories() {
        return theiaCategories;
    }

    public void setTheiaCategories(List<String> theiaCategories) {
        this.theiaCategories = theiaCategories;
    }

}
