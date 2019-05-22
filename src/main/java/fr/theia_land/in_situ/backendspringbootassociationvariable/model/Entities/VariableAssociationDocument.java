/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.model.Entities;

import java.util.Date;
import java.util.List;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 *
 * @author coussotc
 */
@Document(collection = "variableAssociations")
public class VariableAssociationDocument {
    private ObjectId _id;
    String producerId;
    List<String> theiaCategories;
    String producerVariableName;
    Date lastModified;

    public ObjectId getId() {
        return _id;
    }

    public void setId(ObjectId _id) {
        this._id = _id;
    }

    public String getProducerId() {
        return producerId;
    }

    public void setProducerId(String producerId) {
        this.producerId = producerId;
    }

    public List<String> getTheiaCategories() {
        return theiaCategories;
    }

    public void setTheiaCategories(List<String> theiaCategories) {
        this.theiaCategories = theiaCategories;
    }

    public String getProducerVariableName() {
        return producerVariableName;
    }

    public void setProducerVariableName(String producerVariableName) {
        this.producerVariableName = producerVariableName;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }
}
