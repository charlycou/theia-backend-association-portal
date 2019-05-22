/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.DAO;

import fr.theia_land.in_situ.backendspringbootassociationvariable.model.Entities.TheiaVariableName;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 *
 * @author coussotc
 */

public interface TheiaVariableRepository extends MongoRepository<TheiaVariableName, ObjectId> {
    
}
