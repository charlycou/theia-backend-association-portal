/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.theia_land.in_situ.backendspringbootassociationvariable.CustomConfig;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.WriteResultChecking;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

/**
 * @Configuration: allow to register extra Spring beans in the context or import additional configuration classes
 */
@Configuration
public class BackendSpringbootAssociationVariableApplicationConfiguration extends AbstractMongoConfiguration {
    @Value("${spring.data.mongodb.uri}")
    private String uri;
    @Value("${spring.data.mongodb.database}")
    private String database;

    /**
     * Configure the MongoClient with the uri
     *
     * @return MongoClient.class
     */
    @Override
    public MongoClient mongoClient() {
        return new MongoClient(new MongoClientURI(uri));
    }

    /**
     * Database name getter
     *
     * @return the database the query will be performed
     */
    @Override
    protected String getDatabaseName() {
        return database;
    }

    /**
     * @return MongoTemplate.class
     * @Bean: explicitly declare that a method produces a Spring bean to be managed by the Spring container.
     * Configuration of the MongoTemplate with the newly defined custom converters. The MongoTemplate class is the
     * central class of Spring’s MongoDB support and provides a rich feature set for interacting with the database. The
     * template offers convenience operations to create, update, delete, and query MongoDB documents and provides a
     * mapping between your domain objects and MongoDB documents.
     */
    @Bean
    @Override
    public MongoTemplate mongoTemplate() {
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient(), getDatabaseName());
        MappingMongoConverter mongoMapping = (MappingMongoConverter) mongoTemplate.getConverter();
        mongoTemplate.setWriteResultChecking(WriteResultChecking.EXCEPTION);
        mongoTemplate.setWriteConcern(WriteConcern.MAJORITY);
        mongoMapping.setCustomConversions(customConversions()); // tell mongodb to use the custom converters
        mongoMapping.afterPropertiesSet();
        return mongoTemplate;
    }
}
