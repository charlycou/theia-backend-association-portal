package fr.theia_land.in_situ.backendspringbootassociationvariable.service;

import fr.theia_land.in_situ.backendspringbootassociationvariable.DAO.MongoDbUtils;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ObservedProperty;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ProducerStat;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class ProducerListServiceImpl implements ProducerListService {
    private final  MongoDbUtils mongoDbUtils;

    @Autowired
    public ProducerListServiceImpl(MongoDbUtils mongoDbUtils) {
        this.mongoDbUtils = mongoDbUtils;
    }

    @Override
    public List<ProducerStat> setProducerStats() {
        return mongoDbUtils.setProducerStats();
    }

    @Override
    public List<List<ObservedProperty>> setProducerVariables(String producerId) {
        return mongoDbUtils.getDifferentProducerVariableSorted(producerId);
    }
}
