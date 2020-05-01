package fr.theia_land.in_situ.backendspringbootassociationvariable.service;

import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ObservedProperty;
import fr.theia_land.in_situ.backendspringbootassociationvariable.model.POJO.ProducerStat;

import java.util.List;

public interface ProducerListService {
    List<ProducerStat> setProducerStats();
    List<List<ObservedProperty>> setProducerVariables(String producerId);
}
