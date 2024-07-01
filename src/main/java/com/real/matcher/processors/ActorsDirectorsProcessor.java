package com.real.matcher.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.real.matcher.cache.EhcacheManager;
import com.real.matcher.dto.ActorsDirectorsDTO;
import com.real.matcher.entities.ActorsDirectors;
import com.real.matcher.utils.Utils;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ActorsDirectorsProcessor {

    private static final CsvMapper mapper = new CsvMapper();

    private ActorsDirectorsProcessor() {
    }

    public static List<ActorsDirectors> parseCSVs(String csvData, Properties props) throws IOException {
        CsvSchema schema = CsvSchema.builder().addColumn(props.getProperty("actors.directors.field.first"),
                CsvSchema.ColumnType.STRING).addColumn(props.getProperty("actors.directors.field.second"),
                CsvSchema.ColumnType.STRING).addColumn(props.getProperty("actors.directors.field.third"),
                CsvSchema.ColumnType.STRING).build();

        try (MappingIterator<ActorsDirectors> it = mapper.readerFor(
                ActorsDirectors.class).with(schema).readValues(csvData)) {
            return it.readAll();
        }
    }

    public static void storeCSVs(List<ActorsDirectors> actorsDirectors) throws JsonProcessingException {

        for (ActorsDirectors actorsDirector : actorsDirectors) {
            if (actorsDirector != null && actorsDirector.getName() != null) {
                if (actorsDirector.getRole().equalsIgnoreCase("director")) {
                    addDirectorToCache(actorsDirector);
                } else {
                    addActorToCache(actorsDirector);
                }
            }
        }
    }

    private static void addActorToCache(ActorsDirectors actorsDirector) throws JsonProcessingException {
        Cache actorsDirectorsCache = EhcacheManager.getActorsDirectorsCache();
        Element element = actorsDirectorsCache.get(actorsDirector.getId());
        ActorsDirectorsDTO directorsRTO;
        List<String> actorsList;
        if (element == null) {
            directorsRTO = new ActorsDirectorsDTO();
            directorsRTO.setId(Utils.convertStringToInt(actorsDirector.getId()));
            actorsList = new ArrayList<>();
        } else {
            String json = (String) element.getObjectValue();
            ObjectMapper objectMapper = new ObjectMapper();
            directorsRTO = objectMapper.readValue(json, ActorsDirectorsDTO.class);
            actorsList = directorsRTO.getActors();
            if (actorsList == null) {
                actorsList = new ArrayList<>();
            }
        }
        String standardizedName = Utils.standardizeName(actorsDirector.getName());
        actorsList.add(standardizedName);
        directorsRTO.setActors(actorsList);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(directorsRTO);
        element = new Element(actorsDirector.getId(), json);
        actorsDirectorsCache.put(element);
    }

    private static void addDirectorToCache(ActorsDirectors actorsDirector) throws JsonProcessingException {
        Cache actorsDirectorsCache = EhcacheManager.getActorsDirectorsCache();
        Element element = actorsDirectorsCache.get(actorsDirector.getId());
        ActorsDirectorsDTO directorsRTO;
        if (element == null) {
            directorsRTO = new ActorsDirectorsDTO();
            directorsRTO.setId(Utils.convertStringToInt(actorsDirector.getId()));
        } else {
            String json = (String) element.getObjectValue();
            ObjectMapper objectMapper = new ObjectMapper();
            directorsRTO = objectMapper.readValue(json, ActorsDirectorsDTO.class);
        }
        String standardizedName = Utils.standardizeName(actorsDirector.getName());
        directorsRTO.setDirector(standardizedName);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(directorsRTO);
        element = new Element(actorsDirector.getId(), json);
        actorsDirectorsCache.put(element);
    }
}
