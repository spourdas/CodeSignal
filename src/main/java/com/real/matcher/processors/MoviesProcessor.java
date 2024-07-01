package com.real.matcher.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.real.matcher.cache.EhcacheManager;
import com.real.matcher.entities.Movies;
import com.real.matcher.utils.Utils;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class MoviesProcessor {

    private static final CsvMapper mapper = new CsvMapper();

    private MoviesProcessor() {
    }

    public static List<Movies> parseCSVs(String csvData, Properties props) throws IOException {
        CsvSchema schema = CsvSchema.builder().addColumn(props.getProperty("movies.field.first"),
                CsvSchema.ColumnType.STRING).addColumn(props.getProperty("movies.field.second"),
                CsvSchema.ColumnType.STRING).addColumn(props.getProperty("movies.field.third"),
                CsvSchema.ColumnType.STRING).build();

        try (MappingIterator<Movies> it = mapper.readerFor(Movies.class).with(schema).readValues(csvData)) {
            return it.readAll();
        }
    }

    public static void storeCSVs(List<Movies> movies) throws JsonProcessingException {

        for (Movies movie : movies) {
            if (movie != null && movie.getTitle() != null) {
                String standardizedTitle = Utils.standardizeName(movie.getTitle());
                ObjectMapper objectMapper = new ObjectMapper();
                movie.setTitle(standardizedTitle);
                String jsonMovie = objectMapper.writeValueAsString(movie);
                Cache moviesCache = EhcacheManager.getMoviesCache(Utils.convertStringToInt(movie.getYear()),
                        standardizedTitle.length());
                Element element = new Element(standardizedTitle, jsonMovie);
                moviesCache.put(element);
            }
        }
    }

}
