package com.real.matcher.consumers;

import com.real.matcher.entities.Movies;
import com.real.matcher.processors.MoviesProcessor;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class MoviesConsumer extends AbstractKafkaStoreConsumer {

    @Override
    void storeCSVData(String csvData, Properties props) throws IOException {
        List<Movies> movieList = MoviesProcessor.parseCSVs(csvData, props);
        MoviesProcessor.storeCSVs(movieList);
    }

}
