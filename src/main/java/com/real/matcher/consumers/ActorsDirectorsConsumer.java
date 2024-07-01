package com.real.matcher.consumers;

import com.real.matcher.entities.ActorsDirectors;
import com.real.matcher.processors.ActorsDirectorsProcessor;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class ActorsDirectorsConsumer extends AbstractKafkaStoreConsumer {

    @Override
    void storeCSVData(String csvData, Properties props) throws IOException {
        List<ActorsDirectors> actorsDirectorsList = ActorsDirectorsProcessor.parseCSVs(csvData, props);
        ActorsDirectorsProcessor.storeCSVs(actorsDirectorsList);
    }
}
