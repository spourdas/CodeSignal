package com.real.matcher.consumers;

import com.real.matcher.Matcher;
import com.real.matcher.processors.FeedProcessor;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class FeedConsumer extends AbstractKafkaAnalysisConsumer {

    @Override
    void analyzeCSVData(String csvData, List<Matcher.IdMapping> list, String header,
                                           Properties props)
            throws IOException {
        FeedProcessor.parseCSV(csvData, list, header, props);
    }
}
