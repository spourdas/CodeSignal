package com.real.matcher.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.real.matcher.Matcher;
import com.real.matcher.cache.EhcacheManager;
import com.real.matcher.dto.ActorsDirectorsDTO;
import com.real.matcher.entities.Movies;
import com.real.matcher.utils.Utils;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.text.similarity.LevenshteinDistance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FeedProcessor {

    private FeedProcessor() {
    }

    public static void parseCSV(String csvData, List<Matcher.IdMapping> idMappings, String header,
                                Properties props)
            throws IOException {

        Cache movieVisitedCache = EhcacheManager.getMoviesVisitedCache();

        String[] headers = header.split(",");

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(headers)
                .setSkipHeaderRecord(false)
                .build();

        String title = "";
        String feedId = "";
        String date = "";
        String actors = "";
        String director = "";
        try (Reader reader = new BufferedReader(new StringReader(csvData))) {
            Iterable<CSVRecord> records = csvFormat.parse(reader);
            for (CSVRecord csvRecord : records) {
                title = csvRecord.get(props.getProperty("header.column.title"));
                feedId = csvRecord.get(props.getProperty("header.column.id"));
                date = csvRecord.get(props.getProperty("header.column.date"));
                actors = csvRecord.get(props.getProperty("header.column.actors"));
                director = csvRecord.get(props.getProperty("header.column.director"));
            }
        }

        // MOVIE TITLE
        String standardizedTitle = Utils.standardizeName(title);

        boolean visited = true;
        synchronized (FeedProcessor.class) {
            if (movieVisitedCache.get(feedId) == null) {
                Element element = new Element(feedId, "true");
                movieVisitedCache.put(element);
                visited = false;
            }
        }
        if (!visited) {

            Element element = new Element(feedId, "true");
            movieVisitedCache.put(element);

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                    props.getProperty("header.column.date.format"));
            LocalDateTime dateTime = LocalDateTime.parse(date, formatter);

            // MOVIE YEAR
            int year = dateTime.getYear();

            // MOVIE DIRECTOR
            String standardizedDirector = Utils.standardizeName(director);

            // MOVIE ACTORS
            List<String> feedActorsList = parseFeedActors(actors);

            Cache moviesCache = EhcacheManager.getMoviesCache(year, standardizedTitle.length());

            if (moviesCache.get(standardizedTitle) != null) {

                pickTheExactMovieMatch(idMappings, year, standardizedTitle, feedId);

            } else {

                pickTheBestMatch(idMappings, props, standardizedTitle, year, standardizedDirector, feedActorsList,
                        feedId);
            }
        }

    }

    private static List<String> parseFeedActors(String actors) {
        List<String> feedActorsList = new ArrayList<>();
        if (actors != null && !actors.isBlank()) {
            String[] actorsArray = actors.split(",");
            for (String actor : actorsArray) {
                if (!actor.isEmpty()) {
                    feedActorsList.add(Utils.standardizeName(actor));
                }
            }
        }
        return feedActorsList;
    }

    private static void pickTheBestMatch(List<Matcher.IdMapping> idMappings, Properties props, String standardizedTitle, int year, String standardizedDirector, List<String> feedActorsList, String feedId) throws JsonProcessingException {
        int acceptancePercentage = Integer.parseInt(
                props.getProperty("strings.levenshtein.acceptance.threshold.percentage"));
        List<Movies> possibleMovies = gatherPossibleMovieMatchesBasedOnMovieTitle(standardizedTitle, year,
                acceptancePercentage);
        if (!standardizedDirector.isEmpty() && possibleMovies.size() > 1) {
            scorePossibleMovieMatchesBasedOnDirector(possibleMovies, standardizedDirector,
                    acceptancePercentage);
        }
        if (!feedActorsList.isEmpty() && possibleMovies.size() > 1) {
            scorePossibleMovieMatchesBasedOnActors(possibleMovies, feedActorsList, acceptancePercentage);
        }
        if (!possibleMovies.isEmpty()) {
            if (possibleMovies.size() == 1) {
                pickTheOnlyMovieMatchLeft(idMappings, possibleMovies, feedId);
            } else {
                pickPossibleMovieMatchThatHasHighestScore(idMappings, possibleMovies, feedId);
            }
        }
    }

    private static void pickTheExactMovieMatch(List<Matcher.IdMapping> idMappings, int year, String standardizedTitle,
                                               String feedId) throws JsonProcessingException {
        Cache moviesCache = EhcacheManager.getMoviesCache(year, standardizedTitle.length());
        Element element = moviesCache.get(standardizedTitle);
        String json = (String) element.getObjectValue();
        ObjectMapper objectMapper = new ObjectMapper();
        Movies movie = objectMapper.readValue(json, Movies.class);
        Matcher.IdMapping idMapping = new Matcher.IdMapping(Utils.convertStringToInt(movie.getId()),
                feedId);
        idMappings.add(idMapping);
    }

    private static void pickTheOnlyMovieMatchLeft(List<Matcher.IdMapping> idMappings, List<Movies> possibleMovies,
                                                  String feedId) {
        Movies movieMatched = possibleMovies.get(0);
        Matcher.IdMapping idMapping = new Matcher.IdMapping(Utils.convertStringToInt(movieMatched.
                getId()), feedId);
        idMappings.add(idMapping);
    }

    private static void pickPossibleMovieMatchThatHasHighestScore(List<Matcher.IdMapping> idMappings,
                                                                  List<Movies> possibleMovies, String id) {
        int highestScore = 0;
        Movies movieSelected = null;
        for (Movies movie : possibleMovies) {
            if (movie.getMatchScore() >= highestScore) {
                highestScore = movie.getMatchScore();
                movieSelected = movie;
            }
        }
        if (movieSelected != null && movieSelected.getId() != null) {
            Matcher.IdMapping idMapping = new Matcher.IdMapping(Utils.convertStringToInt(movieSelected.getId()), id);
            idMappings.add(idMapping);
        }
    }

    private static void scorePossibleMovieMatchesBasedOnActors(List<Movies> possibleMovies,
                                                               List<String> feedActorsList,
                                                               int acceptancePercentage)
            throws JsonProcessingException {
        Cache actorsCache = EhcacheManager.getActorsDirectorsCache();
        for (Movies movie : possibleMovies) {
            Element element = actorsCache.get(movie.getId());
            if (element != null) {
                String json = (String) element.getObjectValue();
                ObjectMapper objectMapper = new ObjectMapper();
                ActorsDirectorsDTO actorsDirectorsDTO = objectMapper.readValue(json,
                        ActorsDirectorsDTO.class);
                List<String> movieActors = actorsDirectorsDTO.getActors();
                scoreMoviesFoundBasedOnFeedActors(feedActorsList, acceptancePercentage, movie, movieActors, actorsDirectorsDTO);
            }
        }
    }

    private static void scoreMoviesFoundBasedOnFeedActors(List<String> feedActorsList, int acceptancePercentage,
                                                          Movies movie, List<String> movieActors,
                                                          ActorsDirectorsDTO actorsDirectorsDTO) {
        if (movieActors != null && !movieActors.isEmpty()) {
            for (String movieActor : movieActors) {
                extractMoviesActorWithFeedActor(feedActorsList, acceptancePercentage, movie, actorsDirectorsDTO, movieActor);
            }
        }
    }

    private static void extractMoviesActorWithFeedActor(List<String> feedActorsList, int acceptancePercentage, Movies movie, ActorsDirectorsDTO actorsDirectorsDTO, String movieActor) {
        for (String feedActor : feedActorsList) {
            if (actorsDirectorsDTO.getActors() != null) {
                int threshold = Utils.calculateLevenshteinThreshold(
                        movieActor, feedActor, acceptancePercentage);
                LevenshteinDistance levenshteinDistance = new
                        LevenshteinDistance(threshold);
                int distance = levenshteinDistance.apply(
                        movieActor,
                        feedActor);
                if (distance <= threshold && distance >= 0) {
                    movie.setMatchScore(movie.getMatchScore() + (threshold - distance + 1));
                }
            }
        }
    }

    private static void scorePossibleMovieMatchesBasedOnDirector(List<Movies> possibleMovies,
                                                                 String standardizedDirector,
                                                                 int acceptancePercentage)
            throws JsonProcessingException {

        Cache directorsCache = EhcacheManager.getActorsDirectorsCache();

        for (Movies movie : possibleMovies) {
            Element element = directorsCache.get(movie.getId());
            if (element != null) {
                String json = (String) element.getObjectValue();
                ObjectMapper objectMapper = new ObjectMapper();
                ActorsDirectorsDTO actorsDirectorsDTO = objectMapper.readValue(json,
                        ActorsDirectorsDTO.class);
                if (actorsDirectorsDTO.getDirector() != null) {
                    int threshold = Utils.calculateLevenshteinThreshold(
                            actorsDirectorsDTO.getDirector(), standardizedDirector, acceptancePercentage);
                    LevenshteinDistance levenshteinDistance = new
                            LevenshteinDistance(threshold);
                    int distance = levenshteinDistance.apply(
                            actorsDirectorsDTO.getDirector(),
                            standardizedDirector);
                    if (distance <= threshold && distance >= 0) {
                        movie.setMatchScore(movie.getMatchScore() + (threshold - distance + 1));
                    }
                }
            }

        }
    }

    private static List<Movies> gatherPossibleMovieMatchesBasedOnMovieTitle(String standardizedTitle, int year,
                                                                            int acceptancePercentage)
            throws JsonProcessingException {
        Cache moviesCache = EhcacheManager.getMoviesCache(year, standardizedTitle.length());
        List<Movies> possibleMovies = new ArrayList<>();
        List<String> movies = moviesCache.getKeys();
        for (String movieKey : movies) {
            Element element = moviesCache.get(movieKey);
            String json = (String) element.getObjectValue();
            ObjectMapper objectMapper = new ObjectMapper();
            Movies movie = objectMapper.readValue(json, Movies.class);
            if (movie.getTitle() != null) {
                int threshold = Utils.calculateLevenshteinThreshold(movie.getTitle(), standardizedTitle,
                        acceptancePercentage);
                LevenshteinDistance levenshteinDistance = new LevenshteinDistance(threshold);
                int distance = levenshteinDistance.apply(
                        movie.getTitle(),
                        standardizedTitle);

                if (distance <= threshold && distance >= 0) {
                    movie.setMatchScore(threshold - distance + 1);
                    possibleMovies.add(movie);
                }
            }
        }
        return possibleMovies;
    }

}
