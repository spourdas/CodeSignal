package com.real.matcher.processors;

import com.real.matcher.Matcher;
import com.real.matcher.cache.EhcacheManager;
import com.real.matcher.entities.ActorsDirectors;
import com.real.matcher.entities.Movies;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class FeedProcessorTest {

    private static Properties feedProps = null;
    private static Properties configProps = null;

    @BeforeAll
    public static void beforeAll() {
        feedProps = new Properties();
        feedProps.put("header.column.title", "Title");
        feedProps.put("header.column.id", "MediaId");
        feedProps.put("header.column.date", "OriginalReleaseDate");
        feedProps.put("header.column.date.format", "M/d/yyyy h:mm:ss a");
        feedProps.put("header.column.actors", "Actors");
        feedProps.put("header.column.director", "Director");
        feedProps.put("strings.levenshtein.acceptance.threshold.percentage", "20");

        configProps = new Properties();

        configProps.put("movies.field.first", "id");
        configProps.put("movies.field.second", "title");
        configProps.put("movies.field.third", "year");

        configProps.put("actors.directors.field.first", "id");
        configProps.put("actors.directors.field.second", "name");
        configProps.put("actors.directors.field.third", "role");


    }

    @BeforeEach
    public void before() {
        EhcacheManager.clearAll();
    }

    public String buildHeader() {
        return "Country,StudioNetwork,MediaId,Title,OriginalReleaseDate,MediaType,SeriesMediaId,SeasonMediaId," +
                "SeriesTitle,SeasonNumber,EpisodeNumber,LicenseType,ActualRetailPrice,OfferStartDate,OfferEndDate," +
                "Actors,Director,XboxLiveURL";
    }

    public String buildFeedCSVLine(String id, String title, String date, String actors, String director) {
        return "Country,StudioNetwork," + id + "," + title + "," + date +
                ",MediaType,SeriesMediaId,SeasonMediaId,SeriesTitle,SeasonNumber,EpisodeNumber,LicenseType" +
                ",ActualRetailPrice,OfferStartDate,OfferEndDate," + actors + "," + director +
                ",XboxLiveURL";
    }

    public String buildMovieLine(String movieId, String title, String year) {
        return movieId + "," + title + "," + year;
    }

    public String builedActorDirectorLine(String movieId, String name, String role) {
        return movieId + "," + name + "," + role;
    }

    public static List<String> generatePartialPermutations(String s, int k) {
        List<String> result = new ArrayList<>();
        if (s == null || s.isEmpty() || k <= 0 || k > s.length()) {
            return result;
        }

        char[] chars = s.toCharArray();
        boolean[] used = new boolean[k];
        StringBuilder current = new StringBuilder();

        permute(chars, used, current, result, k, 0);

        return result;
    }

    private static void permute(char[] chars, boolean[] used, StringBuilder current,
                                List<String> result, int k, int depth) {
        if (depth == k) {
            result.add(current.toString());
            return;
        }

        for (int i = 0; i < k; i++) {
            if (!used[i]) {
                used[i] = true;
                current.append(chars[i]);
                permute(chars, used, current, result, k, depth + 1);
                current.setLength(current.length() - 1);
                used[i] = false;
            }
        }
    }

    private List<Movies> generateCloseMovieMatches(String movieTitle, int numChars, String year) throws IOException {
        List<String> permutations = generatePartialPermutations(movieTitle, numChars);
        int id = 1;
        List<Movies> allMovies = new ArrayList<>();
        for (String newString : permutations) {
            String modifiedString = newString + movieTitle.substring(numChars);
            if (!modifiedString.equals(movieTitle)) {
                List<Movies> movies = MoviesProcessor.parseCSVs(buildMovieLine(Integer.toString(id), modifiedString,
                                year),
                        configProps);
                allMovies.addAll(movies);
                id++;
            }
        }
        return allMovies;
    }

    private List<ActorsDirectors> generateCloseActorDirectorMatches(int movieId, String name, String role, int numChars)
            throws IOException {
        List<String> permutations = generatePartialPermutations(name, numChars);
        List<ActorsDirectors> allDirectors = new ArrayList<>();
        for (String newString : permutations) {
            String modifiedString = newString + name.substring(numChars);
            if (!modifiedString.equals(name)) {
                List<ActorsDirectors> directors = ActorsDirectorsProcessor.parseCSVs(builedActorDirectorLine(
                        Integer.toString(movieId), modifiedString, role), configProps);
                allDirectors.addAll(directors);
                break;
            }
        }
        return allDirectors;
    }

    private String buildDateFromYear(String year) {
        return "1/30/" + year + " 8:01:00 AM";
    }

    @Test
    void testExactMovieMatch() throws IOException {
        // test no actors
        // test scoring algorithm
        // test record handled already
        // test different string sizes and thresholds
        // test non standardized text

        String movieTitle = "Lawrence of Arabia";
        String year = "2012";

        List<Movies> movies = MoviesProcessor.parseCSVs(buildMovieLine("1",
                movieTitle, year), configProps);
        MoviesProcessor.storeCSVs(movies);

        UUID uuid = UUID.randomUUID();
        String csvLine = buildFeedCSVLine(uuid.toString(), movieTitle, buildDateFromYear(year),
                null, null);

        List<Matcher.IdMapping> inMappings = new ArrayList<>();
        FeedProcessor.parseCSV(csvLine, inMappings, buildHeader(), feedProps);
        assertFalse(inMappings.isEmpty());
        assertEquals(1, inMappings.size());

        for (Matcher.IdMapping mapping : inMappings) {
            assertEquals(1, mapping.getInternalId());
            assertEquals(uuid.toString(), mapping.getExternalId());
        }
    }

    @Test
    void testCloseMovieMatches() throws IOException {

        String movieTitle = "Lawrence of Arabia";
        String year = "1962";

        // Attach a movie name that is only two chars out of place (as opposed to 3) to movie id 1
        List<Movies> movies = generateCloseMovieMatches(movieTitle, 2, year);
        List<Movies> movies2 = generateCloseMovieMatches(movieTitle, 3, year);
        movies2.remove(0);
        movies2.addAll(movies);
        MoviesProcessor.storeCSVs(movies);

        List<Matcher.IdMapping> inMappings = new ArrayList<>();

        UUID uuid = UUID.randomUUID();

        String csvLine = buildFeedCSVLine(uuid.toString(), movieTitle, buildDateFromYear(year),
                null, null);

        FeedProcessor.parseCSV(csvLine, inMappings, buildHeader(), feedProps);

        assertFalse(inMappings.isEmpty());
        assertEquals(1, inMappings.size());
        assertEquals(1, inMappings.get(0).getInternalId());
        assertEquals(inMappings.get(0).getExternalId(), uuid.toString());
    }

    @Test
    void testCloseMovieMatchesWithExactDirector() throws IOException {

        String movieTitle = "Lawrence of Arabia";
        String year = "1962";
        List<Movies> movies = generateCloseMovieMatches(movieTitle, 4, year);
        MoviesProcessor.storeCSVs(movies);

        // Attach an exact director name to movie id 3
        String director = "David Lean";
        List<ActorsDirectors> directors = ActorsDirectorsProcessor.parseCSVs(builedActorDirectorLine(
                "3", director, "director"), configProps);
        ActorsDirectorsProcessor.storeCSVs(directors);

        List<Matcher.IdMapping> inMappings = new ArrayList<>();

        UUID uuid = UUID.randomUUID();
        String csvLine = buildFeedCSVLine(uuid.toString(), movieTitle, buildDateFromYear(year),
                null, director);
        FeedProcessor.parseCSV(csvLine, inMappings, buildHeader(), feedProps);
        assertFalse(inMappings.isEmpty());
        assertEquals(1, inMappings.size());
        assertEquals(3, inMappings.get(0).getInternalId());
        assertEquals(inMappings.get(0).getExternalId(), uuid.toString());
    }

    @Test
    void testCloseMovieMatchesWithCloseDirectorMatches() throws IOException {

        String movieTitle = "Lawrence of Arabia";
        String year = "1962";
        List<Movies> movies = generateCloseMovieMatches(movieTitle, 4, year);
        MoviesProcessor.storeCSVs(movies);

        String director = "David Lean";

        // Attach one director with name that has two chars out of place to movie id 3
        List<ActorsDirectors> directors = generateCloseActorDirectorMatches(3, director, "director", 2);
        ActorsDirectorsProcessor.storeCSVs(directors);

        List<Matcher.IdMapping> inMappings = new ArrayList<>();

        UUID uuid = UUID.randomUUID();
        String csvLine = buildFeedCSVLine(uuid.toString(), movieTitle, buildDateFromYear(year),
                null, director);
        FeedProcessor.parseCSV(csvLine, inMappings, buildHeader(), feedProps);
        assertFalse(inMappings.isEmpty());
        assertEquals(1, inMappings.size());
        assertEquals(3, inMappings.get(0).getInternalId());
        assertEquals(inMappings.get(0).getExternalId(), uuid.toString());
    }

    @Test
    void testCloseMovieMatchesWithExactActor() throws IOException {

        String movieTitle = "Lawrence of Arabia";
        String year = "1962";
        List<Movies> movies = generateCloseMovieMatches(movieTitle, 4, year);
        MoviesProcessor.storeCSVs(movies);

        // Attach an exact actor name to movie id 3
        String actor = "Anthony Quinn";
        List<ActorsDirectors> actors = ActorsDirectorsProcessor.parseCSVs(builedActorDirectorLine(
                "3", actor, "cast"), configProps);
        ActorsDirectorsProcessor.storeCSVs(actors);

        List<Matcher.IdMapping> inMappings = new ArrayList<>();

        UUID uuid = UUID.randomUUID();
        String csvLine = buildFeedCSVLine(uuid.toString(), movieTitle, buildDateFromYear(year),
                actor, null);
        FeedProcessor.parseCSV(csvLine, inMappings, buildHeader(), feedProps);
        assertFalse(inMappings.isEmpty());
        assertEquals(1, inMappings.size());
        assertEquals(3, inMappings.get(0).getInternalId());
        assertEquals(inMappings.get(0).getExternalId(), uuid.toString());
    }

    @Test
    void testCloseMovieMatchesWithCloseActorMatches() throws IOException {

        String movieTitle = "Lawrence of Arabia";
        String year = "1962";
        List<Movies> movies = generateCloseMovieMatches(movieTitle, 4, year);
        MoviesProcessor.storeCSVs(movies);

        // Attach one actor with name that has two chars out of place to movie id 3
        String actor = "Anthony Quinn";
        List<ActorsDirectors> actors = generateCloseActorDirectorMatches(3, actor, "cast", 2);
        ActorsDirectorsProcessor.storeCSVs(actors);

        List<Matcher.IdMapping> inMappings = new ArrayList<>();

        UUID uuid = UUID.randomUUID();
        String csvLine = buildFeedCSVLine(uuid.toString(), movieTitle, buildDateFromYear(year),
                actor, null);
        FeedProcessor.parseCSV(csvLine, inMappings, buildHeader(), feedProps);
        assertFalse(inMappings.isEmpty());
        assertEquals(1, inMappings.size());
        assertEquals(3, inMappings.get(0).getInternalId());
        assertEquals(inMappings.get(0).getExternalId(), uuid.toString());
    }


}
