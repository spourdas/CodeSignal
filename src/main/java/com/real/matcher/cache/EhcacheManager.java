package com.real.matcher.cache;

import com.real.matcher.MatcherException;
import com.real.matcher.MatcherImpl;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class EhcacheManager {
    public static final String MOVIES_CACHE_PREFIX = "MoviesCache";
    public static final String ACTORS_DIRECTORS_CACHE = "ActorsDirectorsCache";
    public static final String MOVIES_VISITED_CACHE = "MoviesVisitedCache";

    private static final CacheManager cacheManager;
    private static final Properties props;

    private EhcacheManager() {
    }

    static {

        props = new Properties();
        String fileName = "/cache.properties";
        InputStream input = MatcherImpl.class.getResourceAsStream(fileName);
        try {
            props.load(input);
        } catch (IOException e) {
            throw new MatcherException("Unable to load " + fileName, e);
        }

        cacheManager = CacheManager.create();

    }

    public static synchronized Cache getMoviesCache(int year, int stringLength) {
        int bucketSize = Integer.parseInt(props.getProperty("cache.movies.names.bucket.size"));
        int maxElementsInMemory = Integer.parseInt(props.getProperty("cache.movies.max.elements.in.memory"));
        String cacheName = MOVIES_CACHE_PREFIX + "_" + year + "_" + ((stringLength / bucketSize) + 1);
        if (!cacheManager.cacheExists(cacheName)) {
            Cache cache = new Cache(cacheName, maxElementsInMemory, null, true, null,
                    true, 0, 0, false, 0, null);
            cacheManager.addCache(cache);
            return cache;
        } else {
            return cacheManager.getCache(cacheName);
        }
    }

    public static synchronized Cache getActorsDirectorsCache() {
        int maxElementsInMemory = Integer.parseInt(props.getProperty("cache.actors.director.max.elements.in.memory"));
        String cacheName = ACTORS_DIRECTORS_CACHE;
        if (!cacheManager.cacheExists(cacheName)) {
            Cache cache = new Cache(cacheName, maxElementsInMemory, null, true, null,
                    true, 0, 0, false, 0, null);
            cacheManager.addCache(cache);
            return cache;
        } else {
            return cacheManager.getCache(cacheName);
        }
    }

    public static synchronized Cache getMoviesVisitedCache() {
        int maxElementsInMemory = Integer.parseInt(props.getProperty("cache.movies.visited.max.elements.in.memory"));
        if (!cacheManager.cacheExists(MOVIES_VISITED_CACHE)) {
            Cache cache = new Cache(MOVIES_VISITED_CACHE, maxElementsInMemory, null, true, null,
                    true, 0, 0, false, 0, null);
            cacheManager.addCache(cache);
            return cache;
        } else {
            return cacheManager.getCache(MOVIES_VISITED_CACHE);
        }
    }

    public static void clearAll() {
        cacheManager.clearAll();

    }

    public static void shutdownCacheManager() {
        cacheManager.shutdown();
    }

}
