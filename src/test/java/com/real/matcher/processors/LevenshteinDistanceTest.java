package com.real.matcher.processors;

import com.real.matcher.utils.Utils;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.junit.jupiter.api.Test;

public class LevenshteinDistanceTest {

    @Test
    public void test() {
        String possibility1 = "lrwaence of arabia";
        String possibility2 = "rawlence of arabia";
        String subject = "lawrence of arabia";
        int threshold1 = Utils.calculateLevenshteinThreshold(possibility1, subject,20);
        LevenshteinDistance levenshteinDistance = new LevenshteinDistance(threshold1);
        int distance1 = levenshteinDistance.apply(
                possibility1,
                subject);

        System.out.println(distance1);

        int threshold2 = Utils.calculateLevenshteinThreshold(possibility2, subject,20);
        levenshteinDistance = new LevenshteinDistance(threshold2);
        int distance2 = levenshteinDistance.apply(
                possibility2,
                subject);

        System.out.println(distance2);

    }

    @Test
    public void test2() {
        String possibility1 = "advid Lean";
        String subject =      "david Lean";
        int threshold1 = Utils.calculateLevenshteinThreshold(possibility1, subject,20);
        LevenshteinDistance levenshteinDistance = new LevenshteinDistance(threshold1);
        int distance1 = levenshteinDistance.apply(
                possibility1,
                subject);

        System.out.println(distance1);


    }

}
