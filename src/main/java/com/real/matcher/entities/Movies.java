package com.real.matcher.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Movies {
    private String id;
    private String title;
    private String year;
    private int matchScore;
}
