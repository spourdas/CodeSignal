package com.real.matcher.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ActorsDirectorsDTO {
    private int id;
    private String director;
    private List<String> actors;
}
