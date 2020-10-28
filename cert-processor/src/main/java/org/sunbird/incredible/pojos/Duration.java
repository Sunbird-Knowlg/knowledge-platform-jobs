package org.sunbird.incredible.pojos;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Date;

@JsonSerialize
public class Duration {
    private Date startDate;
    private Date endDate;
}
