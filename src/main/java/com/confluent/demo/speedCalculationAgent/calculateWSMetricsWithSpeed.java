/**
 * Copyright 2025 Confluent Inc. All Rights Reserved
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * This code was written as part of a Demonstration for how to quickly get started with 
 * Confluent Cloud.  This code is NOT built / intended to be used for any Production purposes
 * and should only be considered for prototyping / experimental uses only
 * 
 * Any questions on this please reach out to Confluent Professional Services in your
 * respective area
 */

package com.confluent.demo.speedCalculationAgent;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;

//This helper function is needed for Confluent Cloud UDF definition to ensure it can scrape out the meta DATA
@FunctionHint(output = @DataTypeHint("ROW<distance DOUBLE, bearing DOUBLE, heightDifference DOUBLE, glideRatio DOUBLE, speed DOUBLE>"))
/**
 * This class is basically the UDF which is used by flink to complete the actual Calculation of distance, bearing, height difference, glide ratio and speed.
 */
public class calculateWSMetricsWithSpeed extends TableFunction<Row> {

    private static final long serialVersionUID = 1L;
	private static final double EARTH_RADIUS = 6371000; // Earth radius in meters

    /**
     * Calculates the distance, bearing, speed, and height difference between two geographical coordinates.
     * 
     * VERY IMPORTANT!  The Params here need to MATCH the args else Confluent Cloud will have issues with Signatures / metadata
     *
     * @param lat1 Latitude of the first point in degrees.
     * @param lon1 Longitude of the first point in degrees.
     * @param lat2 Latitude of the second point in degrees.
     * @param lon2 Longitude of the second point in degrees.
     * @param h1   Height above sea level of the first point in meters.
     * @param h2   Height above sea level of the second point in meters.
     * @param timestamp2 timestamp used for speed calc
     * @param previousTimeStamp used as the previous timestamp
     */
    
    public void eval(Double lat1, Double lon1, Double lat2, Double lon2, Double h1, Double h2, String timestamp2, String previousTimestamp) {
        
    	//This is the Haversine Function to calculate the distance between two GPS points
    	double radLat1 = Math.toRadians(lat1);
        double radLat2 = Math.toRadians(lat2);

        double dLon = Math.toRadians(lon2 - lon1);
        double dLat = Math.toRadians(lat2 - lat1);

        double y = Math.sin(dLon) * Math.cos(radLat2);
        double x = Math.cos(radLat1) * Math.sin(radLat2) -
                   Math.sin(radLat1) * Math.cos(radLat2) * Math.cos(dLon);
        
        // work out the bearing vector
        double bearing = Math.toDegrees(Math.atan2(y, x));
        bearing = (bearing + 360) % 360; // Normalize to 0-360 range

        double a = Math.pow(Math.sin(dLat / 2), 2) +
                   Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(dLon / 2), 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = EARTH_RADIUS * c;

        double heightDifference = h2 - h1; // Simple height difference
        double glideRatio = 0.0; // Default value
        
        // calculate the GR based on Pythagorean theorem
        if (heightDifference != 0) {
            double glidePathDistance = Math.sqrt(Math.pow(distance, 2) + Math.pow(heightDifference, 2));
            glideRatio = glidePathDistance / Math.abs(heightDifference);
        }
        //complete speed calculation
        double speed; // Default speed        
        speed = distance / ((convertStringToTimestaamp(timestamp2) - convertStringToTimestaamp(previousTimestamp)) / 1000.0); // Speed in m/s
        
		
        collect(Row.of(distance, bearing, heightDifference, glideRatio, speed));

    }//end of function eval
    
    /**
     * Simple helper function that converts a String into a timestamp
     * @param timestamp
     * @return a timestamp in long format so it can be used in a speed calculation.
     */
    private long convertStringToTimestaamp(String timestamp)
    {
    	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS");
    	LocalDateTime dateTime = LocalDateTime.parse(timestamp, formatter);
    	
    	return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }//end of function convertStringToTimestamp
    
}//end of class