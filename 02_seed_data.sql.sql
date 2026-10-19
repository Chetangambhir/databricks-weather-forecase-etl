-- Databricks notebook source
-- Seed data for config.locations

INSERT INTO `workspace`.`weather_config`.`locations` (location_id, city_name, latitude, longitude, local_timezone, enabled) VALUES ('DEL', 'Delhi', 28.6139, 77.209, 'Asia/Kolkata', true);
INSERT INTO `workspace`.`weather_config`.`locations` (location_id, city_name, latitude, longitude, local_timezone, enabled) VALUES ('HYD', 'Hyderabad', 17.385, 78.4867, 'Asia/Kolkata', true);
INSERT INTO `workspace`.`weather_config`.`locations` (location_id, city_name, latitude, longitude, local_timezone, enabled) VALUES ('LON', 'London', 51.5074, -0.1278, 'Europe/London', true);
INSERT INTO `workspace`.`weather_config`.`locations` (location_id, city_name, latitude, longitude, local_timezone, enabled) VALUES ('SG', 'Singapore', 1.3521, 103.8198, 'Asia/Singapore', true);
INSERT INTO `workspace`.`weather_config`.`locations` (location_id, city_name, latitude, longitude, local_timezone, enabled) VALUES ('SYD', 'Sydney', -33.8688, 151.2093, 'Australia/Sydney', true);