/*
Schema migration script for Initialization.
*/

INSTALL sqlite;
INSTALL ducklake;

ATTACH 'ducklake:sqlite:metadata.sqlite' as dl (DATA_PATH 'data_files/');
USE dl;

/* create schema */
CREATE SCHEMA IF NOT EXISTS dl.raw;
CREATE SCHEMA IF NOT EXISTS dl.staging;
CREATE SCHEMA IF NOT EXISTS dl.final;

