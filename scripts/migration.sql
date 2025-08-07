/*
Schema migration script for Initialization.
*/

INSTALL sqlite;
INSTALL ducklake;

ATTACH 'ducklake:sqlite:_build/metadata.sqlite' as dl (DATA_PATH '_build/data_files/');
USE dl;

/* create schema */
CREATE SCHEMA IF NOT EXISTS dl.bronze;
CREATE SCHEMA IF NOT EXISTS dl.silver;
CREATE SCHEMA IF NOT EXISTS dl.gold;

