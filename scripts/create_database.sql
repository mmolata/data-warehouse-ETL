--terminate existing connections to data-warehouse
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'datawarehouse_db' AND pid <> pg_backend_pid();

--if datawarehouse_db exists, delete it
DROP DATABASE IF EXISTS datawarehouse_db; 

--create database
CREATE DATABASE datawarehouse_db;



