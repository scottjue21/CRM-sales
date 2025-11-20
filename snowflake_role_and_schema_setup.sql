-- Create objects
use role SYSADMIN;

create database if not exists DPS_ANALYTICS;

use database DPS_ANALYTICS;
create schema if not exists STAGING;
create schema if not exists INTERMEDIATE;
create schema if not exists CORE;
create schema if not exists GOOGLE_SHEETS; 
create schema if not exists SCOTT_DBT;

-- Users/Roles/Grants
use role SECURITYADMIN;

-- roles
create role if not exists LOADER;        -- Airbyte
create role if not exists TRANSFORMER;   -- dbt
create role if not exists ORCHESTRATOR;  -- Prefect

-- users
create user if not exists AIRBYTE_USER
  password = 'airbyte123'
  default_role = LOADER
  default_warehouse = COMPUTE_WH;

create user if not exists DBT_USER
  password = 'dbt123'
  default_role = TRANSFORMER
  default_warehouse = COMPUTE_WH;

create user if not exists PREFECT_USER
  password = 'prefect123'
  default_role = ORCHESTRATOR
  default_warehouse = COMPUTE_WH;

-- assign roles
grant role LOADER        to user AIRBYTE_USER;
grant role TRANSFORMER   to user DBT_USER;
grant role ORCHESTRATOR  to user PREFECT_USER;

-- WAREHOUSE usage (add these so dbt/Prefect can run)
grant usage on warehouse COMPUTE_WH to role LOADER;
grant usage on warehouse COMPUTE_WH to role TRANSFORMER;
grant usage on warehouse COMPUTE_WH to role ORCHESTRATOR;

-- LOADER (Airbyte) permissions
grant usage on database DPS_ANALYTICS to role LOADER;
grant usage on schema DPS_ANALYTICS.GOOGLE_SHEETS to role LOADER;

grant create schema on database DPS_ANALYTICS                  to role LOADER;
grant create table       on schema DPS_ANALYTICS.GOOGLE_SHEETS to role LOADER;
grant create stage       on schema DPS_ANALYTICS.GOOGLE_SHEETS to role LOADER;
grant create file format on schema DPS_ANALYTICS.GOOGLE_SHEETS to role LOADER;

grant select, insert, update, delete, truncate
  on all tables  in schema DPS_ANALYTICS.GOOGLE_SHEETS to role LOADER;
grant select, insert, update, delete, truncate
  on future tables in schema DPS_ANALYTICS.GOOGLE_SHEETS to role LOADER;

-- TRANSFORMER (dbt) permissions
grant usage on database DPS_ANALYTICS to role TRANSFORMER;

grant usage on schema DPS_ANALYTICS.GOOGLE_SHEETS  to role TRANSFORMER;
grant usage on schema DPS_ANALYTICS.STAGING        to role TRANSFORMER;
grant usage on schema DPS_ANALYTICS.INTERMEDIATE   to role TRANSFORMER;
grant usage on schema DPS_ANALYTICS.CORE           to role TRANSFORMER;
grant usage on schema DPS_ANALYTICS.SCOTT_DBT      to role TRANSFORMER;

-- read raw
grant select on all tables  in schema DPS_ANALYTICS.GOOGLE_SHEETS to role TRANSFORMER;
grant select on future tables in schema DPS_ANALYTICS.GOOGLE_SHEETS to role TRANSFORMER;

-- dev writes
grant create table, create view on schema DPS_ANALYTICS.SCOTT_DBT to role TRANSFORMER;

-- prod writes
grant create view  on schema DPS_ANALYTICS.STAGING      to role TRANSFORMER;
grant create table on schema DPS_ANALYTICS.INTERMEDIATE to role TRANSFORMER;
grant create view  on schema DPS_ANALYTICS.INTERMEDIATE to role TRANSFORMER;
grant create table on schema DPS_ANALYTICS.CORE         to role TRANSFORMER;
grant create view  on schema DPS_ANALYTICS.CORE         to role TRANSFORMER;

-- read modeled layers
grant select on all views  in schema DPS_ANALYTICS.STAGING      to role TRANSFORMER;
grant select on future views in schema DPS_ANALYTICS.STAGING    to role TRANSFORMER;

grant select on all tables in schema DPS_ANALYTICS.INTERMEDIATE to role TRANSFORMER;
grant select on all views  in schema DPS_ANALYTICS.INTERMEDIATE to role TRANSFORMER;
grant select on future tables in schema DPS_ANALYTICS.INTERMEDIATE to role TRANSFORMER;
grant select on future views  in schema DPS_ANALYTICS.INTERMEDIATE to role TRANSFORMER;

grant select on all tables in schema DPS_ANALYTICS.CORE to role TRANSFORMER;
grant select on all views  in schema DPS_ANALYTICS.CORE to role TRANSFORMER;
grant select on future tables in schema DPS_ANALYTICS.CORE to role TRANSFORMER;
grant select on future views  in schema DPS_ANALYTICS.CORE to role TRANSFORMER;

-- ORCHESTRATOR (Prefect) minimal access
grant usage on database DPS_ANALYTICS to role ORCHESTRATOR;
grant usage on schema DPS_ANALYTICS.STAGING      to role ORCHESTRATOR;
grant usage on schema DPS_ANALYTICS.INTERMEDIATE to role ORCHESTRATOR;
grant usage on schema DPS_ANALYTICS.CORE         to role ORCHESTRATOR;

grant select on all tables  in schema DPS_ANALYTICS.CORE         to role ORCHESTRATOR;
grant select on future tables in schema DPS_ANALYTICS.CORE       to role ORCHESTRATOR;
grant select on all tables  in schema DPS_ANALYTICS.INTERMEDIATE to role ORCHESTRATOR;
grant select on future tables in schema DPS_ANALYTICS.INTERMEDIATE to role ORCHESTRATOR;

grant usage on database DPS_ANALYTICS to user AIRBYTE_USER;
alter user AIRBYTE_USER set default_role = LOADER;


-- === Test LOADER permissions ===

use role LOADER;
use warehouse COMPUTE_WH;
use database DPS_ANALYTICS;
use schema DPS_ANALYTICS.GOOGLE_SHEETS;

create table if not exists _ab_perm_probe (id int);
drop table if exists _ab_perm_probe;