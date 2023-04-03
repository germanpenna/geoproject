-- Change the configuration of the next parameters:
-- <etl_warehouse>. Warehouse to run the data tranformation
-- <query_warehouse>. Warehouse to run the queries, especially end-users.
-- <database>. Database name.
-- <raw>. Schema to storage the raw data.
-- <model>. Schema to storage the data tranformed.

-- virtual warehouses: etl, query
USE ROLE SYSADMIN;

CREATE WAREHOUSE <etl_warehouse>
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'A computing resource for dbt to run ETL';

CREATE WAREHOUSE <query_warehouse>
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'A computing resource for dbt to run ETL';

-- databases and schemas
CREATE DATABASE IF NOT EXISTS <database>;
CREATE SCHEMA IF NOT EXISTS <database>.<raw>;
CREATE SCHEMA IF NOT EXISTS <database>.<transformed>;


-- roles and users
USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS <airbyte_role>;
CREATE USER IF NOT EXISTS <airbyte_user>
    PASSWORD=<airbyte_password>
    LOGIN_NAME=<airbyte_user>
    MUST_CHANGE_PASSWORD=FALSE
    DEFAULT_WAREHOUSE=<etl_warehouse>
    DEFAULT_ROLE=<airbyte_role>
    DEFAULT_NAMESPACE=<database>.<raw>
    COMMENT=<comment>;

CREATE ROLE IF NOT EXISTS <dbt_role>;
CREATE USER IF NOT EXISTS <dbt_user>
    PASSWORD=<dbt_password>
    LOGIN_NAME=<dbt_user>
    MUST_CHANGE_PASSWORD=FALSE
    DEFAULT_WAREHOUSE=<etl_warehouse>
    DEFAULT_ROLE=<dbt_role>
    DEFAULT_NAMESPACE=<database>.<raw>
    COMMENT=<comment>;

CREATE ROLE IF NOT EXISTS <preset_role>;
CREATE USER IF NOT EXISTS <preset_user>
    PASSWORD=<preset_password>
    LOGIN_NAME=<preset_user>
    MUST_CHANGE_PASSWORD=FALSE
    DEFAULT_WAREHOUSE=<query_warehouse>
    DEFAULT_ROLE=<preset_role>
    DEFAULT_NAMESPACE=<database>.<raw>
    COMMENT=<comment>;

USE ROLE SECURITYADMIN;

GRANT ROLE <airbyte_role> TO USER <airbyte_user>;
GRANT ROLE <dbt_role> TO USER <dbt_user>;
GRANT ROLE <preset_role> TO USER <preset_user>;


-- set up permissions
USE ROLE SECURITYADMIN;

-- airbyte_rw account level
GRANT USAGE ON WAREHOUSE <etl_warehouse> TO ROLE <airbyte_role>;
GRANT USAGE ON WAREHOUSE <query_warehouse> TO ROLE <airbyte_role>;
GRANT ALL ON DATABASE <database> TO ROLE <airbyte_role>;

-- airbyte_rw schema level
GRANT ALL ON ALL SCHEMAS IN DATABASE <database> TO ROLE <airbyte_role>;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE <database> TO ROLE <airbyte_role>;

-- airbyte_rw object level
GRANT ALL ON ALL TABLES IN SCHEMA <database>.<raw> TO ROLE <airbyte_role>;
GRANT ALL ON FUTURE TABLES IN SCHEMA <database>.<raw> TO ROLE <airbyte_role>;
GRANT ALL ON ALL TABLES IN SCHEMA <database>.<transformed> TO ROLE <airbyte_role>;
GRANT ALL ON FUTURE TABLES IN SCHEMA <database>.<transformed> TO ROLE <airbyte_role>;

GRANT ALL ON ALL VIEWS IN SCHEMA <database>.<raw> TO ROLE <airbyte_role>;
GRANT ALL ON FUTURE VIEWS IN SCHEMA <database>.<raw> TO ROLE <airbyte_role>;
GRANT ALL ON ALL VIEWS IN SCHEMA <database>.<transformed> TO ROLE <airbyte_role>;
GRANT ALL ON FUTURE VIEWS IN SCHEMA <database>.<transformed> TO ROLE <airbyte_role>;

-- dbt_rw account level
GRANT USAGE ON WAREHOUSE <etl_warehouse> TO ROLE <dbt_role>;
GRANT USAGE ON WAREHOUSE <query_warehouse> TO ROLE <dbt_role>;
GRANT ALL ON DATABASE <database> TO ROLE <dbt_role>;

-- dbt_rw schema level
GRANT ALL ON ALL SCHEMAS IN DATABASE <database> TO ROLE <dbt_role>;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE <database> TO ROLE <dbt_role>;

-- dbt_rw object level
GRANT ALL ON ALL TABLES IN SCHEMA <database>.<raw> TO ROLE <dbt_role>;
GRANT ALL ON FUTURE TABLES IN SCHEMA <database>.<raw> TO ROLE <dbt_role>;
GRANT ALL ON ALL TABLES IN SCHEMA <database>.<transformed> TO ROLE <dbt_role>;
GRANT ALL ON FUTURE TABLES IN SCHEMA <database>.<transformed> TO ROLE <dbt_role>;

GRANT ALL ON ALL VIEWS IN SCHEMA <database>.<raw> TO ROLE <dbt_role>;
GRANT ALL ON FUTURE VIEWS IN SCHEMA <database>.<raw> TO ROLE <dbt_role>;
GRANT ALL ON ALL VIEWS IN SCHEMA <database>.<transformed> TO ROLE <dbt_role>;
GRANT ALL ON FUTURE VIEWS IN SCHEMA <database>.<transformed> TO ROLE <dbt_role>;

-- preset_rw account level
GRANT USAGE ON WAREHOUSE <query_warehouse> TO ROLE <preset_role>;
GRANT ALL ON DATABASE <database> TO ROLE <preset_role>;

-- preset_rw schema level
GRANT ALL ON ALL SCHEMAS IN DATABASE <database> TO ROLE <preset_role>;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE <database> TO ROLE <preset_role>;

-- preset_rw object level
GRANT ALL ON ALL TABLES IN SCHEMA <database>.<transformed> TO ROLE <preset_role>;
GRANT ALL ON FUTURE TABLES IN SCHEMA <database>.<transformed> TO ROLE <preset_role>;

GRANT ALL ON ALL VIEWS IN SCHEMA <database>.<transformed> TO ROLE <preset_role>;
GRANT ALL ON FUTURE VIEWS IN SCHEMA <database>.<transformed> TO ROLE <preset_role>;