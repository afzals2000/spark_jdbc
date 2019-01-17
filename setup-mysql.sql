-- create new databases, tables, and users.

-- Create test database
create database sparkie;

-- Create a user representing your Spark cluster
-- Use % as a wildcard when specifying an IP subnet, such as '123.456.78.%'
create user 'sparkie'@'<SparkCluster>' identified by '<password>';

-- Add privileges for the Spark cluster
grant create, delete, drop, insert, select, update on sparkie.* to 'sparkie'@'<subnetOfSparkCluster>';
flush privileges;

-- Create a test table of physical characteristics.
use sparkie;
create table people (
  id int(10) not null auto_increment,
  name char(50) not null,
  is_male tinyint(1) not null,
  height_in int(4) not null,
  weight_lb int(4) not null,
  primary key (id),
  key (id)
);

-- Create sample data to load into a DataFrame
insert into people values (null, 'Alice', 0, 60, 125);
insert into people values (null, 'Brian', 1, 64, 131);
insert into people values (null, 'Charlie', 1, 74, 183);
insert into people values (null, 'Doris', 0, 58, 102);
insert into people values (null, 'Ellen', 0, 66, 140);
insert into people values (null, 'Frank', 1, 66, 151);
insert into people values (null, 'Gerard', 1, 68, 190);
insert into people values (null, 'Harold', 1, 61, 128);
