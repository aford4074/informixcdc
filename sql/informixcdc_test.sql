database sysmaster;

drop database if exists informixcdc_test;

create database informixcdc_test with log;

create table informixcdc_test (
    cdc_serial8         serial8,
    cdc_int8_low        int8 default -9223372036854775807,
    cdc_int8_high       int8 default 9223372036854775807,
    cdc_bigint_low      bigint default -9223372036854775807,
    cdc_bigint_high     bigint default 9223372036854775807,
    cdc_char            char(16) default "I heart CDC",
    cdc_date            date default today,
    cdc_datetime        datetime year to fraction default current,
    cdc_decimal_low     decimal(32,16) default -1234567890123456.1234567890123456,
    cdc_decimal_high    decimal(32,16) default 1234567890123456.1234567890123456,
    cdc_float_low       float default -99.99999999999999,
    cdc_float_high      float default 99.99999999999999,
    cdc_integer_low     integer default -2147483647,
    cdc_integer_high    integer default 2147483647,
    cdc_smallfloat_low  smallfloat default -99.99999999999999,
    cdc_smallfloat_high smallfloat default 99.99999999999999,
    cdc_smallint_low    smallint default -32767,
    cdc_smallint_high   smallint default 32767,
    cdc_varchar         varchar(255, 16) default "I still love CDC",
    cdc_lvarchar        lvarchar(256) default "Almost as much as waffles"
);
