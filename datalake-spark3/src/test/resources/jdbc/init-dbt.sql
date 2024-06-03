create schema test_schema

create table test_schema.test
(
    uid        varchar    not null,
    oid        varchar    not null,
    createdOn  timestamp  not null,
    updatedOn  timestamp  not null,
    data       bigint,
    chromosome varchar(2) not null,
    start      bigint
);