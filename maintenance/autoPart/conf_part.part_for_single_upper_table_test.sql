-- 月粒度
CREATE TABLE "TEST"."SALES" (
    id integer,
    date date,
    amt numeric(10,2)
)
 DISTRIBUTED BY (id) PARTITION BY RANGE(date)
          (
          START ('2024-08-01'::date) END ('2024-09-01'::date) WITH (tablename='SALES_1_prt_r181928501', appendonly='false'),
          START ('2024-09-01'::date) END ('2024-10-01'::date) WITH (tablename='SALES_1_prt_r1203295510', appendonly='false'),
          START ('2024-10-01'::date) END ('2024-11-01'::date) WITH (tablename='SALES_1_prt_r1345390349', appendonly='false'),
          START ('2024-11-01'::date) END ('2024-12-01'::date) WITH (tablename='SALES_1_prt_r694335272', appendonly='false'),
          START ('2024-12-01'::date) END ('2025-01-01'::date) WITH (tablename='SALES_1_prt_r1042089164', appendonly='false'),
          START ('2025-01-01'::date) END ('2025-02-01'::date) WITH (tablename='SALES_1_prt_r771309138', appendonly='false')
          );
-- 周粒度
CREATE TABLE "TEST"."SALES_WEEK" (
    id integer,
    date date,
    amt numeric(10,2)
)
 DISTRIBUTED BY (id) PARTITION BY RANGE(date)
( START (date '2020-09-07') INCLUSIVE
   END (date '2024-02-12') EXCLUSIVE
   EVERY (INTERVAL '7 days') );
   
-- 天粒度
CREATE TABLE "TEST"."SALES_DAY" (
    id integer,
    date date,
    amt numeric(10,2)
)
 DISTRIBUTED BY (id) PARTITION BY RANGE(date)
( START (date '2024-09-07') INCLUSIVE
   END (date '2024-10-12') EXCLUSIVE
   EVERY (INTERVAL '1 day') );
   
-- 自动分区函数
select conf_part.part_for_single_upper_table('TEST','SALES_DAY');
select conf_part.part_for_single_upper_table('TEST','SALES_WEEK');
select conf_part.part_for_single_upper_table('TEST','SALES_MONTH');