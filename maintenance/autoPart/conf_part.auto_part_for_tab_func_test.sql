-- 天粒度
CREATE TABLE public."GH_PAR_RANGE_EVERY" (
    id bigint NOT NULL,
    name text,
    s_date timestamp without time zone NOT NULL
)
 DISTRIBUTED BY (id) PARTITION BY RANGE(s_date)
          (
          PARTITION p20241114 START ('2024-11-14 00:00:00'::timestamp without time zone) END ('2024-11-15 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_1_prt_p20241114', appendonly='false'),
          PARTITION p20241115 START ('2024-11-15 00:00:00'::timestamp without time zone) END ('2024-11-16 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_1_prt_p20241115', appendonly='false'),
          PARTITION p20241116 START ('2024-11-16 00:00:00'::timestamp without time zone) END ('2024-11-17 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_1_prt_p20241116', appendonly='false'),
          PARTITION p20241117 START ('2024-11-17 00:00:00'::timestamp without time zone) END ('2024-11-18 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_1_prt_p20241117', appendonly='false'),
          PARTITION p20241118 START ('2024-11-18 00:00:00'::timestamp without time zone) END ('2024-11-19 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_1_prt_p20241118', appendonly='false'),
          PARTITION p20241119 START ('2024-11-19 00:00:00'::timestamp without time zone) END ('2024-11-20 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_1_prt_p20241119', appendonly='false'),
          PARTITION p20241120 START ('2024-11-20 00:00:00'::timestamp without time zone) END ('2024-11-21 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_1_prt_p20241120', appendonly='false'),
          PARTITION p20241121 START ('2024-11-21 00:00:00'::timestamp without time zone) END ('2024-11-22 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_1_prt_p20241121', appendonly='false')
          );
-- 月粒度                     
CREATE TABLE public."GH_PAR_RANGE_EVERY_MONTH" (
    id bigint NOT NULL,
    name text,
    s_date timestamp without time zone NOT NULL
)
 DISTRIBUTED BY (id) PARTITION BY RANGE(s_date)
          (
          PARTITION p20241001 START ('2024-10-01 00:00:00'::timestamp without time zone) END ('2024-11-01 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_MONTH_1_prt_p20241001', appendonly='false'),
          PARTITION p20241101 START ('2024-11-01 00:00:00'::timestamp without time zone) END ('2024-12-01 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_MONTH_1_prt_p20241101', appendonly='false'),
          PARTITION p20241201 START ('2024-12-01 00:00:00'::timestamp without time zone) END ('2025-01-01 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_MONTH_1_prt_p20241201', appendonly='false')
          );
          
-- 周粒度，取每周一为分区名
CREATE TABLE public."GH_PAR_RANGE_EVERY_WEEK" (
    id bigint NOT NULL,
    name text,
    s_date timestamp without time zone NOT NULL
)
 DISTRIBUTED BY (id) PARTITION BY RANGE(s_date)
          (
          PARTITION p20241021 START ('2024-10-21 00:00:00'::timestamp without time zone) END ('2024-10-28 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_WEEK_1_prt_p20241021', appendonly='false'),
          PARTITION p20241118 START ('2024-11-18 00:00:00'::timestamp without time zone) END ('2024-11-25 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_WEEK_1_prt_p20241118', appendonly='false'),
          PARTITION p20241125 START ('2024-11-25 00:00:00'::timestamp without time zone) END ('2024-12-02 00:00:00'::timestamp without time zone) WITH (tablename='GH_PAR_RANGE_EVERY_WEEK_1_prt_p20241125', appendonly='false')
          );

-- 添加约束
ALTER TABLE ONLY public."GH_PAR_RANGE_EVERY_MONTH"
    ADD CONSTRAINT "GH_PAR_RANGE_EVERY_MONTH_pkey" PRIMARY KEY (id, s_date);
-- 添加约束
ALTER TABLE ONLY public."GH_PAR_RANGE_EVERY_WEEK"
    ADD CONSTRAINT "GH_PAR_RANGE_EVERY_WEEK_pkey" PRIMARY KEY (id, s_date);
-- 添加约束
ALTER TABLE ONLY public."GH_PAR_RANGE_EVERY"
    ADD CONSTRAINT "GH_PAR_RANGE_EVERY_pkey" PRIMARY KEY (id, s_date);
    
-- 函数使用方法
select conf_part.auto_part_for_tab_func('public','GH_PAR_RANGE_EVERY');
select conf_part.auto_part_for_tab_func('public','GH_PAR_RANGE_EVERY_WEEK');
select conf_part.auto_part_for_tab_func('public','GH_PAR_RANGE_EVERY_MONTH');

-- 带有分区名
sjgxpt=# select partitiontablename,partitionname,partitionboundary from pg_partitions where tablename = 'GH_PAR_RANGE_EVERY_MONTH';
            partitiontablename            | partitionname |                                                            partitionboundary                                                       
     
------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------
 GH_PAR_RANGE_EVERY_MONTH_1_prt_p20241001 | p20241001     | PARTITION p20241001 START ('2024-10-01 00:00:00'::timestamp without time zone) END ('2024-11-01 00:00:00'::timestamp without time z
one)
 GH_PAR_RANGE_EVERY_MONTH_1_prt_p20241101 | p20241101     | PARTITION p20241101 START ('2024-11-01 00:00:00'::timestamp without time zone) END ('2024-12-01 00:00:00'::timestamp without time z
one)
 GH_PAR_RANGE_EVERY_MONTH_1_prt_p20241201 | p20241201     | PARTITION p20241201 START ('2024-12-01 00:00:00'::timestamp without time zone) END ('2025-01-01 00:00:00'::timestamp without time z
one)
(3 rows)

sjgxpt=# select partitiontablename,partitionname,partitionboundary from pg_partitions where tablename = 'GH_PAR_RANGE_EVERY_WEEK';
           partitiontablename            | partitionname |                                                            partitionboundary                                                        
    
-----------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------
 GH_PAR_RANGE_EVERY_WEEK_1_prt_p20241021 | p20241021     | PARTITION p20241021 START ('2024-10-21 00:00:00'::timestamp without time zone) END ('2024-10-28 00:00:00'::timestamp without time zo
ne)
 GH_PAR_RANGE_EVERY_WEEK_1_prt_p20241118 | p20241118     | PARTITION p20241118 START ('2024-11-18 00:00:00'::timestamp without time zone) END ('2024-11-25 00:00:00'::timestamp without time zo
ne)
 GH_PAR_RANGE_EVERY_WEEK_1_prt_p20241125 | p20241125     | PARTITION p20241125 START ('2024-11-25 00:00:00'::timestamp without time zone) END ('2024-12-02 00:00:00'::timestamp without time zo
ne)
(3 rows)

sjgxpt=# select partitiontablename,partitionname,partitionboundary from pg_partitions where tablename = 'GH_PAR_RANGE_EVERY';
         partitiontablename         | partitionname |                                                            partitionboundary                                                            
------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------
 GH_PAR_RANGE_EVERY_1_prt_p20241114 | p20241114     | PARTITION p20241114 START ('2024-11-14 00:00:00'::timestamp without time zone) END ('2024-11-15 00:00:00'::timestamp without time zone)
 GH_PAR_RANGE_EVERY_1_prt_p20241115 | p20241115     | PARTITION p20241115 START ('2024-11-15 00:00:00'::timestamp without time zone) END ('2024-11-16 00:00:00'::timestamp without time zone)
 GH_PAR_RANGE_EVERY_1_prt_p20241116 | p20241116     | PARTITION p20241116 START ('2024-11-16 00:00:00'::timestamp without time zone) END ('2024-11-17 00:00:00'::timestamp without time zone)
 GH_PAR_RANGE_EVERY_1_prt_p20241117 | p20241117     | PARTITION p20241117 START ('2024-11-17 00:00:00'::timestamp without time zone) END ('2024-11-18 00:00:00'::timestamp without time zone)
 GH_PAR_RANGE_EVERY_1_prt_p20241118 | p20241118     | PARTITION p20241118 START ('2024-11-18 00:00:00'::timestamp without time zone) END ('2024-11-19 00:00:00'::timestamp without time zone)
 GH_PAR_RANGE_EVERY_1_prt_p20241119 | p20241119     | PARTITION p20241119 START ('2024-11-19 00:00:00'::timestamp without time zone) END ('2024-11-20 00:00:00'::timestamp without time zone)
 GH_PAR_RANGE_EVERY_1_prt_p20241120 | p20241120     | PARTITION p20241120 START ('2024-11-20 00:00:00'::timestamp without time zone) END ('2024-11-21 00:00:00'::timestamp without time zone)
 GH_PAR_RANGE_EVERY_1_prt_p20241121 | p20241121     | PARTITION p20241121 START ('2024-11-21 00:00:00'::timestamp without time zone) END ('2024-11-22 00:00:00'::timestamp without time zone)
(8 rows)