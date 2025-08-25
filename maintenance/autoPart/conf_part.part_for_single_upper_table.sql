CREATE OR REPLACE FUNCTION conf_part.part_for_single_upper_table(    
    s_schema_name varchar,  -- 模式名（schema）
    s_table_name varchar    -- 表名（table）
)
RETURNS "pg_catalog"."void" AS $BODY$
DECLARE
    v_current_max_part_endtime_text varchar;  -- 当前最大分区结束时间（文本格式）
    v_current_max_part_endtime TIMESTAMP;     -- 当前最大分区结束时间（时间戳格式）
    v_part_row conf_part.part_auto_conf%ROWTYPE;  -- 用于存储part_auto_conf表的一行记录
    v_part_unit conf_part.part_auto_conf."part_unit"%TYPE;  -- 分区单位（如：1天、1周、1月）
    v_retention_days conf_part.part_auto_conf."retention_days"%TYPE;  -- 数据保留天数
    v_min_retention_time TIMESTAMP;  -- 最小保留时间（用于判断是否需要删除过期分区）
    v_command varchar;  -- 动态执行的SQL命令
    parts pg_partitions%ROWTYPE;  -- 用于存储分区表（pg_partitions）的一行记录
    v_part_end_time TIMESTAMP;  -- 分区的结束时间
    v_step INTERVAL;  -- 分区步长
BEGIN
    -- 处理表名和模式名，确保大小写处理正确
    RAISE NOTICE 'Processing schema: %, table: %', s_schema_name, s_table_name;

    -- 从conf_part.part_auto_conf表中获取与给定表名对应的分区配置信息
    FOR v_part_row IN SELECT * FROM conf_part.part_auto_conf WHERE table_name = s_table_name
    LOOP
        -- 获取数据保留天数和分区单位
        v_retention_days := v_part_row.retention_days;
        v_part_unit := v_part_row.part_unit;

        -- 设置分区步长
        IF v_part_unit = '1 day' THEN
            v_step := '1 day'::INTERVAL;
        ELSIF v_part_unit = '1 week' THEN
            v_step := '1 week'::INTERVAL;
        ELSIF v_part_unit = '1 month' THEN
            v_step := '1 month'::INTERVAL;
        ELSE
            RAISE EXCEPTION 'Unsupported partition unit: %', v_part_unit;
        END IF;

        -- 正确计算最小保留时间
        v_min_retention_time := CURRENT_DATE - (v_retention_days * '1 day'::INTERVAL);

        BEGIN
            -- 调试信息
            RAISE NOTICE 'Processing table: %, schema: %, retention_days: %, part_unit: %', s_table_name, s_schema_name, v_retention_days, v_part_unit;

            -- 获取当前表的最大分区结束时间
            WITH tb1 AS (
                SELECT MAX(partitionrank) AS maxrank 
                FROM pg_partitions 
                WHERE tablename = s_table_name AND schemaname = s_schema_name
            )
            SELECT partitionrangeend INTO v_current_max_part_endtime_text 
            FROM tb1, pg_partitions 
            WHERE tablename = s_table_name AND schemaname = s_schema_name AND partitionrank = tb1.maxrank;

            -- 将分区结束时间文本转换为时间戳
            EXECUTE 'SELECT ' || v_current_max_part_endtime_text INTO v_current_max_part_endtime;
            RAISE NOTICE 'Current max partition end time: %', v_current_max_part_endtime;

            -- 添加分区逻辑
            WHILE v_current_max_part_endtime < CURRENT_DATE + (2 * v_step)
            LOOP
                v_command := 'ALTER TABLE "' || s_schema_name || '"."' || s_table_name || '" ADD PARTITION START (''' || v_current_max_part_endtime || ''') INCLUSIVE END (''' || v_current_max_part_endtime + v_step || ''') EXCLUSIVE';
                RAISE NOTICE 'v_command: %', v_command;

                BEGIN
                    EXECUTE v_command;
                    RAISE NOTICE 'Partition added successfully for %', v_command;
                EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'Error while adding partition for table %: %', s_table_name, SQLERRM;
                    INSERT INTO conf_part.part_auto_conf_log (id, schema_name, table_name, operation, oper_time, partition_name, command)
                    VALUES (nextval('conf_part.part_conf_seq'), s_schema_name, s_table_name, 'add_error', CURRENT_TIMESTAMP, 'N/A', SQLERRM);
                END;

                INSERT INTO conf_part.part_auto_conf_log (id, schema_name, table_name, operation, oper_time, partition_name, command)
                VALUES (nextval('conf_part.part_conf_seq'), s_schema_name, s_table_name, 'add', CURRENT_TIMESTAMP, 'N/A', v_command);

                v_current_max_part_endtime := v_current_max_part_endtime + v_step;
            END LOOP;

            -- 删除过期分区
            FOR parts IN SELECT * FROM pg_partitions WHERE tablename = s_table_name AND schemaname = s_schema_name
            LOOP
                EXECUTE 'SELECT ' || parts.partitionrangeend INTO v_part_end_time;

                IF v_part_end_time < v_min_retention_time THEN
                    v_command := 'ALTER TABLE "' || s_schema_name || '"."' || s_table_name || '" DROP PARTITION FOR (RANK(1))';
                    RAISE NOTICE 'command will execute: %', v_command;

                    BEGIN
                        EXECUTE v_command;
                        RAISE NOTICE 'Partition dropped successfully for %', v_command;
                    EXCEPTION WHEN OTHERS THEN
                        RAISE NOTICE 'Error while dropping partition for table %: %', s_table_name, SQLERRM;
                        INSERT INTO conf_part.part_auto_conf_log (id, schema_name, table_name, operation, oper_time, partition_name, command)
                        VALUES (nextval('conf_part.part_conf_seq'), s_schema_name, s_table_name, 'drop_error', CURRENT_TIMESTAMP, parts.partitiontablename, SQLERRM);
                    END;

                    INSERT INTO conf_part.part_auto_conf_log (id, schema_name, table_name, operation, oper_time, partition_name, command)
                    VALUES (nextval('conf_part.part_conf_seq'), s_schema_name, s_table_name, 'drop', CURRENT_TIMESTAMP, parts.partitiontablename, v_command);
                END IF;
            END LOOP;
        END;
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;