CREATE OR REPLACE FUNCTION conf_part.auto_part_for_tab_func(s_schema_name text, s_table_name text) 
RETURNS void AS $$
DECLARE
    -- 声明变量类型
    v_part_row conf_part.part_auto_conf%ROWTYPE;  -- 使用 %ROWTYPE 获取整行数据
    v_part_unit conf_part.part_auto_conf.part_unit%TYPE;  -- 使用 %TYPE 获取单列数据类型
    v_retention_days conf_part.part_auto_conf.retention_days%TYPE;  -- 使用 %TYPE 获取单列数据类型

    -- 存储当前创建的分区名
    partition_name text;

    -- 当前日期，默认值为今天
    current_date date := current_date;

    -- 删除分区的日期
    delete_before_date date;

    -- 标识分区是否存在的布尔值
    partition_exists boolean := false;
BEGIN
    -- 获取分区配置
    SELECT * 
    INTO v_part_row
    FROM conf_part.part_auto_conf
    WHERE schema_name = s_schema_name AND table_name = s_table_name;

    -- 如果没有找到配置，则退出
    IF NOT FOUND THEN
        RAISE NOTICE 'No partition configuration found for %.%.', s_schema_name, s_table_name;
        RETURN;
    END IF;

    -- 获取分区粒度和数据保留天数
    v_part_unit := v_part_row.part_unit;
    v_retention_days := v_part_row.retention_days;

    -- 计算删除的分区日期
    delete_before_date := current_date - v_retention_days;

    -- 查找并删除所有早于 delete_before_date 的分区
    FOR partition_name IN
        SELECT partitionname
        FROM pg_partitions 
        WHERE schemaname = s_schema_name AND tablename = s_table_name AND partitionname LIKE 'p%' 
        AND CAST(SUBSTRING(partitionname FROM 2 FOR 8) AS date) < delete_before_date
    LOOP
        -- 删除分区
        EXECUTE format('ALTER TABLE %I.%I DROP PARTITION IF EXISTS %I', s_schema_name, s_table_name, partition_name);
        RAISE NOTICE 'Dropped partition % for %.%.', partition_name, s_schema_name, s_table_name;

        -- 删除分区并记录操作
        INSERT INTO conf_part.part_auto_conf_log (id, schema_name, table_name, operation, oper_time, partition_name, command)
        VALUES (nextval('conf_part.part_conf_seq'), s_schema_name, s_table_name, 'DROP PARTITION', current_timestamp, partition_name, 
                format('ALTER TABLE %I.%I DROP PARTITION IF EXISTS %I', s_schema_name, s_table_name, partition_name));
    END LOOP;

    -- 处理不同粒度的分区
    IF v_part_unit = '1 day' THEN
        -- 从当前日期开始创建分区，未来7天
        FOR i IN 0..7 LOOP  -- 创建未来7天的分区
            partition_name := 'p' || to_char(current_date + i, 'YYYYMMDD');  -- 格式化分区名为 pYYYYMMDD

            -- 检查分区是否存在
            SELECT EXISTS (
                SELECT 1 
                FROM pg_partitions 
                WHERE schemaname = s_schema_name AND tablename = s_table_name AND partitionname = partition_name
            ) INTO partition_exists;
            
            IF NOT partition_exists THEN
                -- 使用 ALTER TABLE 添加分区
                EXECUTE format('ALTER TABLE %I.%I ADD PARTITION %I START (%L) END (%L)', 
                               s_schema_name, s_table_name, partition_name, current_date + i, current_date + i + 1);
                
                RAISE NOTICE 'Added partition % for %.%.', partition_name, s_schema_name, s_table_name;

                -- 记录操作
                INSERT INTO conf_part.part_auto_conf_log (id, schema_name, table_name, operation, oper_time, partition_name, command)
                VALUES (nextval('conf_part.part_conf_seq'), s_schema_name, s_table_name, 'ADD PARTITION', current_timestamp, partition_name, 
                        format('ALTER TABLE %I.%I ADD PARTITION %I', s_schema_name, s_table_name, partition_name));
            ELSE
                RAISE NOTICE 'Partition % for %.% already exists, skipping.', partition_name, s_schema_name, s_table_name;
            END IF;
        END LOOP;

    ELSIF v_part_unit = '1 week' THEN
        -- 从当前日期开始创建分区，未来2周的分区
        FOR i IN 0..1 LOOP  -- 创建未来2周的分区
            partition_name := 'p' || to_char(current_date + (i * 7), 'YYYYMMDD');  -- 格式化分区名为 pYYYYMMDD

            -- 检查分区是否存在
            SELECT EXISTS (
                SELECT 1 
                FROM pg_partitions 
                WHERE schemaname = s_schema_name AND tablename = s_table_name AND partitionname = partition_name
            ) INTO partition_exists;
            
            IF NOT partition_exists THEN
                -- 使用 ALTER TABLE 添加分区
                EXECUTE format('ALTER TABLE %I.%I ADD PARTITION %I START (%L) END (%L)', 
                               s_schema_name, s_table_name, partition_name, current_date + (i * 7), current_date + (i * 7) + 7);
                
                RAISE NOTICE 'Added partition % for %.%.', partition_name, s_schema_name, s_table_name;

                -- 记录操作
                INSERT INTO conf_part.part_auto_conf_log (id, schema_name, table_name, operation, oper_time, partition_name, command)
                VALUES (nextval('conf_part.part_conf_seq'), s_schema_name, s_table_name, 'ADD PARTITION', current_timestamp, partition_name, 
                        format('ALTER TABLE %I.%I ADD PARTITION %I', s_schema_name, s_table_name, partition_name));
            ELSE
                RAISE NOTICE 'Partition % for %.% already exists, skipping.', partition_name, s_schema_name, s_table_name;
            END IF;
        END LOOP;

    ELSIF v_part_unit = '1 month' THEN
        -- 从当前日期开始创建分区，未来2个月的分区
        FOR i IN 0..1 LOOP  -- 创建未来2个月的分区
            partition_name := 'p' || to_char(date_trunc('month', current_date) + interval '1 month' * i, 'YYYYMMDD');  -- 格式化分区名为每月第一天

            -- 检查分区是否存在
            SELECT EXISTS (
                SELECT 1 
                FROM pg_partitions 
                WHERE schemaname = s_schema_name AND tablename = s_table_name AND partitionname = partition_name
            ) INTO partition_exists;
            
            IF NOT partition_exists THEN
                -- 使用 ALTER TABLE 添加分区
                EXECUTE format('ALTER TABLE %I.%I ADD PARTITION %I START (%L) END (%L)', 
                               s_schema_name, s_table_name, partition_name, 
                               date_trunc('month', current_date) + interval '1 month' * i, 
                               date_trunc('month', current_date) + interval '1 month' * (i + 1));
                
                RAISE NOTICE 'Added partition % for %.%.', partition_name, s_schema_name, s_table_name;

                -- 记录操作
                INSERT INTO conf_part.part_auto_conf_log (id, schema_name, table_name, operation, oper_time, partition_name, command)
                VALUES (nextval('conf_part.part_conf_seq'), s_schema_name, s_table_name, 'ADD PARTITION', current_timestamp, partition_name, 
                        format('ALTER TABLE %I.%I ADD PARTITION %I', s_schema_name, s_table_name, partition_name));
            ELSE
                RAISE NOTICE 'Partition % for %.% already exists, skipping.', partition_name, s_schema_name, s_table_name;
            END IF;
        END LOOP;

    END IF;

    -- 打印结束信息
    RAISE NOTICE 'Finished processing table %.%.', s_schema_name, s_table_name;

EXCEPTION
    WHEN OTHERS THEN
        -- 错误处理并打印错误信息
        RAISE EXCEPTION 'Error in partition creation or deletion for %.%. Error: %', 
                         s_schema_name, s_table_name, SQLERRM;
                         
        -- 将错误日志记录到 conf_part.part_auto_conf_log
        INSERT INTO conf_part.part_auto_conf_log (id, schema_name, table_name, operation, oper_time, partition_name, command)
        VALUES (nextval('conf_part.part_conf_seq'), s_schema_name, s_table_name, 'ERROR', current_timestamp, 'N/A', 
                format('Error in partition creation or deletion for %.%. Error: %', s_schema_name, s_table_name, SQLERRM));

END;
$$ LANGUAGE plpgsql;