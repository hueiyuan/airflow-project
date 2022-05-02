ALTER TABLE {{ params.table_name }} ADD IF NOT EXISTS PARTITION (dt='{{ macros.ds_add(ds, 1) }}')
