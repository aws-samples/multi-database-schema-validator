from database import MSSQL, POSTGRES, MYSQL, ORACLE

# Add queries for specific database type in the following manner to each of the sections:
# eg: For adding an Oracle query to GET_TABLE_ROW_COUNTS add it this way:
#
#     GET_TABLE_ROW_COUNTS = {
#         ORACLE: """
#                 SELECT blah from DUAL
#                 """
#     }
# Based on the database types defined in the configuration file/environment variables,
# these queries will automatically be chosen


GET_TABLE_ROW_COUNTS = {
    MSSQL: """
        SELECT lower(SCHEMA_NAME(sOBJ.schema_id)) as schema_name , lower(sOBJ.name) AS [table_name]
        , SUM(sPTN.Rows) AS [row_count]
        FROM
        sys.objects AS sOBJ
        INNER JOIN sys.partitions AS sPTN
        ON sOBJ.object_id = sPTN.object_id
        WHERE
        sOBJ.type = 'U'
        AND sOBJ.is_ms_shipped = 0x0
        AND index_id < 2 -- 0:Heap, 1:Clustered
        GROUP BY
        sOBJ.schema_id
        , sOBJ.name
        ORDER BY 1,2
        """,
    POSTGRES: """
        SELECT
        lower(nspname) AS schema_name, lower(relname) as table_name, reltuples::bigint as row_count
        FROM pg_class C
        LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
        WHERE
        lower(nspname) NOT IN ('public','information_schema','pg_catalog','aws_sqlserver_ext') AND
        relkind='r'
        ORDER BY 1 ,2  asc;
        """,
    ORACLE: """
        select lower(owner) as schema_name, lower(table_name) as table_name, nvl(num_rows,-1)  as row_count
        from all_tables WHERE lower(owner) = lower('{0}')
        ORDER BY 1
        """,
    MYSQL: """
        SELECT lower(TABLE_SCHEMA) as schema_name, lower(table_name) as table_name, table_rows as row_count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE lower(TABLE_SCHEMA) = lower('{0}')
        order by 1,2, 3;
        """
}

GET_VERSION = {
    MSSQL: """
        SELECT @@version as version
    """,

    POSTGRES: """
        SELECT version() as version
    """,

    MYSQL: """
        SELECT version() as version
    """
}

GET_DB_SIZE = {
    MSSQL: """
        SELECT 
            SUM(size * 8 / 1024.0) AS database_size
        FROM 
            sys.master_files
        WHERE 
            type = 0 -- 0 = Data files
            AND DB_NAME(database_id) = '{0}'
        GROUP BY 
            database_id;
    """,
    POSTGRES: """
        SELECT pg_size_pretty(pg_database_size('{0}')) as database_size
    """,
    MYSQL: """
        SELECT  
        SUM(data_length + index_length) / 1024 / 1024 as database_size
        FROM information_schema.tables 
        WHERE   
        lower(table_schema) not in ('mysql','information_schema','performance_schema','sys' )
    """
}

GET_ENCODING = {
    MSSQL: """
        SELECT CONVERT(sysname, DATABASEPROPERTYEX('{0}', 'Collation')) AS encoding;
    """,
    POSTGRES: """
        SELECT 'Encoding = '||pg_encoding_to_char(encoding)||', Collation = '||datcollate "encoding" FROM pg_database WHERE datname = current_database();    
    """,
    MYSQL: """
        SELECT  distinct concat('DEFAULT_CHARACTER_SET_NAME: ',DEFAULT_CHARACTER_SET_NAME ,'; ','DEFAULT_COLLATION_NAME: ', DEFAULT_COLLATION_NAME) as encoding
        FROM INFORMATION_SCHEMA.SCHEMATA 
        WHERE lower(schema_name) not in ('mysql','information_schema','performance_schema','sys' ) 
    """
}

GET_SCHEMAS = {
    MSSQL: """
        SELECT lower(REPLACE(NAME,' ','_')) AS schema_name
        FROM   sys.schemas
        WHERE  schema_id NOT  IN (2,3,4)
        AND    schema_id < 16380 
        """,
    POSTGRES: """
        SELECT lower(nspname) AS schema_name
        FROM   pg_catalog.pg_namespace
        WHERE  ( nspname NOT LIKE 'pg_toast%%'
         AND nspname NOT LIKE 'pg_temp%%'
         AND nspname NOT LIKE '%%pg_toast_temp%%'
         AND lower(nspname) NOT IN ( 'pg_catalog', 'public', 'information_schema','aws_sqlserver_ext','aws_sqlserver_ext_data',
         'datamart','cady' )) ORDER  BY 1; 
        """,
    ORACLE: """
        SELECT lower(username) as schema_name
        FROM sys.dba_users
        where lower(username)= lower('{0}') 
        union 
        SELECT  lower(object_name)
        FROM dba_objects
        WHERE lower(owner)=lower('{0}')
        and object_type in ('PACKAGE')  
        """,
    MYSQL: """
        select lower(schema_name) as schema_name
        from information_schema.schemata
        WHERE lower(schema_name) not in ('mysql','information_schema','performance_schema','sys')
        order by schema_name;
        """
}

GET_TABLES = {
    MSSQL: """
            SELECT lower(table_schema)      AS schema_name,
                   lower(table_name) AS table_name
            FROM   information_schema.tables
            WHERE  table_type = 'BASE TABLE'
            AND lower(table_schema)  = lower('{0}')
            AND      lower(table_schema) NOT IN ('information_schema', 'pg_catalog', 'public','aws_sqlserver_ext')
            ORDER  BY table_schema,table_name; 
        """,

    POSTGRES: """
            SELECT   lower(table_schema)     AS schema_name,
              lower(table_name) AS table_name
            FROM     information_schema.tables
            WHERE    table_type ='BASE TABLE'
            AND      lower(table_schema)  = lower('{0}')
            AND      lower(table_schema) NOT IN ('information_schema', 'pg_catalog', 'public','aws_sqlserver_ext', 
            'aws_sqlserver_ext_data')
            ORDER BY table_name;
        """,

    ORACLE: """
            select lower(owner) schema_name, lower(OBJECT_name) table_name 
            from dba_objects
            WHERE (OBJECT_name not like 'DR%' and OBJECT_name not like 'BIN$%' and OBJECT_name not like 'MLOG$%' and 
            object_name not like 'I_SNAP$%')
            and (OBJECT_name not in (select mview_name from dba_mviews where object_type='TABLE'))
            and TEMPORARY!='Y'
            and object_type in ('TABLE')
            AND lower(owner) = lower('{0}')
            ORDER BY 1,2
            """,
    MYSQL: """
            select lower(table_schema) as schema_name,
            lower(table_name) as table_name
            from information_schema.tables
            where table_type = 'BASE TABLE'
            and lower(table_schema) = lower('{0}')
            order by 1,2; 
            """
}

GET_VIEWS = {
    MSSQL: """
        SELECT lower(table_schema)      AS schema_name,
       lower(table_name) AS view_name
        FROM   information_schema.TABLES
        WHERE  table_type = 'VIEW'
               and  lower(table_schema) = lower('{0}')
         ORDER  BY table_name;
        """,

    POSTGRES: """
        SELECT lower(table_schema)      AS schema_name,
        lower(table_name)  AS view_name
        FROM   information_schema.TABLES
        WHERE  table_type = 'VIEW'
               and lower(table_schema) = lower('{0}')
         ORDER  BY table_name; 
        """,

    ORACLE: """
        select lower(owner) schema_name, lower(OBJECT_name) view_name 
        from dba_objects
        WHERE TEMPORARY!='Y'
        and object_type in ('MATERIALIZED_VIEW', 'VIEW')
        AND lower(owner) = lower('{0}')
        ORDER BY 1,2
        """
    ,
    MYSQL: """
        SELECT lower(TABLE_SCHEMA) as schema_name, lower(table_name)
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE lower(TABLE_SCHEMA) = lower('{0}')
        ORDER BY 1,2; 
        """
}

GET_PROCEDURES = {
    MSSQL: """
        SELECT lower(Schema_name(schema_id)) AS schema_name,
        name   AS procedure_name
        FROM   sys.objects
        WHERE  TYPE = 'P'
        and lower(Schema_name(schema_id)) =lower('{0}')
        ORDER  BY name;
        """,
    POSTGRES: """
        SELECT lower(n.nspname )       AS schema_name,
        lower(p.proname)  AS procedure_name
        FROM   pg_proc p
               join pg_namespace n
               ON p.pronamespace = n.oid
        WHERE  p.prokind = 'p'
        and lower(n.nspname) = lower('{0}')
        ORDER  BY p.proname; 
        """,
    ORACLE: """
        select lower(owner) schema_name, lower(OBJECT_name) procedure_name
        from dba_objects
        WHERE TEMPORARY!='Y'
        and object_type in ('PROCEDURE')
        AND lower(owner) = lower('{0}')
        ORDER BY 1,2
        """,
    MYSQL: """
        SELECT lower(ROUTINE_SCHEMA) as schema_name, lower(ROUTINE_NAME) procedure_name
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE lower(ROUTINE_SCHEMA) = lower('{0}')
        AND ROUTINE_TYPE = 'PROCEDURE'
        ORDER BY 1,2;
        """
}

GET_FUNCTIONS = {
    MSSQL: """
            SELECT lower(Schema_name(schema_id)) AS schema_name,
                   lower(name)                   AS function_name
            FROM   sys.objects
            WHERE  TYPE in( 'FN' ,'TF')  and lower(Schema_name(schema_id)) = lower('{0}')
            ORDER  BY Schema_name(schema_id), name; 
        """,
    POSTGRES: """
        SELECT lower(n.nspname) AS schema_name,
        lower(p.proname) AS function_name
        FROM   pg_proc p
               join pg_namespace n
                 ON p.pronamespace = n.oid
        WHERE p.prokind = 'f' and lower(n.nspname) = lower('{0}') order by p.proname; 
        """,
    ORACLE: """
        select lower(owner) schema_name, lower(OBJECT_name) function_name
        from dba_objects
        WHERE TEMPORARY!='Y'
        and object_type in ('FUNCTION')
        AND lower(owner) = lower('{0}')
        ORDER BY 1,2
        """,
    MYSQL: """
        SELECT lower(ROUTINE_SCHEMA) as schema_name, lower(ROUTINE_NAME) function_name
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE lower(ROUTINE_SCHEMA) = lower('{0}')
        AND ROUTINE_TYPE = 'FUNCTION'
        ORDER BY 1,2;
        """
}

GET_INDEXES = {
    MSSQL: """
        SELECT lower(sc.name) AS schema_name,
         lower(i.name) as index_name,
               i.type_desc 
        FROM   sys.INDEXES i
               inner join sys.objects o
                       ON i.object_id = o.object_id
               inner join sys.schemas sc
                       ON o.schema_id = sc.schema_id
        WHERE  i.name IS NOT NULL
               AND o.TYPE = 'U'
               and lower(sc.name) = lower('{0}') 
        ORDER  BY sc.name,i.name,
                  i.type_desc
        """,
    POSTGRES: """
        SELECT lower(schemaname) as schema_name, lower(indexname) as index_name
        FROM   pg_indexes
        WHERE    lower(schemaname) = lower('{0}')
        ORDER  BY indexname; 
        """,
    ORACLE: """
        select lower(owner) schema_name , lower(index_name) as index_name
        from dba_indexes
        where index_type!='LOB'
        and index_name not like 'I_SNAP$%'
        AND lower(owner) = lower('{0}')
        ORDER BY schema_name, index_name
        """,
    MYSQL: """
         SELECT lower(TABLE_SCHEMA) as schema_name, concat(lower(INDEX_NAME),'_',COLUMN_NAME) as index_name
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE lower(TABLE_SCHEMA) = lower('{0}')
        ORDER BY 1,2; """
}

GET_TRIGGERS = {
    MSSQL: """
        SELECT lower(Schema_name(schema_id)) AS schema_name,
        name            AS trigger_name
        FROM   sys.objects
        WHERE  TYPE = 'TR'
        and Schema_name(schema_id)  = lower('{0}')
        ORDER  BY name; 
        """,
    POSTGRES: """
        SELECT lower(trigger_schema)      AS schema_name,
        lower(trigger_name)  AS trigger_name
        FROM   information_schema.TRIGGERS
        WHERE lower(trigger_schema) =lower('{0}')
        ORDER  BY trigger_name;
        """,
    ORACLE: """
        Select lower(owner) schema_name ,lower(object_name) trigger_name
        from dba_objects
        where object_type='TRIGGER'
        AND lower(owner) = lower('{0}')
        ORDER BY schema_name, object_name
        """,
    MYSQL: """
       SELECT lower(TRIGGER_SCHEMA) as schema_name, lower(TRIGGER_NAME) as trigger_name
        FROM INFORMATION_SCHEMA.TRIGGERS
        WHERE lower(TRIGGER_SCHEMA) = lower('{0}') 
        ORDER BY 1,2; """
}

GET_CONSTRAINTS = {
    MSSQL: """
         SELECT lower(Schema_name(schema_id)) AS schema_name,
            lower(name)            AS constraint_name, 
            case TYPE when 'C' then 'Check'  when 'D' then 'Default'
            when 'PK' then 'Primary_Key' when 'F' then 'FOREIGN_KEY'
            when 'UQ' then 'Unique' else type end
                                AS constraint_type
            FROM   sys.objects
            WHERE  TYPE  IN  ('PK','F','UQ','C','D')
            and lower(Schema_name(schema_id)) = lower('{0}')
            ORDER  BY  name,TYPE ;
        """,
    POSTGRES: """
        With cte_final as (	SELECT lower(n.nspname)                       AS schema_name,
               lower(conname)                         AS constraint_name,
                case contype when 'c' then 'Check'  
                when 'p' then 'Primary_Key' when 'f' then 'FOREIGN_KEY'
                when 'u' then 'Unique' else 'Other' end 
                                    AS constraint_type
        FROM   pg_constraint c
               join pg_namespace n
                 ON n.oid = c.connamespace
        WHERE  contype IN ( 'p','f','u','c' )
               AND conrelid :: regclass :: VARCHAR <> '-'
                               and lower(n.nspname)  = lower('{0}')
               AND lower(n.nspname) NOT IN ( 'aws_sqlserver_ext', 
        'aws_sqlserver_ext_data',  'pg_catalog' ) 
        union 
        SELECT lower(col.table_schema) AS schema_name, 
                lower(column_name)  AS Const_column_name,'Default' as constrainttype
                FROM   information_schema.COLUMNS col
                WHERE  col.column_default IS NOT NULL
                       AND col.table_schema NOT IN (
                           'aws_sqlserver_ext', 'aws_sqlserver_ext_data',
                           'pg_catalog' )
                           and lower(col.table_schema) = lower('{0}') )
                           select * from cte_final
                ORDER  BY schema_name,constraint_name,constraint_type  

        """,
    ORACLE: """
        select lower(cs.owner) schema_name, lower(cs.constraint_name) constraint_name
        from dba_constraints cs
        where cs.CONSTRAINT_TYPE IN ('C')
        and upper(cs.search_condition_vc) not like '%IS NOT NULL'
        AND lower(owner) = lower('{0}')
        ORDER BY 1
    """,
    MYSQL: """
            SELECT   lower(CONSTRAINT_SCHEMA) schema_name, concat(lower(CONSTRAINT_NAME),'_',TABLE_NAME) constraint_name  
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
            WHERE CONSTRAINT_TYPE in ( 'PRIMARY KEY','FOREIGN KEY','CHECK','UNIQUE' ) 
            AND lower(CONSTRAINT_SCHEMA) = lower('{0}')  
            ORDER BY 1,2; """
}

GET_SEQUENCES = {
    MSSQL: """
        SELECT lower(Schema_name(schema_id)) AS schema_name,
        NAME            AS sequence_name
        FROM   sys.sequences
        where   lower(Schema_name( schema_id)) = lower('{0}')
        ORDER  BY NAME; 
        """,
    POSTGRES: """            
        SELECT relnamespace::regnamespace::text as schema_name,c.relname as sequence_name
        FROM pg_class c WHERE c.relkind = 'S'
        and lower(relnamespace::regnamespace::text) =lower('{0}')
        order by sequence_name;
        """,
    ORACLE: """
        select lower(sequence_owner) schema_name , lower(sequence_name) sequence_name
        from dba_sequences
        WHERE lower(sequence_owner) = lower('{0}')
        ORDER BY 1,2
        """,
    MYSQL: """
        SELECT  lower(table_schema) schema_name,  concat(lower(extra),'_',column_name) sequence_name 
        from information_schema.columns
        where extra like '%auto_increment%'
        and lower(table_schema) = lower('{0}')
        ORDER BY 1; 
        """
}


GET_PARTITIONED_INDEX = {
    MSSQL: """
            SELECT SCHEMA_NAME(o.schema_id) AS schema_name  
              ,OBJECT_NAME(p.object_id) AS table_name  
              ,i.name AS index_name  
              ,p.partition_number  
              ,rows   
              FROM sys.partitions AS p  
              INNER JOIN sys.indexes AS i ON p.object_id = i.object_id AND p.index_id = i.index_id  
              INNER JOIN sys.partition_schemes ps ON i.data_space_id=ps.data_space_id  
              INNER JOIN sys.objects AS o ON o.object_id = i.object_id  
              ORDER BY index_name, partition_number;
           """,
    POSTGRES: """
        select n.nspname  as schemaname  ,t.relname as tablename,  i.relname as indexname,  idx.indisprimary
              from pg_class i
              join pg_index idx on idx.indexrelid = i.oid
              join pg_class t on t.oid = idx.indrelid
              join pg_catalog.pg_namespace n on i.relnamespace= n."oid"
              where i.relkind = 'I';
        """
}

GET_DATATYPE_COUNTS = {
    MSSQL: """
         SELECT CASE WHEN  C.CHARACTER_MAXIMUM_LENGTH =-1 THEN C.DATA_TYPE+'max' 
         ELSE C.DATA_TYPE END AS data_type  , count(*) as count
         FROM SYS.TABLES T INNER JOIN SYS.SCHEMAS S
         ON T.SCHEMA_ID =S.SCHEMA_ID 
         INNER JOIN INFORMATION_SCHEMA.COLUMNS  C
         ON T.NAME=C.TABLE_NAME AND S.NAME = C.TABLE_SCHEMA
         WHERE T.TYPE ='U' AND S.PRINCIPAL_ID NOT IN (2,3,4)
         --AND C.CHARACTER_MAXIMUM_LENGTH =-1
         group by CASE WHEN  C.CHARACTER_MAXIMUM_LENGTH =-1 THEN C.DATA_TYPE+'max' 
         ELSE C.DATA_TYPE end
         order by 2 desc
         """,
    POSTGRES: """
        select data_type, count(*) as count
        from information_schema.columns where table_schema not in ('aws_sqlserver_ext','aws_sqlserver_ext_data',
        'information_schema','pubic','pg_catalog') group by data_type order by data_type
        """,
    ORACLE: """
        SELECT  data_type ,COUNT(DATA_TYPE) count
        FROM dba_tab_columns
        WHERE lower(OWNER) = lower('{0}')
        and table_name not in (select dba_objects.OBJECT_NAME from dba_objects where object_type = 'VIEW' AND dba_objects.owner = UPPER('crm_owner'))
        GROUP BY DATA_TYPE
        ORDER BY 1,2
        """,
    MYSQL: """
        SELECT distinct DATA_TYPE data_type, count(data_type) count 
        from INFORMATION_SCHEMA. COLUMNS  
        where lower(table_schema) = lower('{0}')
        group by data_type
        order by 1 """
}

GET_DATATYPES = {
    MSSQL: """
            SELECT S.NAME AS schema_name, T.NAME AS table_name ,
            C.COLUMN_NAME AS column_name,
            CASE WHEN C.CHARACTER_MAXIMUM_LENGTH =-1 THEN C.DATA_TYPE+'max' 
            ELSE C.DATA_TYPE END AS data_type
            FROM SYS.TABLES T INNER JOIN SYS.SCHEMAS S
            ON T.SCHEMA_ID =S.SCHEMA_ID 
            INNER JOIN INFORMATION_SCHEMA.COLUMNS C
            ON T.NAME=C.TABLE_NAME AND S.NAME = C.TABLE_SCHEMA
            WHERE T.TYPE ='U' AND S.PRINCIPAL_ID NOT IN (2,3,4)
            --AND C.CHARACTER_MAXIMUM_LENGTH =-1
            ORDER BY S.NAME, t.name;
        """,
    POSTGRES: """
            select table_schema as schema_name, table_name, column_name, data_type
            from information_schema.columns where table_schema not in ('aws_sqlserver_ext','aws_sqlserver_ext_data',
            'information_schema','pubic','pg_catalog')
            order by 1,2,3,4  ;
        """,
    ORACLE: """
        SELECT lower(OWNER) as schema_name , lower(TABLE_NAME) as table_name , lower(COLUMN_NAME) as column_name,
          DATA_TYPE ,
          CASE WHEN DATA_TYPE IN ('VARCHAR2','CHAR','BLOB','LONG','DATE') THEN NULL ELSE DATA_LENGTH END AS DATA_LENGTH
          FROM dba_tab_columns
          WHERE
           table_name not in (select dba_objects.OBJECT_NAME from dba_objects where object_type = 'VIEW')
          ORDER BY OWNER,DATA_TYPE
        """,
    MYSQL: """
        SELECT  table_schema as schema_name,table_name, column_name,DATA_TYPE  
        from INFORMATION_SCHEMA. COLUMNS  where lower(table_schema) = lower('{0}')
        order by 1,2,3,4 ; """
}
