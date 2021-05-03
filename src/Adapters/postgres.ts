import * as knex from 'knex'
import {AdapterInterface, TableDefinition, ColumnDefinition, EnumDefinition} from './AdapterInterface'
import {Config} from '..'

export default class implements AdapterInterface {
    async getAllEnums(db: knex, config: Config): Promise<EnumDefinition[]> {
        const query = db('pg_type')
            .select('pg_namespace.nspname AS schema')
            .select('pg_type.typname AS name')
            .select('pg_enum.enumlabel AS value')
            .join('pg_enum', 'pg_enum.enumtypid', 'pg_type.oid')
            .join('pg_namespace', 'pg_namespace.oid', 'pg_type.typnamespace')
        if (config.schemas?.length > 0)
            query.whereIn('pg_namespace.nspname', config.schemas)

        const enums: { schema: string, name: string, value: string }[] = await query
        const foundEnums: { [key: string]: EnumDefinition } = {}

        function getValues(schema: string, name: string) {
            const values = {}
            for (const row of enums.filter(e => e.schema == schema && e.name == name)) {
                values[row.value] = row.value
            }
            return values
        }

        for (const row of enums) {
            const mapKey = row.schema + '.' + row.name
            if (foundEnums[mapKey] == undefined) {
                foundEnums[mapKey] = {name: row.name, schema: row.schema, values: getValues(row.schema, row.name)}
            }
        }
        return Object.values(foundEnums)
    }

    async getAllTables(db: knex, schemas: string[]): Promise<TableDefinition[]> {
        const query = db('pg_tables')
            .select('schemaname AS schema')
            .select('tablename AS name')
            .union(qb => {
                qb
                    .select('schemaname AS schema')
                    .select('matviewname AS name')
                    .from('pg_matviews')
                if (schemas.length > 0)
                    qb.whereIn('schemaname', schemas)
            })
            .whereNotIn('schemaname', ['pg_catalog', 'information_schema'])
        if (schemas.length > 0)
            query.whereIn('schemaname', schemas)
        return await query
    }

    async getAllColumns(db: knex, config: Config, table: string, schema: string): Promise<ColumnDefinition[]> {
        const sql = `
            SELECT typns.nspname                                        AS enumSchema,
                   pg_type.typname                                      AS enumType,
                   attributname.attname                                 AS name,
                   pg_namespace.nspname                                 AS schema,
                   pg_catalog.format_type(attributname.atttypid, null)  AS type,
                   attributname.attnotnull                              AS notNullable,
                   attributname.atthasdef                               AS hasDefault,
                   tablename.relname                                    AS table,
                   pg_type.typcategory                                  AS typcategory,
                   (SELECT (
                               WITH unnested_conkey AS (
                                   SELECT oid, unnest(conkey) as conkey
                                   FROM pg_constraint
                               )
                               SELECT referenced_tbl.relname AS referenced_table
                               FROM pg_constraint c
                                        LEFT JOIN unnested_conkey con ON c.oid = con.oid
                                        LEFT JOIN pg_class tbl ON tbl.oid = c.conrelid
                                        LEFT JOIN pg_attribute col ON (col.attrelid = tbl.oid AND col.attnum = con.conkey)
                                        LEFT JOIN pg_class referenced_tbl ON c.confrelid = referenced_tbl.oid
                               WHERE c.contype = 'f'
                                 AND tbl.relname = tablename.relname
                                 AND col.attname = attributname.attname
                           ) AS CONSTRAINT_REF_TABLE),                  AS contraintRefTable
                   CASE
                       WHEN EXISTS(SELECT null
                                   FROM pg_index
                                   WHERE pg_index.indrelid = attributname.attrelid
                                     AND attributname.attnum = any (pg_index.indkey)
                                     AND pg_index.indisprimary) THEN 1
                       ELSE 0
                        END                                              isPrimaryKey
            FROM pg_attribute attributname
                     JOIN pg_class tablename ON tablename.oid = attributname.attrelid
                     JOIN pg_type ON pg_type.oid = attributname.atttypid
                     JOIN pg_namespace ON pg_namespace.oid = tablename.relnamespace
                     JOIN pg_namespace AS typns ON typns.oid = pg_type.typnamespace
            where attributname.attnum > 0
              AND tablename.relname = :table
              AND pg_namespace.nspname = :schema;
        `
        return (await db.raw(sql, {table, schema})).rows
            .map((c: { name: string, type: string, notnullable: boolean, hasdefault: boolean, typcategory: string, enumschema: string, enumtype: string, contraintRefTable: string, isprimarykey: number }) => (
                {
                    name: c.name,
                    type: c.typcategory == "E" && config.schemaAsNamespace ? `${c.enumschema}.${c.enumtype}` : c.enumtype,
                    isNullable: !c.notnullable,
                    contraintRefTable: c.contraintRefTable,
                    isOptional: c.hasdefault,
                    isEnum: c.typcategory == "E",
                    isPrimaryKey: c.isprimarykey == 1
                }) as ColumnDefinition)
    }
}
