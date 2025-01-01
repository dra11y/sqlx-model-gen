use std::collections::HashMap;

use crate::generator::{ColumnInfo, Generator, TableInfo};
use sqlx::{Connection, PgConnection};

pub struct PgGenerator;

impl Generator for PgGenerator {
    async fn get_tables(&self, database_url: &str, schema: &str) -> Vec<TableInfo> {
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let sql =
            "select table_name, (table_type = 'VIEW') AS is_view from information_schema.tables where table_type IN ('BASE TABLE', 'VIEW') AND table_schema = $1;".to_string();
        sqlx::query_as(sql.as_str())
            .bind(schema)
            .fetch_all(&mut conn)
            .await
            .expect("Failed to query postgres tables")
    }

    async fn query_all_columns(&self, database_url: &str, schemas: &[&str]) -> Vec<ColumnInfo> {
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let sql = "SELECT
                column_name,
                table_name,
                table_schema AS schema_name,
                udt_name,
                (is_nullable = 'YES') AS is_nullable
            FROM information_schema.columns
            WHERE table_schema = ANY($1)
            ORDER BY ordinal_position ASC;";
        let columns: Vec<ColumnInfo> = sqlx::query_as(sql)
            .bind(schemas)
            .fetch_all(&mut conn)
            .await
            .unwrap();
        columns
    }

    async fn query_columns(&self, database_url: &str, table_name: &str) -> Vec<ColumnInfo> {
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let sql = "SELECT
                column_name,
                table_name,
                table_schema AS schema_name,
                udt_name,
                (is_nullable = 'YES') AS is_nullable
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position ASC;";
        let columns: Vec<ColumnInfo> = sqlx::query_as(sql)
            .bind(table_name)
            .fetch_all(&mut conn)
            .await
            .unwrap();
        columns
    }

    fn get_mapping_type(
        &self,
        sql_type: &str,
        is_nullable: bool,
        udt_mappings: &HashMap<String, String>,
    ) -> String {
        let sql_type = sql_type.to_uppercase();

        // serde_json::Value handles nulls as Value::Null.
        // Implement custom udt mapping for "json" / "jsonb" if you want to handle nulls differently,
        // e.g. NullableValue(Option<Value>), or override as Option<serde_json::Value>.
        let is_nullable = is_nullable && !["JSON", "JSONB"].contains(&sql_type.as_str());

        if let Some(array_of_type) = sql_type.strip_prefix('_') {
            return format!(
                "Vec<{}>",
                self.wrap_nullable(
                    is_nullable,
                    &self.get_mapping_type(array_of_type, is_nullable, udt_mappings)
                ),
            );
        }

        if sql_type.contains("char(") {
            return self.wrap_nullable(is_nullable, "String");
        }

        if sql_type.starts_with("numeric") {
            return self.wrap_nullable(is_nullable, "sqlx::types::BigDecimal");
        }

        if let Some(user_type) = udt_mappings.get(&format!("{}?", &sql_type)) {
            return user_type.to_string();
        }

        if let Some(user_type) = udt_mappings.get(&sql_type) {
            return self.wrap_nullable(is_nullable, user_type);
        }

        self.wrap_nullable(
            is_nullable,
            match sql_type.as_str() {
                "BIGINT" => "i64",
                "BIGSERIAL" => "i64",
                "BOOL" | "BOOLEAN" => "bool",
                "BYTEA" => "Vec<u8>",
                "CHAR" | "BPCHAR" | "CHARACTER" => "String",
                "DATE" => "chrono::NaiveDate",
                "DOUBLE" => "f64",
                "FLOAT4" => "f32",
                "REAL" => "f32",
                "FLOAT8" => "f64",
                "DOUBLE PRECISION" => "f64",
                "INT" => "i32",
                "INT2" => "i16",
                "INT4" => "i32",
                "INT8" => "i64",
                "INTEGER" => "i32",
                "INTERVAL" => "sqlx::postgres::types::PgInterval",
                "JSON" => "serde_json::Value",
                "JSONB" => "serde_json::Value",
                "NUMERIC" => "sqlx::types::Decimal",
                "OID" => "i32",
                "SERIAL" => "i32",
                "SMALLINT" => "i16",
                "SMALLSERIAL" => "i16",
                "TEXT" | "VARCHAR" | "NAME" | "CITEXT" => "String",
                "TIME WITH TIME ZONE" => "sqlx::postgres::types::PgTimeTz",
                "TIME WITHOUT TIME ZONE" => "chrono::NaiveTime",
                "TIME" => "chrono::NaiveTime",
                "TIMESTAMP WITH TIME ZONE" => "chrono::DateTime<chrono::Utc>",
                "TIMESTAMP WITHOUT TIME ZONE" => "chrono::NaiveDateTime",
                "TIMESTAMP" => "chrono::NaiveDateTime",
                "TIMESTAMPTZ" => "chrono::DateTime<chrono::Utc>",
                "UUID" => "uuid::Uuid",
                "VOID" => "()",
                _ => panic!("Unsupported type: {}", sql_type),
            },
        )
    }

    fn gen_insert_returning_id_fn(&self, table_name: &str, column_infos: &[ColumnInfo]) -> String {
        let struct_name = self.gen_struct_name(table_name);
        let ret = self.gen_field_and_value_str(column_infos, false);

        let fn_str = format!(
            r#"
pub async fn insert_returning_id(conn: &mut PgConnection, obj: {struct_name}) -> i64 {{
    let mut sql = sql_builder::SqlBuilder::insert_into("{table_name}");
{ret}
    sql.returning_id();
    let sql = sql.sql().unwrap();
    let columns:(i64,) = sqlx::query_as(sql.as_str()).fetch_one(conn).await.unwrap();
    return columns.0
}}
    "#
        );

        fn_str
    }

    fn gen_insert_fn(&self, table_name: &str, column_infos: &[ColumnInfo]) -> String {
        let struct_name = self.gen_struct_name(table_name);
        let ret = self.gen_field_and_value_str(column_infos, true);

        let fn_str = format!(
            r#"
pub async fn insert(conn: &mut PgConnection, obj: {struct_name}) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error>  {{
    let mut sql = sql_builder::SqlBuilder::insert_into("{table_name}");
{ret}
    let sql = sql.sql().unwrap();
    sqlx::query(sql.as_str()).execute(conn).await

}}
    "#
        );

        fn_str
    }

    fn gen_batch_insert_returning_id_fn(
        &self,
        table_name: &str,
        column_infos: &[ColumnInfo],
    ) -> String {
        let struct_name = self.gen_struct_name(table_name);

        let ret = self.gen_field_and_batch_values_str(column_infos, false);

        let fn_str = format!(
            r#"

pub async fn batch_insert_returning_id(conn: &mut PgConnection, objs: Vec<{struct_name}>) -> Vec<i64> {{
    let mut sql = sql_builder::SqlBuilder::insert_into("{table_name}");
{ret}

    sql.returning_id();
    let sql = sql.sql().unwrap();
    let columns:Vec<(i64,)> = sqlx::query_as(sql.as_str()).fetch_all(conn).await.unwrap();
    let mut ret = vec![];
    for v in columns {{
        ret.push(v.0)
    }}
    println!("insert id:{{:?}}", ret);
    return ret;

}}
    "#
        );

        fn_str
    }

    fn gen_batch_insert_fn(&self, table_name: &str, column_infos: &[ColumnInfo]) -> String {
        let struct_name = self.gen_struct_name(table_name);

        let ret = self.gen_field_and_batch_values_str(column_infos, true);

        let fn_str = format!(
            r#"

pub async fn batch_insert(conn: &mut PgConnection, objs: Vec<{struct_name}>) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error>  {{
    let mut sql = sql_builder::SqlBuilder::insert_into("{table_name}");
{ret}

    let sql = sql.sql().unwrap();
    sqlx::query(sql.as_str()).execute(conn).await

}}
    "#
        );

        fn_str
    }

    fn gen_select_by_id_fn(&self, table_name: &str, column_infos: &[ColumnInfo]) -> String {
        let sql = self.gen_select_sql(table_name, column_infos);
        let struct_name = self.gen_struct_name(table_name);
        format!(
            r#"
pub async fn select_by_id(conn: &mut PgConnection,id: i64) -> Result<{struct_name}, sqlx::Error> {{
    let sql = format!("{sql} where id='{{}}'", id);
    let result = sqlx::query_as(sql.as_str()).fetch_one(conn).await;
    result
}}

        "#
        )
    }

    fn gen_delete_by_id_fn(&self, table_name: &str) -> String {
        let sql = self.gen_delete_by_id_sql(table_name);
        format!(
            r#"
pub async fn delete_by_id(conn: &mut PgConnection,id: i64) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {{
    let sql = format!("{sql} '{{}}'", id);
    sqlx::query(sql.as_str()).execute(conn).await
}}
        "#
        )
    }
}

#[cfg(test)]
mod test {
    use crate::field_to_string::FieldToString;
    use crate::generator::Generator;
    use crate::pg_generator::PgGenerator;
    use chrono::{DateTime, FixedOffset, Utc};
    use core::f64;
    use sqlx::postgres::types::{PgInterval, PgTimeTz};
    use sqlx::types::Decimal;
    use sqlx::{Connection, PgConnection};
    use std::time::SystemTime;
    #[derive(sqlx::FromRow, Debug, PartialEq)]
    pub struct TestTable {
        id: i64,
        b1: bool,
        b2: Option<bool>,
        c1: String,
        c2: Option<String>,
        i4: i32,
        i41: Option<i32>,
        r1: f32,
        r2: Option<f64>,
        d1: f64,
        d2: Option<f64>,
        t1: String,
        t2: String,
        t3: Option<String>,
        t4: Option<String>,
        byte1: Option<Vec<u8>>,
        interval1: Option<sqlx::postgres::types::PgInterval>,
        big1: Option<sqlx::types::Decimal>,
        big2: Option<sqlx::types::Decimal>,
        ts1: chrono::NaiveDateTime,
        ts2: Option<chrono::DateTime<chrono::Utc>>,
        date1: Option<chrono::NaiveDate>,
        date2: Option<chrono::NaiveDate>,
        time1: chrono::NaiveTime,
        time2: Option<sqlx::postgres::types::PgTimeTz>,
        uid1: uuid::Uuid,
        json1: Option<serde_json::Value>,
        json2: Option<serde_json::Value>,
        i5: Option<i16>,
    }

    pub async fn insert_returning_id(conn: &mut PgConnection, obj: TestTable) -> i64 {
        let mut sql = sql_builder::SqlBuilder::insert_into("test_table");
        sql.field("b1");
        sql.field("b2");
        sql.field("c1");
        sql.field("c2");
        sql.field("i4");
        sql.field("i41");
        sql.field("r1");
        sql.field("r2");
        sql.field("d1");
        sql.field("d2");
        sql.field("t1");
        sql.field("t2");
        sql.field("t3");
        sql.field("t4");
        sql.field("byte1");
        sql.field("interval1");
        sql.field("big1");
        sql.field("big2");
        sql.field("ts1");
        sql.field("ts2");
        sql.field("date1");
        sql.field("date2");
        sql.field("time1");
        sql.field("time2");
        sql.field("uid1");
        sql.field("json1");
        sql.field("json2");
        sql.field("i5");
        sql.values(&[
            sql_builder::quote(obj.b1.field_to_string()),
            sql_builder::quote(obj.b2.unwrap().field_to_string()),
            sql_builder::quote(obj.c1.field_to_string()),
            sql_builder::quote(obj.c2.unwrap().field_to_string()),
            sql_builder::quote(obj.i4.field_to_string()),
            sql_builder::quote(obj.i41.unwrap().field_to_string()),
            sql_builder::quote(obj.r1.field_to_string()),
            sql_builder::quote(obj.r2.unwrap().field_to_string()),
            sql_builder::quote(obj.d1.field_to_string()),
            sql_builder::quote(obj.d2.unwrap().field_to_string()),
            sql_builder::quote(obj.t1.field_to_string()),
            sql_builder::quote(obj.t2.field_to_string()),
            sql_builder::quote(obj.t3.unwrap().field_to_string()),
            sql_builder::quote(obj.t4.unwrap().field_to_string()),
            sql_builder::quote(obj.byte1.unwrap().field_to_string()),
            sql_builder::quote(obj.interval1.unwrap().field_to_string()),
            sql_builder::quote(obj.big1.unwrap().field_to_string()),
            sql_builder::quote(obj.big2.unwrap().field_to_string()),
            sql_builder::quote(obj.ts1.field_to_string()),
            sql_builder::quote(obj.ts2.unwrap().field_to_string()),
            sql_builder::quote(obj.date1.unwrap().field_to_string()),
            sql_builder::quote(obj.date2.unwrap().field_to_string()),
            sql_builder::quote(obj.time1.field_to_string()),
            sql_builder::quote(obj.time2.unwrap().field_to_string()),
            sql_builder::quote(obj.uid1.field_to_string()),
            sql_builder::quote(obj.json1.unwrap().field_to_string()),
            sql_builder::quote(obj.json2.unwrap().field_to_string()),
            sql_builder::quote(obj.i5.unwrap().field_to_string()),
        ]);

        sql.returning_id();
        let sql = sql.sql().unwrap();
        let columns: (i64,) = sqlx::query_as(sql.as_str()).fetch_one(conn).await.unwrap();
        columns.0
    }

    pub async fn insert(
        conn: &mut PgConnection,
        obj: TestTable,
    ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
        let mut sql = sql_builder::SqlBuilder::insert_into("test_table");
        sql.field("id");
        sql.field("b1");
        sql.field("b2");
        sql.field("c1");
        sql.field("c2");
        sql.field("i4");
        sql.field("i41");
        sql.field("r1");
        sql.field("r2");
        sql.field("d1");
        sql.field("d2");
        sql.field("t1");
        sql.field("t2");
        sql.field("t3");
        sql.field("t4");
        sql.field("byte1");
        sql.field("interval1");
        sql.field("big1");
        sql.field("big2");
        sql.field("ts1");
        sql.field("ts2");
        sql.field("date1");
        sql.field("date2");
        sql.field("time1");
        sql.field("time2");
        sql.field("uid1");
        sql.field("json1");
        sql.field("json2");
        sql.field("i5");
        sql.values(&[
            sql_builder::quote(obj.id.field_to_string()),
            sql_builder::quote(obj.b1.field_to_string()),
            sql_builder::quote(obj.b2.unwrap().field_to_string()),
            sql_builder::quote(obj.c1.field_to_string()),
            sql_builder::quote(obj.c2.unwrap().field_to_string()),
            sql_builder::quote(obj.i4.field_to_string()),
            sql_builder::quote(obj.i41.unwrap().field_to_string()),
            sql_builder::quote(obj.r1.field_to_string()),
            sql_builder::quote(obj.r2.unwrap().field_to_string()),
            sql_builder::quote(obj.d1.field_to_string()),
            sql_builder::quote(obj.d2.unwrap().field_to_string()),
            sql_builder::quote(obj.t1.field_to_string()),
            sql_builder::quote(obj.t2.field_to_string()),
            sql_builder::quote(obj.t3.unwrap().field_to_string()),
            sql_builder::quote(obj.t4.unwrap().field_to_string()),
            sql_builder::quote(obj.byte1.unwrap().field_to_string()),
            sql_builder::quote(obj.interval1.unwrap().field_to_string()),
            sql_builder::quote(obj.big1.unwrap().field_to_string()),
            sql_builder::quote(obj.big2.unwrap().field_to_string()),
            sql_builder::quote(obj.ts1.field_to_string()),
            sql_builder::quote(obj.ts2.unwrap().field_to_string()),
            sql_builder::quote(obj.date1.unwrap().field_to_string()),
            sql_builder::quote(obj.date2.unwrap().field_to_string()),
            sql_builder::quote(obj.time1.field_to_string()),
            sql_builder::quote(obj.time2.unwrap().field_to_string()),
            sql_builder::quote(obj.uid1.field_to_string()),
            sql_builder::quote(obj.json1.unwrap().field_to_string()),
            sql_builder::quote(obj.json2.unwrap().field_to_string()),
            sql_builder::quote(obj.i5.unwrap().field_to_string()),
        ]);

        let sql = sql.sql().unwrap();
        sqlx::query(sql.as_str()).execute(conn).await
    }

    pub async fn batch_insert_returning_id(
        conn: &mut PgConnection,
        objs: Vec<TestTable>,
    ) -> Vec<i64> {
        let mut sql = sql_builder::SqlBuilder::insert_into("test_table");
        sql.field("b1");
        sql.field("b2");
        sql.field("c1");
        sql.field("c2");
        sql.field("i4");
        sql.field("i41");
        sql.field("r1");
        sql.field("r2");
        sql.field("d1");
        sql.field("d2");
        sql.field("t1");
        sql.field("t2");
        sql.field("t3");
        sql.field("t4");
        sql.field("byte1");
        sql.field("interval1");
        sql.field("big1");
        sql.field("big2");
        sql.field("ts1");
        sql.field("ts2");
        sql.field("date1");
        sql.field("date2");
        sql.field("time1");
        sql.field("time2");
        sql.field("uid1");
        sql.field("json1");
        sql.field("json2");
        sql.field("i5");
        for obj in objs {
            sql.values(&[
                sql_builder::quote(obj.b1.field_to_string()),
                sql_builder::quote(obj.b2.unwrap().field_to_string()),
                sql_builder::quote(obj.c1.field_to_string()),
                sql_builder::quote(obj.c2.unwrap().field_to_string()),
                sql_builder::quote(obj.i4.field_to_string()),
                sql_builder::quote(obj.i41.unwrap().field_to_string()),
                sql_builder::quote(obj.r1.field_to_string()),
                sql_builder::quote(obj.r2.unwrap().field_to_string()),
                sql_builder::quote(obj.d1.field_to_string()),
                sql_builder::quote(obj.d2.unwrap().field_to_string()),
                sql_builder::quote(obj.t1.field_to_string()),
                sql_builder::quote(obj.t2.field_to_string()),
                sql_builder::quote(obj.t3.unwrap().field_to_string()),
                sql_builder::quote(obj.t4.unwrap().field_to_string()),
                sql_builder::quote(obj.byte1.unwrap().field_to_string()),
                sql_builder::quote(obj.interval1.unwrap().field_to_string()),
                sql_builder::quote(obj.big1.unwrap().field_to_string()),
                sql_builder::quote(obj.big2.unwrap().field_to_string()),
                sql_builder::quote(obj.ts1.field_to_string()),
                sql_builder::quote(obj.ts2.unwrap().field_to_string()),
                sql_builder::quote(obj.date1.unwrap().field_to_string()),
                sql_builder::quote(obj.date2.unwrap().field_to_string()),
                sql_builder::quote(obj.time1.field_to_string()),
                sql_builder::quote(obj.time2.unwrap().field_to_string()),
                sql_builder::quote(obj.uid1.field_to_string()),
                sql_builder::quote(obj.json1.unwrap().field_to_string()),
                sql_builder::quote(obj.json2.unwrap().field_to_string()),
                sql_builder::quote(obj.i5.unwrap().field_to_string()),
            ]);
        }

        sql.returning_id();
        let sql = sql.sql().unwrap();
        let columns: Vec<(i64,)> = sqlx::query_as(sql.as_str()).fetch_all(conn).await.unwrap();
        let mut ret = vec![];
        for v in columns {
            ret.push(v.0)
        }
        println!("insert id:{:?}", ret);
        ret
    }

    pub async fn batch_insert(
        conn: &mut PgConnection,
        objs: Vec<TestTable>,
    ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
        let mut sql = sql_builder::SqlBuilder::insert_into("test_table");
        sql.field("id");
        sql.field("b1");
        sql.field("b2");
        sql.field("c1");
        sql.field("c2");
        sql.field("i4");
        sql.field("i41");
        sql.field("r1");
        sql.field("r2");
        sql.field("d1");
        sql.field("d2");
        sql.field("t1");
        sql.field("t2");
        sql.field("t3");
        sql.field("t4");
        sql.field("byte1");
        sql.field("interval1");
        sql.field("big1");
        sql.field("big2");
        sql.field("ts1");
        sql.field("ts2");
        sql.field("date1");
        sql.field("date2");
        sql.field("time1");
        sql.field("time2");
        sql.field("uid1");
        sql.field("json1");
        sql.field("json2");
        sql.field("i5");
        for obj in objs {
            sql.values(&[
                sql_builder::quote(obj.id.field_to_string()),
                sql_builder::quote(obj.b1.field_to_string()),
                sql_builder::quote(obj.b2.unwrap().field_to_string()),
                sql_builder::quote(obj.c1.field_to_string()),
                sql_builder::quote(obj.c2.unwrap().field_to_string()),
                sql_builder::quote(obj.i4.field_to_string()),
                sql_builder::quote(obj.i41.unwrap().field_to_string()),
                sql_builder::quote(obj.r1.field_to_string()),
                sql_builder::quote(obj.r2.unwrap().field_to_string()),
                sql_builder::quote(obj.d1.field_to_string()),
                sql_builder::quote(obj.d2.unwrap().field_to_string()),
                sql_builder::quote(obj.t1.field_to_string()),
                sql_builder::quote(obj.t2.field_to_string()),
                sql_builder::quote(obj.t3.unwrap().field_to_string()),
                sql_builder::quote(obj.t4.unwrap().field_to_string()),
                sql_builder::quote(obj.byte1.unwrap().field_to_string()),
                sql_builder::quote(obj.interval1.unwrap().field_to_string()),
                sql_builder::quote(obj.big1.unwrap().field_to_string()),
                sql_builder::quote(obj.big2.unwrap().field_to_string()),
                sql_builder::quote(obj.ts1.field_to_string()),
                sql_builder::quote(obj.ts2.unwrap().field_to_string()),
                sql_builder::quote(obj.date1.unwrap().field_to_string()),
                sql_builder::quote(obj.date2.unwrap().field_to_string()),
                sql_builder::quote(obj.time1.field_to_string()),
                sql_builder::quote(obj.time2.unwrap().field_to_string()),
                sql_builder::quote(obj.uid1.field_to_string()),
                sql_builder::quote(obj.json1.unwrap().field_to_string()),
                sql_builder::quote(obj.json2.unwrap().field_to_string()),
                sql_builder::quote(obj.i5.unwrap().field_to_string()),
            ]);
        }

        let sql = sql.sql().unwrap();
        sqlx::query(sql.as_str()).execute(conn).await
    }

    #[allow(unused)]
    pub fn select_sql() -> String {
        "select id, b1, b2, c1, c2, i4, i41, r1, r2, d1, d2, t1, t2, t3, t4, byte1, interval1, big1, big2, ts1, ts2, date1, date2, time1, time2, uid1, json1, json2, i5  from test_table".to_string()
    }

    pub async fn select_by_id(conn: &mut PgConnection, id: i64) -> Result<TestTable, sqlx::Error> {
        let sql = format!("select id, b1, b2, c1, c2, i4, i41, r1, r2, d1, d2, t1, t2, t3, t4, byte1, interval1, big1, big2, ts1, ts2, date1, date2, time1, time2, uid1, json1, json2, i5  from test_table where id='{}'", id);

        sqlx::query_as(sql.as_str()).fetch_one(conn).await
    }

    pub async fn delete_by_id(
        conn: &mut PgConnection,
        id: i64,
    ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
        let sql = format!("delete from test_table where id= '{}'", id);
        sqlx::query(sql.as_str()).execute(conn).await
    }

    #[test]
    fn name_struct_test() {
        let name = "group_history";
        let gen = PgGenerator {};
        gen.gen_struct_name(name);
    }

    #[tokio::test]
    async fn select_test() {
        let database_url =
            "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let sql = "select * from test_table".to_string();

        let columns: Vec<TestTable> = sqlx::query_as(sql.as_str())
            .fetch_all(&mut conn)
            .await
            .unwrap();
        println!("columns:{:?}", columns)
    }

    #[test]
    fn to_string_test() {
        let now: DateTime<Utc> = DateTime::from(SystemTime::now());
        println!("now:{}", now)
    }

    #[tokio::test]
    async fn gen_file_test() {
        let gen = PgGenerator {};
        let database_url =
            "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let table_name = "test_table";
        let result = gen
            .gen_struct_module(database_url, table_name, &[], &[], None)
            .await;
        println!("result:{:?}", result)
    }

    #[tokio::test]
    async fn insert_returning_id_test() {
        let obj = gen_test_table_obj();
        let database_url =
            "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let id = insert_returning_id(&mut conn, obj).await;
        println!("insert return id:{id}")
    }
    #[tokio::test]
    async fn insert_test() {
        let obj = gen_test_table_obj();
        let database_url =
            "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let result = insert(&mut conn, obj).await;
        println!("{}", result.unwrap().rows_affected())
    }

    #[tokio::test]
    async fn test_query_1() {
        let database_url =
            "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let columns: Vec<TestTable> = sqlx::query_as("select * from test_table")
            .fetch_all(&mut conn)
            .await
            .unwrap();
        println!("columns:{:?}", columns)
    }

    #[tokio::test]
    async fn batch_insert_returning_id_test() {
        let obj = gen_test_table_obj();
        let obj1 = gen_test_table_obj();
        let list = vec![obj, obj1];
        let database_url =
            "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let ids = batch_insert_returning_id(&mut conn, list).await;
        println!("insert ids: {:?}", ids)
    }

    #[tokio::test]
    async fn batch_insert_test() {
        let mut obj = gen_test_table_obj();
        obj.id = 60;
        let mut obj1 = gen_test_table_obj();
        obj1.id = 61;
        let list = vec![obj, obj1];
        let database_url =
            "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let result = batch_insert(&mut conn, list).await;
        println!("{:?}", result);
    }

    #[tokio::test]
    async fn select_by_id_test() {
        let database_url =
            "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let result = select_by_id(&mut conn, 65).await;
        println!("{:?}", result)
    }

    fn gen_test_table_obj() -> TestTable {
        TestTable {
            id: 0,
            b1: false,
            b2: Some(true),
            c1: "c".to_string(),
            c2: Some("c".to_string()),
            i4: 44,
            i41: Some(455),
            r1: 0.0,
            r2: Some(f64::consts::PI),
            d1: 0.0,
            d2: Some(345.0),
            t1: "4".to_string(),
            t2: "5da".to_string(),
            t3: Some("test".to_string()),
            t4: Some("adf".to_string()),
            byte1: Some(Vec::from("안녕하세요你好こんにちはЗдравствуйте💖💖💖💖💖")),
            interval1: Some(PgInterval {
                months: 0,
                days: 1,
                microseconds: 10000,
            }),
            big1: Some(Decimal::new(234, 1)),
            big2: Some(Decimal::new(223434, 2)),
            ts1: Default::default(),
            ts2: Some(Default::default()),
            date1: Some(Default::default()),
            date2: Some(Default::default()),
            time1: Default::default(),
            time2: Some(PgTimeTz {
                time: Default::default(),
                offset: FixedOffset::east_opt(0).unwrap(),
            }),
            uid1: Default::default(),
            json1: Some(serde_json::from_str("{}").unwrap()),
            json2: Some(serde_json::from_str("[{}, {}]").unwrap()),
            i5: Some(12),
        }
    }

    #[test]
    fn test_vec8() {
        let vec = Vec::from("안녕하세요你好こんにちはЗдравствуйте💖💖💖💖💖");
        let str2 = String::from_utf8(vec).unwrap();
        println!("{}", str2);
    }

    #[tokio::test]
    async fn delete_by_id_test() {
        let database_url =
            "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(database_url).await.unwrap();
        let result = delete_by_id(&mut conn, 3).await;
        println!("delete result:{:?}", result)
    }
}
