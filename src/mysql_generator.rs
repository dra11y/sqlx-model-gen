use std::collections::HashMap;

use crate::generator::{ColumnInfo, Generator, TableInfo};
use sqlx::{Connection, MySqlConnection};

pub struct MysqlGenerator {}

impl Generator for MysqlGenerator {
    async fn get_tables(&self, conn_url: &str, schema: &str) -> Vec<TableInfo> {
        let mut conn: MySqlConnection = MySqlConnection::connect(conn_url).await.unwrap();
        let sql =
            "select table_name, (table_type = 'VIEW') AS is_view from information_schema.tables where table_type IN ('BASE TABLE', 'VIEW') AND table_schema = $1;".to_string();
        sqlx::query_as(sql.as_str())
            .bind(schema)
            .fetch_all(&mut conn)
            .await
            .expect("Failed to query mysql tables")
    }

    async fn query_columns(&self, conn_url: &str, table_name: &str) -> Vec<ColumnInfo> {
        let mut conn: MySqlConnection = MySqlConnection::connect(conn_url).await.unwrap();
        let sql = format!(
            r#"select COLUMN_NAME as column_name, ORDINAL_POSITION as ordinal_position,
     IS_NULLABLE = 'YES' as is_nullable, DATA_TYPE as data_type, CHARACTER_MAXIMUM_LENGTH as character_maximum_length
      from information_schema.columns where table_name = '{table_name}' order by ordinal_position asc; "#
        );

        let columns: Vec<ColumnInfo> = sqlx::query_as(sql.as_str())
            .fetch_all(&mut conn)
            .await
            .unwrap();
        columns
    }

    fn get_mapping_type(&self, sql_type: &str, udt_mappings: &HashMap<String, String>) -> String {
        let sql_type = sql_type.to_uppercase();
        match sql_type.as_str() {
            "TINYINT" => "i8",
            "SMALLINT" => "i16",
            "INT" | "SERIAL" => "i32",
            "OID" => "i32",
            "BIGINT" => "i64",
            "TINYINT UNSIGNED" => "u8",
            "SMALLINT UNSIGNED" => "u16",
            "INT UNSIGNED" => "u32",
            "BIGINT UNSIGNED" => "u64",
            "FLOAT" => "f32",
            "DOUBLE" => "f64",
            "CHAR" | "VARCHAR" => "String",
            "TEXT" | "TINYTEXT" | "LONGTEXT" | "MEDIUMTEXT" => "String",
            "BINARY" | "VARBINARY" => "Vec<u8>",
            "BLOB" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB" => "Vec<u8>",
            "TIMESTAMP" => "chrono::DateTime<chrono::Local>",
            "DATETIME" => "chrono::NaiveDateTime",
            "DATE" => "chrono::NaiveDate",
            "TIME" => "chrono::NaiveTime",
            "DECIMAL" => "sqlx::types::Decimal",
            "UUID" => "uuid::Uuid",
            "JSON" => "serde_json::Value",
            _ => udt_mappings.get(&sql_type).unwrap_or_else(|| {
                panic!("Unsupported type: {}", sql_type);
            }),
        }
        .to_string()
    }

    fn gen_insert_returning_id_fn(&self, table_name: &str, column_infos: &[ColumnInfo]) -> String {
        let struct_name = self.gen_struct_name(table_name);
        let ret = self.gen_field_and_value_str(column_infos, false);

        format!(
            r#"
pub async fn insert_returning_id(conn: &mut sqlx::MySqlConnection, obj: {struct_name}) -> i64 {{
    let mut sql = sql_builder::SqlBuilder::insert_into("{table_name}");
{ret}
   let sql = sql.sql().unwrap();
   let  result = sqlx::query(sql.as_str()).execute(conn).await;
   if result.is_ok() {{
       return result.unwrap().last_insert_id() as i64;
   }}
   println!("insert failed:{{:?}}", result);
   return -1;
}}
    "#
        )
    }

    fn gen_insert_fn(&self, table_name: &str, column_infos: &[ColumnInfo]) -> String {
        let struct_name = self.gen_struct_name(table_name);
        let ret = self.gen_field_and_value_str(column_infos, true);

        format!(
            r#"
pub async fn insert(conn: &mut sqlx::MySqlConnection, obj: {struct_name}) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error>  {{
    let mut sql = sql_builder::SqlBuilder::insert_into("{table_name}");
{ret}
    let sql = sql.sql().unwrap();
    sqlx::query(sql.as_str()).execute(conn).await

}}
    "#
        )
    }

    fn gen_batch_insert_returning_id_fn(
        &self,
        table_name: &str,
        column_infos: &[ColumnInfo],
    ) -> String {
        let struct_name = self.gen_struct_name(table_name);
        let ret = self.gen_field_and_batch_values_str(column_infos, false);

        format!(
            r#"

pub async fn batch_insert_returning_id(conn: &mut sqlx::MySqlConnection, objs: Vec<{struct_name}>) -> Vec<i64> {{
    let len = objs.len();
    let mut sql = sql_builder::SqlBuilder::insert_into("{table_name}");
{ret}

    let sql = sql.sql().unwrap();
    let result = sqlx::query(sql.as_str()).execute(conn).await;
    if result.is_ok() {{
        let last_id = result.unwrap().last_insert_id() as i64;
        println!("last id:{{last_id}}");
        let mut list = vec![];
        for idx in 0..len {{
            list.push(last_id - len as i64 + idx as i64 + 1)
        }}
        return list;
    }}
    println!("insert failed:{{:?}}", result);
    return vec![]

}}
    "#
        )
    }

    fn gen_batch_insert_fn(&self, table_name: &str, column_infos: &[ColumnInfo]) -> String {
        let struct_name = self.gen_struct_name(table_name);

        let ret = self.gen_field_and_batch_values_str(column_infos, true);

        format!(
            r#"

pub async fn batch_insert(conn: &mut sqlx::MySqlConnection, objs: Vec<{struct_name}>) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error>  {{
    let mut sql = sql_builder::SqlBuilder::insert_into("{table_name}");
{ret}

    let sql = sql.sql().unwrap();
    sqlx::query(sql.as_str()).execute(conn).await

}}
    "#
        )
    }

    fn gen_select_by_id_fn(&self, table_name: &str, column_infos: &[ColumnInfo]) -> String {
        let sql = self.gen_select_sql(table_name, column_infos);
        let struct_name = self.gen_struct_name(table_name);

        format!(
            r#"
pub async fn select_by_id(conn: &mut sqlx::MySqlConnection,id: i64) -> Result<{struct_name}, sqlx::Error> {{
    let sql = format!("{sql} where id='{{}}'", id);
    let result = sqlx::query_as(sql.as_str()).fetch_one(conn).await;
    result
}}

        "#
        )
    }

    fn gen_delete_by_id_fn(&self, _table_name: &str) -> String {
        let sql = self.gen_delete_by_id_sql(_table_name);

        format!(
            r#"
pub async fn delete_by_id(conn: &mut sqlx::MySqlConnection,id: i64) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error> {{
    let sql = format!("{sql}'{{}}'", id);
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
    use crate::mysql_generator::MysqlGenerator;
    use chrono::{DateTime, NaiveDate, NaiveDateTime};
    use core::f64;
    use sqlx::types::Decimal;
    use sqlx::{Connection, MySqlConnection};
    use std::str::FromStr;
    use std::time::SystemTime;

    #[tokio::test]
    async fn gen_file_test() {
        let conn_url = "mysql://root:123456@localhost/test_db";
        let table_name = "test_table";
        let gen = MysqlGenerator {};
        let result = gen.gen_struct_module(conn_url, table_name, None).await;
        println!("result:{:?}", result)
    }

    #[tokio::test]
    async fn insert_test() {
        let conn_url = "mysql://root:123456@localhost/test_db";
        let mut conn: MySqlConnection = MySqlConnection::connect(conn_url).await.unwrap();

        let mut obj1 = gen_test_table_obj();
        obj1.id = 1;
        let result = insert(&mut conn, obj1).await;
        println!("insert result:{:?}", result);
    }

    #[tokio::test]
    async fn insert_returning_id_test() {
        let conn_url = "mysql://root:123456@localhost/test_db";
        let mut conn: MySqlConnection = MySqlConnection::connect(conn_url).await.unwrap();
        let obj1 = gen_test_table_obj();
        let result = insert_returning_id(&mut conn, obj1).await;
        println!("insert result:{:?}", result);
    }

    #[tokio::test]
    async fn batch_insert_test() {
        let conn_url = "mysql://root:123456@localhost/test_db";
        let mut conn: MySqlConnection = MySqlConnection::connect(conn_url).await.unwrap();

        let mut obj1 = gen_test_table_obj();
        obj1.id = 2;
        let mut obj2 = gen_test_table_obj();
        obj2.id = 3;
        let result = batch_insert(&mut conn, vec![obj1, obj2]).await;
        println!("insert result:{:?}", result);
    }

    #[tokio::test]
    async fn batch_insert_returning_id_test() {
        let conn_url = "mysql://root:123456@localhost/test_db";
        let mut conn: MySqlConnection = MySqlConnection::connect(conn_url).await.unwrap();
        let obj1 = gen_test_table_obj();
        let obj2 = gen_test_table_obj();
        let result = batch_insert_returning_id(&mut conn, vec![obj1, obj2]).await;
        println!("insert result:{:?}", result);
    }

    #[tokio::test]
    async fn select_by_id_test() {
        let conn_url = "mysql://root:123456@localhost/test_db";
        let mut conn: MySqlConnection = MySqlConnection::connect(conn_url).await.unwrap();
        let result = select_by_id(&mut conn, 19).await;
        println!("{:?}", result)
    }

    #[tokio::test]
    async fn delete_by_id_test() {
        let conn_url = "mysql://root:123456@localhost/test_db";
        let mut conn: MySqlConnection = MySqlConnection::connect(conn_url).await.unwrap();
        let result = delete_by_id(&mut conn, 1).await;
        print!("{:?}", result);
    }

    fn gen_test_table_obj() -> TestTable {
        TestTable {
            id: 0,
            b1: 3,
            b2: Some(4),
            c1: "c".to_string(),
            c2: Some("c".to_string()),
            i4: 44,
            i41: Some(455),
            r1: 0.0,
            r2: Some(f64::consts::PI),
            d1: 0.0,
            d2: Some(345.0),
            t1: "4".to_string(),
            tx1: Some("tet3434".to_string()),
            tx2: Some("tet343432".to_string()),
            tx3: Some("tet34343".to_string()),
            t2: "5da".to_string(),
            t3: Some("test".to_string()),
            t4: Some("adf".to_string()),
            byte1: Some(Vec::from("안녕하세요你好こんにちはЗдравствуйте💖💖💖💖💖")),
            blob4: Some(Vec::from("안녕하세요你好こんにちはЗдравствуйте💖💖💖💖💖")),
            big1: Some(Decimal::new(234, 1)),
            blob2: Some(vec![3, 4, 5]),
            big2: Some(Decimal::new(223434, 2)),
            ts1: DateTime::from(SystemTime::now()),
            date1: Some(NaiveDate::default()),
            time1: Default::default(),
            i5: Some(12),
            blob3: Some(vec![3, 4, 5]),
            dt: NaiveDateTime::from_str("2015-09-18T23:56:04").unwrap(),
        }
    }
    #[derive(sqlx::FromRow, Debug, PartialEq)]
    pub struct TestTable {
        id: i64,
        b1: i8,
        b2: Option<i8>,
        c1: String,
        c2: Option<String>,
        i4: i32,
        i41: Option<i32>,
        r1: f64,
        r2: Option<f64>,
        d1: f64,
        d2: Option<f64>,
        t1: String,
        tx1: Option<String>,
        tx2: Option<String>,
        tx3: Option<String>,
        t2: String,
        t3: Option<String>,
        t4: Option<String>,
        byte1: Option<Vec<u8>>,
        blob4: Option<Vec<u8>>,
        blob3: Option<Vec<u8>>,
        big1: Option<sqlx::types::Decimal>,
        blob2: Option<Vec<u8>>,
        big2: Option<sqlx::types::Decimal>,
        ts1: chrono::DateTime<chrono::Local>,
        dt: chrono::NaiveDateTime,
        date1: Option<chrono::NaiveDate>,
        time1: chrono::NaiveTime,
        i5: Option<i16>,
    }

    pub async fn insert_returning_id(conn: &mut sqlx::MySqlConnection, obj: TestTable) -> i64 {
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
        sql.field("tx1");
        sql.field("tx2");
        sql.field("tx3");
        sql.field("t2");
        sql.field("t3");
        sql.field("t4");
        sql.field("byte1");
        sql.field("blob4");
        sql.field("blob3");
        sql.field("big1");
        sql.field("blob2");
        sql.field("big2");
        sql.field("ts1");
        sql.field("dt");
        sql.field("date1");
        sql.field("time1");
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
            sql_builder::quote(obj.tx1.unwrap().field_to_string()),
            sql_builder::quote(obj.tx2.unwrap().field_to_string()),
            sql_builder::quote(obj.tx3.unwrap().field_to_string()),
            sql_builder::quote(obj.t2.field_to_string()),
            sql_builder::quote(obj.t3.unwrap().field_to_string()),
            sql_builder::quote(obj.t4.unwrap().field_to_string()),
            sql_builder::quote(obj.byte1.unwrap().field_to_string()),
            sql_builder::quote(obj.blob4.unwrap().field_to_string()),
            sql_builder::quote(obj.blob3.unwrap().field_to_string()),
            sql_builder::quote(obj.big1.unwrap().field_to_string()),
            sql_builder::quote(obj.blob2.unwrap().field_to_string()),
            sql_builder::quote(obj.big2.unwrap().field_to_string()),
            sql_builder::quote(obj.ts1.field_to_string()),
            sql_builder::quote(obj.dt.field_to_string()),
            sql_builder::quote(obj.date1.unwrap().field_to_string()),
            sql_builder::quote(obj.time1.field_to_string()),
            sql_builder::quote(obj.i5.unwrap().field_to_string()),
        ]);

        let sql = sql.sql().unwrap();
        let result = sqlx::query(sql.as_str()).execute(conn).await;
        match result {
            Ok(result) => result.last_insert_id() as i64,
            Err(error) => {
                println!("Insert failed: {:?}", error);
                -1
            }
        }
    }

    pub async fn insert(
        conn: &mut sqlx::MySqlConnection,
        obj: TestTable,
    ) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error> {
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
        sql.field("tx1");
        sql.field("tx2");
        sql.field("tx3");
        sql.field("t2");
        sql.field("t3");
        sql.field("t4");
        sql.field("byte1");
        sql.field("blob4");
        sql.field("blob3");
        sql.field("big1");
        sql.field("blob2");
        sql.field("big2");
        sql.field("ts1");
        sql.field("dt");
        sql.field("date1");
        sql.field("time1");
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
            sql_builder::quote(obj.tx1.unwrap().field_to_string()),
            sql_builder::quote(obj.tx2.unwrap().field_to_string()),
            sql_builder::quote(obj.tx3.unwrap().field_to_string()),
            sql_builder::quote(obj.t2.field_to_string()),
            sql_builder::quote(obj.t3.unwrap().field_to_string()),
            sql_builder::quote(obj.t4.unwrap().field_to_string()),
            sql_builder::quote(obj.byte1.unwrap().field_to_string()),
            sql_builder::quote(obj.blob4.unwrap().field_to_string()),
            sql_builder::quote(obj.blob3.unwrap().field_to_string()),
            sql_builder::quote(obj.big1.unwrap().field_to_string()),
            sql_builder::quote(obj.blob2.unwrap().field_to_string()),
            sql_builder::quote(obj.big2.unwrap().field_to_string()),
            sql_builder::quote(obj.ts1.field_to_string()),
            sql_builder::quote(obj.dt.field_to_string()),
            sql_builder::quote(obj.date1.unwrap().field_to_string()),
            sql_builder::quote(obj.time1.field_to_string()),
            sql_builder::quote(obj.i5.unwrap().field_to_string()),
        ]);

        let sql = sql.sql().unwrap();
        sqlx::query(sql.as_str()).execute(conn).await
    }

    pub async fn batch_insert_returning_id(
        conn: &mut sqlx::MySqlConnection,
        objs: Vec<TestTable>,
    ) -> Vec<i64> {
        let len = objs.len();
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
        sql.field("tx1");
        sql.field("tx2");
        sql.field("tx3");
        sql.field("t2");
        sql.field("t3");
        sql.field("t4");
        sql.field("byte1");
        sql.field("blob4");
        sql.field("blob3");
        sql.field("big1");
        sql.field("blob2");
        sql.field("big2");
        sql.field("ts1");
        sql.field("dt");
        sql.field("date1");
        sql.field("time1");
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
                sql_builder::quote(obj.tx1.unwrap().field_to_string()),
                sql_builder::quote(obj.tx2.unwrap().field_to_string()),
                sql_builder::quote(obj.tx3.unwrap().field_to_string()),
                sql_builder::quote(obj.t2.field_to_string()),
                sql_builder::quote(obj.t3.unwrap().field_to_string()),
                sql_builder::quote(obj.t4.unwrap().field_to_string()),
                sql_builder::quote(obj.byte1.unwrap().field_to_string()),
                sql_builder::quote(obj.blob4.unwrap().field_to_string()),
                sql_builder::quote(obj.blob3.unwrap().field_to_string()),
                sql_builder::quote(obj.big1.unwrap().field_to_string()),
                sql_builder::quote(obj.blob2.unwrap().field_to_string()),
                sql_builder::quote(obj.big2.unwrap().field_to_string()),
                sql_builder::quote(obj.ts1.field_to_string()),
                sql_builder::quote(obj.dt.field_to_string()),
                sql_builder::quote(obj.date1.unwrap().field_to_string()),
                sql_builder::quote(obj.time1.field_to_string()),
                sql_builder::quote(obj.i5.unwrap().field_to_string()),
            ]);
        }

        let sql = sql.sql().unwrap();
        let result = sqlx::query(sql.as_str()).execute(conn).await;

        match result {
            Ok(result) => {
                let last_id = result.last_insert_id() as i64;
                println!("last id:{last_id}");
                let mut list = vec![];
                for idx in 0..len {
                    list.push(last_id - len as i64 + idx as i64 + 1)
                }
                list
            }
            Err(error) => {
                println!("insert failed:{:?}", error);
                vec![]
            }
        }
    }

    pub async fn batch_insert(
        conn: &mut sqlx::MySqlConnection,
        objs: Vec<TestTable>,
    ) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error> {
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
        sql.field("tx1");
        sql.field("tx2");
        sql.field("tx3");
        sql.field("t2");
        sql.field("t3");
        sql.field("t4");
        sql.field("byte1");
        sql.field("blob4");
        sql.field("blob3");
        sql.field("big1");
        sql.field("blob2");
        sql.field("big2");
        sql.field("ts1");
        sql.field("dt");
        sql.field("date1");
        sql.field("time1");
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
                sql_builder::quote(obj.tx1.unwrap().field_to_string()),
                sql_builder::quote(obj.tx2.unwrap().field_to_string()),
                sql_builder::quote(obj.tx3.unwrap().field_to_string()),
                sql_builder::quote(obj.t2.field_to_string()),
                sql_builder::quote(obj.t3.unwrap().field_to_string()),
                sql_builder::quote(obj.t4.unwrap().field_to_string()),
                sql_builder::quote(obj.byte1.unwrap().field_to_string()),
                sql_builder::quote(obj.blob4.unwrap().field_to_string()),
                sql_builder::quote(obj.blob3.unwrap().field_to_string()),
                sql_builder::quote(obj.big1.unwrap().field_to_string()),
                sql_builder::quote(obj.blob2.unwrap().field_to_string()),
                sql_builder::quote(obj.big2.unwrap().field_to_string()),
                sql_builder::quote(obj.ts1.field_to_string()),
                sql_builder::quote(obj.dt.field_to_string()),
                sql_builder::quote(obj.date1.unwrap().field_to_string()),
                sql_builder::quote(obj.time1.field_to_string()),
                sql_builder::quote(obj.i5.unwrap().field_to_string()),
            ]);
        }

        let sql = sql.sql().unwrap();
        sqlx::query(sql.as_str()).execute(conn).await
    }

    #[allow(unused)]
    pub fn select_sql() -> String {
        "select id, b1, b2, c1, c2, i4, i41, r1, r2, d1, d2, t1, tx1, tx2, tx3, t2, t3, t4, byte1, blob4, blob3, big1, blob2, big2, ts1, dt, date1, time1, i5  from test_table".to_string()
    }

    pub async fn select_by_id(
        conn: &mut sqlx::MySqlConnection,
        id: i64,
    ) -> Result<TestTable, sqlx::Error> {
        let sql = format!("select id, b1, b2, c1, c2, i4, i41, r1, r2, d1, d2, t1, tx1, tx2, tx3, t2, t3, t4, byte1, blob4, blob3, big1, blob2, big2, ts1, dt, date1, time1, i5  from test_table where id='{}'", id);

        sqlx::query_as(sql.as_str()).fetch_one(conn).await
    }

    pub async fn delete_by_id(
        conn: &mut sqlx::MySqlConnection,
        id: i64,
    ) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error> {
        let sql = format!("delete from test_table where id='{}'", id);
        sqlx::query(sql.as_str()).execute(conn).await
    }
}
