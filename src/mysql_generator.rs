use sqlx::{Connection, MySqlConnection};
use crate::generator::{ColumnInfo, Generator};


struct MysqlGenerator {}

impl Generator for MysqlGenerator {
    async fn query_columns(&self, conn_url: &str, table_name: &str) -> Vec<ColumnInfo> {
        let mut conn: MySqlConnection = MySqlConnection::connect(conn_url).await.unwrap();
        let sql = format!(r#"select COLUMN_NAME as column_name, ORDINAL_POSITION as ordinal_position,
     IS_NULLABLE as is_nullable, DATA_TYPE as data_type, CHARACTER_MAXIMUM_LENGTH as character_maximum_length
      from information_schema.columns where table_name = '{table_name}' order by ordinal_position asc; "#);

        let columns: Vec<ColumnInfo> = sqlx::query_as(sql.as_str()).fetch_all(&mut conn).await.unwrap();
        return columns;
    }

    fn get_mapping_type(&self, sql_type: &str) -> String {
        let sql_type = sql_type.to_uppercase();
        let ret = if sql_type == "TINYINT" {
            "i8"
        } else if sql_type == "SMALLINT" {
            "i16"
        } else if sql_type == "INT" {
            "i32"
        } else if sql_type == "SERIAL" {
            "i32"
        } else if sql_type == "BIGINT" {
            "i64"
        } else if sql_type == "TINYINT UNSIGNED" {
            "u8"
        } else if sql_type == "SMALLINT UNSIGNED" {
            "u16"
        } else if sql_type == "INT UNSIGNED" {
            "u32"
        } else if sql_type == "BIGINT UNSIGNED" {
            "u64"
        } else if sql_type == "FLOAT" {
            "f32"
        } else if sql_type == "DOUBLE" {
            "f64"
        } else if sql_type == "VARCHAR" {
            "String"
        } else if sql_type == "TEXT" || sql_type == "TINYTEXT" || sql_type == "LONGTEXT" || sql_type == "MEDIUMTEXT" {
            "String"
        } else if sql_type == "CHAR" {
            "String"
        } else if sql_type == "VARBINARY" {
            "Vec<u8>"
        } else if sql_type == "BINARY" {
            "Vec<u8>"
        } else if sql_type == "BLOB" ||  sql_type == "LONGBLOB" ||  sql_type == "MEDIUMBLOB" || sql_type == "TINYBLOB" {
            "Vec<u8>"
        } else if sql_type == "TIMESTAMP" {
            "chrono::DateTime<chrono::Local>"
        } else if sql_type == "DATETIME" {
            "chrono::NaiveDateTime"
        } else if sql_type == "DATE" {
            "chrono::NaiveDate"
        } else if sql_type == "TIME" {
            "chrono::NaiveTime"
        } else if sql_type == "DECIMAL" {
            "sqlx::types::Decimal"
        } else if sql_type == "UUID" {
            "uuid::Uuid"
        } else if sql_type == "JSON" {
            "serde_json::Value"
        } else {
            panic!("{}", format!("not support type:{}", sql_type))
        };
        ret.to_string()
    }

    fn gen_insert_returning_id_fn(&self,table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
        let struct_name = self.gen_struct_name(table_name);
        let ret = self.gen_field_and_value_str(column_infos, false);

        let fn_str = format!(r#"
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
    "#);

        return fn_str
    }

    fn gen_insert_fn(&self, table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
        let struct_name = self.gen_struct_name(table_name);
        let ret = self.gen_field_and_value_str(column_infos, true);

        let fn_str = format!(r#"
pub async fn insert(conn: &mut sqlx::MySqlConnection, obj: {struct_name}) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error>  {{
    let mut sql = sql_builder::SqlBuilder::insert_into("{table_name}");
{ret}
    let sql = sql.sql().unwrap();
    sqlx::query(sql.as_str()).execute(conn).await

}}
    "#);

        return fn_str
    }

    fn gen_batch_insert_returning_id_fn(&self, table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
        let struct_name = self.gen_struct_name(table_name);
        let ret = self.gen_field_and_batch_values_str(column_infos, false);
        let fn_str = format!(r#"

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
    "#);

        fn_str
    }

    fn gen_batch_insert_fn(&self, table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
        let struct_name = self.gen_struct_name(table_name);

        let ret = self.gen_field_and_batch_values_str(column_infos, true);

        let fn_str = format!(r#"

pub async fn batch_insert(conn: &mut sqlx::MySqlConnection, objs: Vec<{struct_name}>) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error>  {{
    let mut sql = sql_builder::SqlBuilder::insert_into("{table_name}");
{ret}

    let sql = sql.sql().unwrap();
    sqlx::query(sql.as_str()).execute(conn).await

}}
    "#);

        fn_str
    }

    fn gen_select_by_id_fn(&self, table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
        let sql = self.gen_select_sql(table_name, column_infos);
        let struct_name = self.gen_struct_name(table_name);
        format!(r#"
pub async fn select_by_id(conn: &mut sqlx::MySqlConnection,id: i64) -> Result<{struct_name}, sqlx::Error> {{
        let sql = format!("{sql} where id='{{}}'", id);
        let result = sqlx::query_as(sql.as_str()).fetch_one(conn).await;
        result
}}

        "#)
    }
}





#[cfg(test)]
mod test {
    use std::str::FromStr;
    use std::time::SystemTime;
    use chrono::{DateTime, NaiveDate, NaiveDateTime};
    use sqlx::{Connection, MySqlConnection};
    use sqlx::types::Decimal;
    use crate::field_to_string::FieldToString;
    use crate::generator::Generator;
    use crate::mysql_generator::MysqlGenerator;

    #[tokio::test]
    async fn gen_file_test() {
        let conn_url = "mysql://root:123456@localhost/test_db";
        let table_name = "test_table";
        let gen = MysqlGenerator{};
        let result = gen.gen_file(conn_url, table_name).await;
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
            r2: Some(3.14),
            d1: 0.0,
            d2: Some(345.0),
            t1: "4".to_string(),
            tx1: Some("tet3434".to_string()),
            tx2: Some("tet343432".to_string()),
            tx3: Some("tet34343".to_string()),
            t2: "5da".to_string(),
            t3: Some("test".to_string()),
            t4: Some("adf".to_string()),
            byte1: Some(vec![2,3,4,5]),
            blob4: Some(vec![2,3,4,5,6]),
            big1: Some(Decimal::new(234,1)),
            blob2: Some(vec![3,4,5]),
            big2: Some(Decimal::new(223434, 2)),
            ts1:  DateTime::from(SystemTime::now()),
            date1: Some(NaiveDate::default()),
            time1: Default::default(),
            i5: Some(12),
            blob3: Some(vec![3,4,5]),
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
            sql_builder::quote(obj.i5.unwrap().field_to_string())
        ]);

        let sql = sql.sql().unwrap();
        let  result = sqlx::query(sql.as_str()).execute(conn).await;
        if result.is_ok() {
            return result.unwrap().last_insert_id() as i64;
        }
        println!("insert failed:{:?}", result);
        return -1;
    }

    pub async fn insert(conn: &mut sqlx::MySqlConnection, obj: TestTable) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error>  {
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
            sql_builder::quote(obj.i5.unwrap().field_to_string())
        ]);

        let sql = sql.sql().unwrap();
        sqlx::query(sql.as_str()).execute(conn).await

    }


    pub async fn batch_insert_returning_id(conn: &mut sqlx::MySqlConnection, objs: Vec<TestTable>) -> Vec<i64> {
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
                sql_builder::quote(obj.i5.unwrap().field_to_string())
            ]);
        }


        let sql = sql.sql().unwrap();
        let result = sqlx::query(sql.as_str()).execute(conn).await;
        if result.is_ok() {
            let last_id = result.unwrap().last_insert_id() as i64;
            println!("last id:{last_id}");
            let mut list = vec![];
            for idx in 0..len {
                list.push(last_id - len as i64 + idx as i64 + 1)
            }
            return list;
        }
        println!("insert failed:{:?}", result);
        return vec![]

    }


    pub async fn batch_insert(conn: &mut sqlx::MySqlConnection, objs: Vec<TestTable>) -> Result<sqlx::mysql::MySqlQueryResult, sqlx::Error>  {
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
                sql_builder::quote(obj.i5.unwrap().field_to_string())
            ]);
        }


        let sql = sql.sql().unwrap();
        sqlx::query(sql.as_str()).execute(conn).await

    }

    pub fn select_sql() -> String {
        "select id, b1, b2, c1, c2, i4, i41, r1, r2, d1, d2, t1, tx1, tx2, tx3, t2, t3, t4, byte1, blob4, blob3, big1, blob2, big2, ts1, dt, date1, time1, i5  from test_table".to_string()
    }

    pub async fn select_by_id(conn: &mut sqlx::MySqlConnection,id: i64) -> Result<TestTable, sqlx::Error> {
        let sql = format!("select id, b1, b2, c1, c2, i4, i41, r1, r2, d1, d2, t1, tx1, tx2, tx3, t2, t3, t4, byte1, blob4, blob3, big1, blob2, big2, ts1, dt, date1, time1, i5  from test_table where id='{}'", id);
        let result = sqlx::query_as(sql.as_str()).fetch_one(conn).await;
        result
    }



}