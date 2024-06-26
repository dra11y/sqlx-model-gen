# About

this is a rust tool for generate sql and table struct

based on sqlx and sql_builder

it support mysql and postgres

for more see examples/pg_examples.rs

# example

```rust
use sql_wrapper::generator::Generator;
use sql_wrapper::pg_generator;

#[tokio::main]
async fn main() {
    // it will generate a file table_name.rs whit table struct, get 4 insert functions
    let gen = pg_generator::PgGenerator{};
    let conn_url = "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
    let table_name = "test_table";
    let result = gen.gen_file(conn_url, table_name).await;
    println!("result:{:?}", result)
}

```

after run the code. it will generate a file test_table.rs

content:

```rust
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
        sql_builder::quote(obj.i5.unwrap().field_to_string())
    ]);

    sql.returning_id();
    let sql = sql.sql().unwrap();
    let columns:(i64,) = sqlx::query_as(sql.as_str()).fetch_one(conn).await.unwrap();
    return columns.0
}
    
pub async fn insert(conn: &mut PgConnection, obj: TestTable) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error>  {
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
        sql_builder::quote(obj.i5.unwrap().field_to_string())
    ]);

    let sql = sql.sql().unwrap();
    sqlx::query(sql.as_str()).execute(conn).await

}
    

pub async fn batch_insert_returning_id(conn: &mut PgConnection, objs: Vec<TestTable>) -> Vec<i64> {
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
            sql_builder::quote(obj.i5.unwrap().field_to_string())
        ]);
    }


    sql.returning_id();
    let sql = sql.sql().unwrap();
    let columns:Vec<(i64,)> = sqlx::query_as(sql.as_str()).fetch_all(conn).await.unwrap();
    let mut ret = vec![];
    for v in columns {
        ret.push(v.0)
    }
    println!("insert id:{:?}", ret);
    return ret;

}
    

pub async fn batch_insert(conn: &mut PgConnection, objs: Vec<TestTable>) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error>  {
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
            sql_builder::quote(obj.i5.unwrap().field_to_string())
        ]);
    }


    let sql = sql.sql().unwrap();
    sqlx::query(sql.as_str()).execute(conn).await

}
    
```