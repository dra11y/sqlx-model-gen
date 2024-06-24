use sqlx::{Connection, PgConnection};
use std::str::FromStr;

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
struct ColumnInfo {
    column_name: String,
    ordinal_position: i32,
    is_nullable: String,
    data_type: String,
    character_maximum_length: Option<i32>,
}

pub(crate) async fn gen_file(conn_url: &str, table_name: &str) -> Result<(), sqlx::Error> {
    let mut conn: PgConnection = PgConnection::connect(conn_url).await?;
    // let sql = format!("select column_name, ordinal_position, is_nullable, data_type, character_maximum_length from information_schema.columns where table_name = '{table_name}' order by ordinal_position asc; ");
    let sql = format!("select * from information_schema.columns where table_name = '{table_name}' order by ordinal_position asc; ");

    let columns: Vec<ColumnInfo> = sqlx::query_as(sql.as_str()).fetch_all(&mut conn).await?;

    if columns.is_empty() {
        println!("没有获取到对应的表信息");
        return Ok(());
    }
    println!("columns:{:?}", columns);
    let mut total_str = String::new();
    let struct_str = gen_struct(table_name, &columns);
    total_str.push_str(struct_str.as_str());
    let insert_fn_str = gen_insert_fn(table_name, &columns);
    total_str.push_str(insert_fn_str.as_str());
    let batch_insert_fn = gen_batch_insert_fn(table_name, &columns);
    total_str.push_str(batch_insert_fn.as_str());
    std::fs::write(format!("{table_name}.rs"), total_str).unwrap();
    Ok(())
}

fn gen_struct(table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
    let mut st =
        String::from_str("#[derive(sqlx::FromRow, Debug, PartialEq)] \npub struct ").unwrap();
    st.push_str(gen_struct_name(table_name).as_str());
    st.push_str(" {\n");
    for c in column_infos {
        st.push_str("    ");
        st.push_str(c.column_name.as_str());
        let ctype = get_mapping_type(c.data_type.as_str());
        st.push_str(": ");
        if c.is_nullable == "NO" {
            st.push_str(ctype.as_str());
        } else {
            st.push_str("Option<");
            st.push_str(ctype.as_str());
            st.push_str(">")
        }
        st.push_str(",\n");
    }
    st.push_str("}\n\n\n");
    println!("gen bean:\n{st}");
    return st;
}

fn get_mapping_type(sql_type: &str) -> String {
    let sql_type = sql_type.to_uppercase();
    let ret = if sql_type == "BOOLEAN" {
        "bool"
    } else if sql_type == "CHARACTER" {
        "String"
    } else if sql_type == "SMALLINT" {
        "i16"
    } else if sql_type == "SMALLSERIAL" {
        "i16"
    } else if sql_type == "INT2" {
        "i16"
    } else if sql_type == "INT" {
        "i32"
    } else if sql_type == "SERIAL" {
        "i32"
    } else if sql_type == "INTEGER" {
        "i32"
    } else if sql_type == "INT4" {
        "i32"
    } else if sql_type == "BIGINT" {
        "i64"
    } else if sql_type == "BIGSERIAL" {
        "i64"
    } else if sql_type == "INT8" {
        "i64"
    } else if sql_type == "REAL" {
        "f32"
    } else if sql_type == "FLOAT4" {
        "f32"
    } else if sql_type == "DOUBLE PRECISION" {
        "f64"
    } else if sql_type == "FLOAT8" {
        "f64"
    } else if sql_type == "CHARACTER VARYING" {
        "String"
    } else if sql_type == "TEXT" {
        "String"
    } else if sql_type == "NAME" {
        "String"
    } else if sql_type == "CITEXT" {
        "String"
    } else if sql_type == "BYTEA" {
        "Vec<u8>"
    } else if sql_type == "VOID" {
        "()"
    } else if sql_type == "INTERVAL" {
        "sqlx::postgres::types::PgInterval"
    } else if sql_type == "NUMERIC" {
        "sqlx::types::Decimal"
    } else if sql_type == "TIMESTAMP WITH TIME ZONE" {
        "chrono::DateTime<chrono::Utc>"
    } else if sql_type == "TIMESTAMP WITHOUT TIME ZONE" {
        "chrono::NaiveDateTime"
    } else if sql_type == "DATE" {
        "chrono::NaiveDate"
    } else if sql_type == "TIME WITHOUT TIME ZONE" {
        "chrono::NaiveTime"
    } else if sql_type == "TIME WITH TIME ZONE" {
        "sqlx::postgres::types::PgTimeTz"
    } else if sql_type == "UUID" {
        "uuid::Uuid"
    } else if sql_type == "JSON" {
        "serde_json::Value"
    } else if sql_type == "JSONB" {
        "serde_json::Value"
    } else {
        panic!("{}", format!("not support type:{}", sql_type))
    };
    ret.to_string()
}

fn gen_struct_name(table: &str) -> String {
    let src_name = table.to_lowercase();
    let mut to_up = true;
    let mut new_name = String::new();
    let mut idx = 0;
    for c in src_name.chars() {
        if idx == 0 {
            to_up = true;
        }
        if c == '_' {
            to_up = true;
            continue;
        }
        if to_up {
            new_name.push(c.to_ascii_uppercase());
            to_up = false;
        } else {
            new_name.push(c);
        }
        idx += 1;
    }
    println!("new name:{new_name}");
    return new_name;
}

fn gen_insert_fn(table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
    let struct_name = gen_struct_name(table_name);
    let mut ret = String::new();
    ret.push_str("pub async fn insert(conn: &mut PgConnection, obj: ");
    ret.push_str(struct_name.as_str());
    ret.push_str(") {\n");

    ret.push_str( format!("    let mut sql = sql_builder::SqlBuilder::insert_into(\"{table_name}\");\n").as_str());

    let mut fields = vec![];
    let mut values = vec![];
    for c in column_infos {
        if c.is_nullable == "NO" {
            fields.push(c.column_name.as_str());
            values.push("obj.".to_string() + c.column_name.as_str())
        } else {
            fields.push(c.column_name.as_str());
            values.push("obj.".to_string() + c.column_name.as_str() + ".unwrap()")
        }
    }
    for x in fields {
        ret.push_str("    sql.field(\"");
        ret.push_str(x);
        ret.push_str("\");\n")
    }
    ret.push_str("    sql.values(&[\n");
    for v in values {
        ret.push_str("        sql_builder::quote(");
        ret.push_str(v.as_str());
        ret.push_str(".field_to_string()),\n")
    }
    ret.remove(ret.len() - 2);
    ret.push_str("    ]);\n");
    ret.push_str("    let sql = sql.sql().unwrap();\n");
    ret.push_str("}\n\n\n");
    println!("insert function:\n{}", ret);
    ret
}


fn gen_batch_insert_fn(table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
    let struct_name = gen_struct_name(table_name);
    let mut ret = String::new();
    ret.push_str("pub async fn batch_insert(conn: &mut PgConnection, objs: Vec<");
    ret.push_str(struct_name.as_str());
    ret.push_str(">) {\n");

    ret.push_str( format!("    let mut sql = sql_builder::SqlBuilder::insert_into(\"{table_name}\");\n").as_str());

    let mut fields = vec![];
    let mut values = vec![];
    for c in column_infos {
        if c.is_nullable == "NO" {
            fields.push(c.column_name.as_str());
            values.push("obj.".to_string() + c.column_name.as_str())
        } else {
            fields.push(c.column_name.as_str());
            values.push("obj.".to_string() + c.column_name.as_str() + ".unwrap()")
        }
    }
    for x in fields {
        ret.push_str("    sql.field(\"");
        ret.push_str(x);
        ret.push_str("\");\n")
    }

    ret.push_str("    for obj in objs {\n");

    ret.push_str("        sql.values(&[\n");
    for v in values {
        ret.push_str("            sql_builder::quote(");
        ret.push_str(v.as_str());
        ret.push_str(".field_to_string()),\n")
    }
    ret.remove(ret.len() - 2);
    ret.push_str("        ]);\n");
    ret.push_str("    }\n");

    ret.push_str("    let sql = sql.sql().unwrap();\n");
    ret.push_str("}\n\n\n");
    println!("batch insert function:\n{}", ret);
    ret
}


#[cfg(test)]
mod test {
    use std::time::SystemTime;
    use chrono::{DateTime, FixedOffset, Utc};
    use crate::generator::gen_struct_name;
    use sqlx::{Connection, PgConnection};
    use sqlx::postgres::types::{PgInterval, PgTimeTz};
    use sqlx::types::Decimal;
    use crate::field_to_string::FieldToString;

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
    #[test]
    fn name_struct_test() {
        let name = "group_history";
        gen_struct_name(name);
    }

    #[tokio::test]
    async fn select_test() {


        let conn_url = "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(conn_url).await.unwrap();
        let sql = format!("select * from test_table");

        let columns: Vec<TestTable> = sqlx::query_as(sql.as_str())
            .fetch_all(&mut conn)
            .await
            .unwrap();
        println!("columns:{:?}", columns)
    }

    #[test]
    fn to_string_test() {
        let now: DateTime<Utc> = DateTime::from(SystemTime::now());
        println!("now:{}", now.to_string())
    }

    pub async fn insert(conn: &mut PgConnection, obj: TestTable) {
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
        sql.values(&[sql_builder::quote(obj.id.field_to_string()) ,sql_builder::quote(obj.b1.field_to_string()) ,sql_builder::quote(obj.b2.unwrap().field_to_string()) ,sql_builder::quote(obj.c1.field_to_string()) ,sql_builder::quote(obj.c2.unwrap().field_to_string()) ,sql_builder::quote(obj.i4.field_to_string()) ,sql_builder::quote(obj.i41.unwrap().field_to_string()) ,sql_builder::quote(obj.r1.field_to_string()) ,sql_builder::quote(obj.r2.unwrap().field_to_string()) ,sql_builder::quote(obj.d1.field_to_string()) ,sql_builder::quote(obj.d2.unwrap().field_to_string()) ,sql_builder::quote(obj.t1.field_to_string()) ,sql_builder::quote(obj.t2.field_to_string()) ,sql_builder::quote(obj.t3.unwrap().field_to_string()) ,sql_builder::quote(obj.t4.unwrap().field_to_string()) ,sql_builder::quote(obj.byte1.unwrap().field_to_string()) ,sql_builder::quote(obj.interval1.unwrap().field_to_string()) ,sql_builder::quote(obj.big1.unwrap().field_to_string()) ,sql_builder::quote(obj.big2.unwrap().field_to_string()) ,sql_builder::quote(obj.ts1.field_to_string()) ,sql_builder::quote(obj.ts2.unwrap().field_to_string()) ,sql_builder::quote(obj.date1.unwrap().field_to_string()) ,sql_builder::quote(obj.date2.unwrap().field_to_string()) ,sql_builder::quote(obj.time1.field_to_string()) ,sql_builder::quote(obj.time2.unwrap().field_to_string()) ,sql_builder::quote(obj.uid1.field_to_string()) ,sql_builder::quote(obj.json1.unwrap().field_to_string()) ,sql_builder::quote(obj.json2.unwrap().field_to_string()) ,sql_builder::quote(obj.i5.unwrap().field_to_string()) ]);
        let sql = sql.sql().unwrap();
        println!("sql:\n{}", sql)
    }




    #[tokio::test]
    async fn insert_test() {
        let obj = gen_test_table_obj();
        let conn_url = "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(conn_url).await.unwrap();
        insert(&mut conn, obj).await;
    }

    #[tokio::test]
    async fn test_query_1() {
        let conn_url = "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(conn_url).await.unwrap();
        let columns: Vec<TestTable> = sqlx::query_as("select * from test_table")
            .fetch_all(&mut conn)
            .await
            .unwrap();
        println!("columns:{:?}", columns)
    }

    pub async fn batch_insert(conn: &mut PgConnection, objs: Vec<TestTable>) {
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
            sql.values(&[sql_builder::quote(obj.id.field_to_string()) ,sql_builder::quote(obj.b1.field_to_string()) ,
                sql_builder::quote(obj.b2.unwrap().field_to_string()) ,sql_builder::quote(obj.c1.field_to_string()) ,
                sql_builder::quote(obj.c2.unwrap().field_to_string()) ,sql_builder::quote(obj.i4.field_to_string()) ,
                sql_builder::quote(obj.i41.unwrap().field_to_string()) ,sql_builder::quote(obj.r1.field_to_string()) ,
                sql_builder::quote(obj.r2.unwrap().field_to_string()) ,sql_builder::quote(obj.d1.field_to_string()) ,
                sql_builder::quote(obj.d2.unwrap().field_to_string()) ,sql_builder::quote(obj.t1.field_to_string()) ,
                sql_builder::quote(obj.t2.field_to_string()) ,sql_builder::quote(obj.t3.unwrap().field_to_string()) ,
                sql_builder::quote(obj.t4.unwrap().field_to_string()) ,sql_builder::quote(obj.byte1.unwrap().field_to_string()) ,
                sql_builder::quote(obj.interval1.unwrap().field_to_string()) ,sql_builder::quote(obj.big1.unwrap().field_to_string()) ,
                sql_builder::quote(obj.big2.unwrap().field_to_string()) ,sql_builder::quote(obj.ts1.field_to_string()) ,
                sql_builder::quote(obj.ts2.unwrap().field_to_string()) ,sql_builder::quote(obj.date1.unwrap().field_to_string()) ,
                sql_builder::quote(obj.date2.unwrap().field_to_string()) ,sql_builder::quote(obj.time1.field_to_string()) ,
                sql_builder::quote(obj.time2.unwrap().field_to_string()) ,sql_builder::quote(obj.uid1.field_to_string()) ,
                sql_builder::quote(obj.json1.unwrap().field_to_string()) ,sql_builder::quote(obj.json2.unwrap().field_to_string()) ,
                sql_builder::quote(obj.i5.unwrap().field_to_string()) ]);
        }

        let sql = sql.sql().unwrap();
        println!("sql:\n{}", sql)
    }

    #[tokio::test]
    async fn batch_insert_test() {
        let obj = gen_test_table_obj();
        let obj1 = gen_test_table_obj();
        let list = vec![obj, obj1];
        let conn_url = "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(conn_url).await.unwrap();
        batch_insert(&mut conn, list).await;
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
            r2: Some(3.14),
            d1: 0.0,
            d2: Some(345.0),
            t1: "4".to_string(),
            t2: "5da".to_string(),
            t3: Some("test".to_string()),
            t4: Some("adf".to_string()),
            byte1: Some(vec![2,3,4,5]),
            interval1: Some(PgInterval{
                months: 0,
                days: 1,
                microseconds: 10000,
            }),
            big1: Some(Decimal::new(234,1)),
            big2: Some(Decimal::new(223434,2)),
            ts1: Default::default(),
            ts2: Some(Default::default()),
            date1: Some(Default::default()),
            date2: Some(Default::default()),
            time1: Default::default(),
            time2: Some(PgTimeTz{ time: Default::default(), offset: FixedOffset::east_opt(0).unwrap() }),
            uid1: Default::default(),
            json1: Some(serde_json::from_str("{}").unwrap()),
            json2: Some(serde_json::from_str("[{}, {}]").unwrap()),
            i5: Some(12),
        }
    }


}
