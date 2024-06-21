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
#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct GroupHistory {
    id: i64,
    message_id: i64,
    owner_id: String,
    sender_id: String,
    group_id: String,
    content: String,
    read_flag: String,
    del_flag: String,
    record_type: String,
    read_time: String,
    create_time: String,
    command: i32,
    mobile_ack: String,
    pc_ack: String,
    read_message_id: Option<i64>,
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
    gen_struct(table_name, &columns);
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
    st.push_str("}\n");
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

#[cfg(test)]
mod test {
    use crate::generator::gen_struct_name;
    use sqlx::{Connection, PgConnection};

    #[test]
    fn name_struct_test() {
        let name = "group_history";
        gen_struct_name(name);
    }

    #[tokio::test]
    async fn select_test() {
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

        let conn_url = "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
        let mut conn: PgConnection = PgConnection::connect(conn_url).await.unwrap();
        let sql = format!("select * from test_table");

        let columns: Vec<TestTable> = sqlx::query_as(sql.as_str())
            .fetch_all(&mut conn)
            .await
            .unwrap();
        println!("columns:{:?}", columns)
    }
}
