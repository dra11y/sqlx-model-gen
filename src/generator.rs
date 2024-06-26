use std::future::Future;
use std::str::FromStr;

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct ColumnInfo {
    pub column_name: String,
    pub is_nullable: String,
    pub data_type: String,
}


pub trait Generator {

    // async fn query_columns(&self, conn_url: &str, table_name: &str) -> Vec<ColumnInfo>;
    fn query_columns(&self, conn_url: &str, table_name: &str) -> impl Future<Output = Vec<ColumnInfo>> + Send;

    ///
    /// This is a tool for gen table_name mapping Struct and basic sql, such as insert.
    ///
    /// Based on sqlx and sql_builder
    ///
    /// generate a {table_name}.rs for one table
    ///
    /// now support
    ///
    ///! MySql
    ///
    ///
    ///! postgres
    ///
    /// include:
    ///  a table name struct with field.
    ///
    ///  insert function
    ///
    ///  insert_returning_id function
    ///
    ///  batch_insert function
    ///
    ///  batch_insert_returning_id function
    ///
    ///  # Examples
    /// ```
    /// use sql_wrapper::generator::Generator;
    /// use sql_wrapper::pg_generator;
    ///     let gen = pg_generator::PgGenerator{};
    ///     let conn_url = "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
    ///     let table_name = "test_table";
    ///     let result = gen.gen_file(conn_url, table_name).await;
    ///     println!("result:{:?}", result)
    /// ```
    /// after run , if success, test_table.rs file would generate.
    ///
    /// test_table.rs content
    /// ```
    /// use sqlx::PgConnection;
    /// use sql_wrapper::field_to_string::FieldToString;
    /// #[derive(sqlx::FromRow, Debug, PartialEq)]
    /// pub struct TestTable {
    ///     id: i64,
    ///     b1: bool,
    ///     b2: Option<bool>,
    ///     c1: String,
    ///     c2: Option<String>,
    ///     i4: i32,
    ///     i41: Option<i32>,
    ///     r1: f32,
    ///     r2: Option<f64>,
    ///     d1: f64,
    ///     d2: Option<f64>,
    ///     t1: String,
    ///     t2: String,
    ///     t3: Option<String>,
    ///     t4: Option<String>,
    ///     byte1: Option<Vec<u8>>,
    ///     interval1: Option<sqlx::postgres::types::PgInterval>,
    ///     big1: Option<sqlx::types::Decimal>,
    ///     big2: Option<sqlx::types::Decimal>,
    ///     ts1: chrono::NaiveDateTime,
    ///     ts2: Option<chrono::DateTime<chrono::Utc>>,
    ///     date1: Option<chrono::NaiveDate>,
    ///     date2: Option<chrono::NaiveDate>,
    ///     time1: chrono::NaiveTime,
    ///     time2: Option<sqlx::postgres::types::PgTimeTz>,
    ///     uid1: uuid::Uuid,
    ///     json1: Option<serde_json::Value>,
    ///     json2: Option<serde_json::Value>,
    ///     i5: Option<i16>,
    /// }
    ///
    ///
    ///
    /// pub async fn insert_returning_id(conn: &mut PgConnection, obj: TestTable) -> i64 {
    ///     let mut sql = sql_builder::SqlBuilder::insert_into("test_table");
    ///     sql.field("b1");
    ///     sql.field("b2");
    ///     sql.field("c1");
    ///     sql.field("c2");
    ///     sql.field("i4");
    ///     sql.field("i41");
    ///     sql.field("r1");
    ///     sql.field("r2");
    ///     sql.field("d1");
    ///     sql.field("d2");
    ///     sql.field("t1");
    ///     sql.field("t2");
    ///     sql.field("t3");
    ///     sql.field("t4");
    ///     sql.field("byte1");
    ///     sql.field("interval1");
    ///     sql.field("big1");
    ///     sql.field("big2");
    ///     sql.field("ts1");
    ///     sql.field("ts2");
    ///     sql.field("date1");
    ///     sql.field("date2");
    ///     sql.field("time1");
    ///     sql.field("time2");
    ///     sql.field("uid1");
    ///     sql.field("json1");
    ///     sql.field("json2");
    ///     sql.field("i5");
    ///     sql.values(&[
    ///         sql_builder::quote(obj.b1.field_to_string()),
    ///         sql_builder::quote(obj.b2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.c1.field_to_string()),
    ///         sql_builder::quote(obj.c2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.i4.field_to_string()),
    ///         sql_builder::quote(obj.i41.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.r1.field_to_string()),
    ///         sql_builder::quote(obj.r2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.d1.field_to_string()),
    ///         sql_builder::quote(obj.d2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.t1.field_to_string()),
    ///         sql_builder::quote(obj.t2.field_to_string()),
    ///         sql_builder::quote(obj.t3.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.t4.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.byte1.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.interval1.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.big1.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.big2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.ts1.field_to_string()),
    ///         sql_builder::quote(obj.ts2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.date1.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.date2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.time1.field_to_string()),
    ///         sql_builder::quote(obj.time2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.uid1.field_to_string()),
    ///         sql_builder::quote(obj.json1.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.json2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.i5.unwrap().field_to_string())
    ///     ]);
    ///
    ///     sql.returning_id();
    ///     let sql = sql.sql().unwrap();
    ///     let columns:(i64,) = sqlx::query_as(sql.as_str()).fetch_one(conn).await.unwrap();
    ///     return columns.0;
    /// }
    ///
    /// pub async fn insert(conn: &mut PgConnection, obj: TestTable) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error>  {
    ///     let mut sql = sql_builder::SqlBuilder::insert_into("test_table");
    ///     sql.field("id");
    ///     sql.field("b1");
    ///     sql.field("b2");
    ///     sql.field("c1");
    ///     sql.field("c2");
    ///     sql.field("i4");
    ///     sql.field("i41");
    ///     sql.field("r1");
    ///     sql.field("r2");
    ///     sql.field("d1");
    ///     sql.field("d2");
    ///     sql.field("t1");
    ///     sql.field("t2");
    ///     sql.field("t3");
    ///     sql.field("t4");
    ///     sql.field("byte1");
    ///     sql.field("interval1");
    ///     sql.field("big1");
    ///     sql.field("big2");
    ///     sql.field("ts1");
    ///     sql.field("ts2");
    ///     sql.field("date1");
    ///     sql.field("date2");
    ///     sql.field("time1");
    ///     sql.field("time2");
    ///     sql.field("uid1");
    ///     sql.field("json1");
    ///     sql.field("json2");
    ///     sql.field("i5");
    ///     sql.values(&[
    ///         sql_builder::quote(obj.id.field_to_string()),
    ///         sql_builder::quote(obj.b1.field_to_string()),
    ///         sql_builder::quote(obj.b2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.c1.field_to_string()),
    ///         sql_builder::quote(obj.c2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.i4.field_to_string()),
    ///         sql_builder::quote(obj.i41.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.r1.field_to_string()),
    ///         sql_builder::quote(obj.r2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.d1.field_to_string()),
    ///         sql_builder::quote(obj.d2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.t1.field_to_string()),
    ///         sql_builder::quote(obj.t2.field_to_string()),
    ///         sql_builder::quote(obj.t3.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.t4.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.byte1.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.interval1.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.big1.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.big2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.ts1.field_to_string()),
    ///         sql_builder::quote(obj.ts2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.date1.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.date2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.time1.field_to_string()),
    ///         sql_builder::quote(obj.time2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.uid1.field_to_string()),
    ///         sql_builder::quote(obj.json1.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.json2.unwrap().field_to_string()),
    ///         sql_builder::quote(obj.i5.unwrap().field_to_string())
    ///     ]);
    ///
    ///     let sql = sql.sql().unwrap();
    ///     sqlx::query(sql.as_str()).execute(conn).await
    ///
    /// }
    ///
    ///
    /// pub async fn batch_insert_returning_id(conn: &mut PgConnection, objs: Vec<TestTable>) -> Vec<i64> {
    ///     let mut sql = sql_builder::SqlBuilder::insert_into("test_table");
    ///     sql.field("b1");
    ///     sql.field("b2");
    ///     sql.field("c1");
    ///     sql.field("c2");
    ///     sql.field("i4");
    ///     sql.field("i41");
    ///     sql.field("r1");
    ///     sql.field("r2");
    ///     sql.field("d1");
    ///     sql.field("d2");
    ///     sql.field("t1");
    ///     sql.field("t2");
    ///     sql.field("t3");
    ///     sql.field("t4");
    ///     sql.field("byte1");
    ///     sql.field("interval1");
    ///     sql.field("big1");
    ///     sql.field("big2");
    ///     sql.field("ts1");
    ///     sql.field("ts2");
    ///     sql.field("date1");
    ///     sql.field("date2");
    ///     sql.field("time1");
    ///     sql.field("time2");
    ///     sql.field("uid1");
    ///     sql.field("json1");
    ///     sql.field("json2");
    ///     sql.field("i5");
    ///     for obj in objs {
    ///         sql.values(&[
    ///             sql_builder::quote(obj.b1.field_to_string()),
    ///             sql_builder::quote(obj.b2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.c1.field_to_string()),
    ///             sql_builder::quote(obj.c2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.i4.field_to_string()),
    ///             sql_builder::quote(obj.i41.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.r1.field_to_string()),
    ///             sql_builder::quote(obj.r2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.d1.field_to_string()),
    ///             sql_builder::quote(obj.d2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.t1.field_to_string()),
    ///             sql_builder::quote(obj.t2.field_to_string()),
    ///             sql_builder::quote(obj.t3.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.t4.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.byte1.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.interval1.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.big1.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.big2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.ts1.field_to_string()),
    ///             sql_builder::quote(obj.ts2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.date1.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.date2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.time1.field_to_string()),
    ///             sql_builder::quote(obj.time2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.uid1.field_to_string()),
    ///             sql_builder::quote(obj.json1.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.json2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.i5.unwrap().field_to_string())
    ///         ]);
    ///     }
    ///
    ///
    ///     sql.returning_id();
    ///     let sql = sql.sql().unwrap();
    ///     let columns:Vec<(i64,)> = sqlx::query_as(sql.as_str()).fetch_all(conn).await.unwrap();
    ///     let mut ret = vec![];
    ///     for v in columns {
    ///         ret.push(v.0)
    ///     }
    ///     println!("insert id:{:?}", ret);
    ///     return ret;
    ///
    /// }
    ///
    ///
    /// pub async fn batch_insert(conn: &mut PgConnection, objs: Vec<TestTable>) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error>  {
    ///     let mut sql = sql_builder::SqlBuilder::insert_into("test_table");
    ///     sql.field("id");
    ///     sql.field("b1");
    ///     sql.field("b2");
    ///     sql.field("c1");
    ///     sql.field("c2");
    ///     sql.field("i4");
    ///     sql.field("i41");
    ///     sql.field("r1");
    ///     sql.field("r2");
    ///     sql.field("d1");
    ///     sql.field("d2");
    ///     sql.field("t1");
    ///     sql.field("t2");
    ///     sql.field("t3");
    ///     sql.field("t4");
    ///     sql.field("byte1");
    ///     sql.field("interval1");
    ///     sql.field("big1");
    ///     sql.field("big2");
    ///     sql.field("ts1");
    ///     sql.field("ts2");
    ///     sql.field("date1");
    ///     sql.field("date2");
    ///     sql.field("time1");
    ///     sql.field("time2");
    ///     sql.field("uid1");
    ///     sql.field("json1");
    ///     sql.field("json2");
    ///     sql.field("i5");
    ///     for obj in objs {
    ///         sql.values(&[
    ///             sql_builder::quote(obj.id.field_to_string()),
    ///             sql_builder::quote(obj.b1.field_to_string()),
    ///             sql_builder::quote(obj.b2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.c1.field_to_string()),
    ///             sql_builder::quote(obj.c2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.i4.field_to_string()),
    ///             sql_builder::quote(obj.i41.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.r1.field_to_string()),
    ///             sql_builder::quote(obj.r2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.d1.field_to_string()),
    ///             sql_builder::quote(obj.d2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.t1.field_to_string()),
    ///             sql_builder::quote(obj.t2.field_to_string()),
    ///             sql_builder::quote(obj.t3.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.t4.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.byte1.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.interval1.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.big1.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.big2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.ts1.field_to_string()),
    ///             sql_builder::quote(obj.ts2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.date1.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.date2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.time1.field_to_string()),
    ///             sql_builder::quote(obj.time2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.uid1.field_to_string()),
    ///             sql_builder::quote(obj.json1.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.json2.unwrap().field_to_string()),
    ///             sql_builder::quote(obj.i5.unwrap().field_to_string())
    ///         ]);
    ///     }
    ///
    ///
    ///     let sql = sql.sql().unwrap();
    ///     sqlx::query(sql.as_str()).execute(conn).await
    ///
    /// }
    ///
    ///
    /// ```
    ///
    ///
    fn gen_file(&self, conn_url: &str, table_name: &str) -> impl Future<Output = Result<(), sqlx::Error>> + Send where Self: Sync {
        async move {
            let columns: Vec<ColumnInfo> = self.query_columns(conn_url, table_name).await;

            if columns.is_empty() {
                println!("can not found columns. exit");
                return Ok(());
            }

            println!("columns:{:?}", columns);
            let mut total_str = String::new();

            let struct_str = self.gen_struct(table_name, &columns);
            total_str.push_str(struct_str.as_str());

            let insert_fn_str = self.gen_insert_returning_id_fn(table_name, &columns);
            total_str.push_str(insert_fn_str.as_str());

            let insert_fn_str = self.gen_insert_fn(table_name, &columns);
            total_str.push_str(insert_fn_str.as_str());


            let batch_insert_fn = self.gen_batch_insert_returning_id_fn(table_name, &columns);
            total_str.push_str(batch_insert_fn.as_str());

            let batch_insert_fn = self.gen_batch_insert_fn(table_name, &columns);
            total_str.push_str(batch_insert_fn.as_str());


            std::fs::write(format!("{table_name}.rs"), total_str).unwrap();
            Ok(())
        }
    }
    fn get_mapping_type(&self, sql_type: &str) -> String;

    fn gen_struct(&self, table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
        let mut st =
            String::from_str("#[derive(sqlx::FromRow, Debug, PartialEq)] \npub struct ").unwrap();
        st.push_str(self.gen_struct_name(table_name).as_str());
        st.push_str(" {\n");
        for c in column_infos {
            st.push_str("    ");
            st.push_str(c.column_name.as_str());
            let ctype = self.get_mapping_type(c.data_type.as_str());
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

    fn gen_struct_name(&self, table: &str) -> String {
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


    fn gen_field_and_value_str(&self, column_infos: &Vec<ColumnInfo>, contain_id: bool) -> String {
        let mut ret = String::new();

        let mut fields = vec![];
        let mut values = vec![];
        for c in column_infos {
            if c.column_name == "id" && !contain_id {
                continue;
            }
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

        ret
    }


    fn gen_field_and_batch_values_str(&self, column_infos: &Vec<ColumnInfo>, contain_id: bool) -> String {
        let mut ret = String::new();
        let mut fields = vec![];
        let mut values = vec![];
        for c in column_infos {
            if c.column_name == "id" && !contain_id {
                continue;
            }
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

        ret
    }

    fn gen_insert_returning_id_fn(&self, _table_name: &str, _column_infos: &Vec<ColumnInfo>) -> String {
        String::new()
    }
    fn gen_insert_fn(&self, _table_name: &str, _column_infos: &Vec<ColumnInfo>) -> String {
        String::new()
    }

    fn gen_batch_insert_returning_id_fn(&self, _table_name: &str, _column_infos: &Vec<ColumnInfo>) -> String {
        String::new()
    }

    fn gen_batch_insert_fn(&self, _table_name: &str, _column_infos: &Vec<ColumnInfo>) -> String {
        String::new()
    }
}


