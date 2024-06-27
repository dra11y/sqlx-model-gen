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

            let select_sql = self.gen_select_sql_fn(table_name, &columns);
            total_str.push_str(select_sql.as_str());

            let select_by_id_fn = self.gen_select_by_id_fn(table_name, &columns);
            total_str.push_str(select_by_id_fn.as_str());


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

    fn gen_select_sql(&self, table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
        let mut sql = String::from_str("select ").unwrap();
        for c in column_infos {
            sql.push_str(c.column_name.as_str());
            sql.push_str(", ")
        }
        sql.remove(sql.len() - 2);
        sql.push_str(" from ");
        sql.push_str(table_name);
        sql
    }

    fn gen_select_sql_fn(&self, table_name: &str, column_infos: &Vec<ColumnInfo>) -> String {
        let sql = self.gen_select_sql(table_name, column_infos);
        format!(r#"
pub fn select_sql() -> String {{
    "{sql}".to_string()
}}
        "#)
    }
    fn gen_select_by_id_fn(&self, _table_name: &str, _column_infos: &Vec<ColumnInfo>) -> String {
        String::new()
    }
}


