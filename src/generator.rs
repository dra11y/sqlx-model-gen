use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;

use inflector::{cases::classcase::to_class_case, string::singularize::to_singular};
use serde::{Deserialize, Serialize};

const RUST_KEYWORDS: [&str; 51] = [
    "abstract", "alignof", "as", "async", "await", "become", "box", "break", "const", "continue",
    "crate", "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "true", "type", "typeof",
    "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

pub(crate) fn escape_rs_keyword_name(input: &str) -> String {
    match RUST_KEYWORDS.contains(&input) {
        true => format!("r#{input}"),
        false => input.to_string(),
    }
}

#[derive(sqlx::FromRow, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableInfo {
    pub table_name: String,
    is_view: bool,
}

impl TableInfo {
    pub fn is_view(&self) -> bool {
        self.is_view
    }

    pub fn is_not_view(&self) -> bool {
        !self.is_view
    }
}

#[derive(sqlx::FromRow, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub column_name: String,
    pub table_name: String,
    pub schema_name: String,
    pub is_nullable: bool,
    pub udt_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructInfo {
    pub struct_name: String,
    pub content: String,
    // pub fields: Vec<FieldInfo>,
    pub user_types: HashMap<String, (String, String)>,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldInfo {
    pub name: String,
    pub column_info: ColumnInfo,
    pub is_option: bool,
    pub field_type: String,
}

pub trait Generator {
    fn get_tables(
        &self,
        database_url: &str,
        schema: &str,
    ) -> impl Future<Output = Vec<TableInfo>> + Send;

    fn query_all_columns(
        &self,
        database_url: &str,
        schemas: &[&str],
    ) -> impl Future<Output = Vec<ColumnInfo>> + Send;

    fn query_columns(
        &self,
        database_url: &str,
        table_name: &str,
    ) -> impl Future<Output = Vec<ColumnInfo>> + Send;

    ///
    /// This is a tool for gen table_name mapping Struct and basic sql, such as insert.
    ///
    /// Based on sqlx and sql_builder
    ///
    /// generate a {table_name}.rs for one table
    ///
    /// now support
    ///
    /// MySql
    ///
    /// postgres
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
    /// use sqlx_model_gen::generator::Generator;
    /// use sqlx_model_gen::pg_generator;
    ///     let gen = pg_generator::PgGenerator{};
    ///     let database_url = "postgres://postgres:123456@localhost/jixin_message?&stringtype=unspecified";
    ///     let table_name = "test_table";
    ///     let result = gen.gen_file(database_url, table_name).await;
    ///     println!("result:{:?}", result)
    /// ```
    /// after run , if success, test_table.rs file would generate.
    ///
    fn gen_struct_module(
        &self,
        database_url: &str,
        table_name: &str,
        extra_derives: &[&str],
        extra_annotations: &[&str],
        udt_mappings: Option<&HashMap<String, String>>,
    ) -> impl std::future::Future<Output = Result<StructInfo, sqlx::Error>> + Send
    where
        Self: Sync,
    {
        async move {
            let columns: Vec<ColumnInfo> = self.query_columns(database_url, table_name).await;

            if columns.is_empty() {
                return Err(sqlx::Error::TypeNotFound {
                    type_name: table_name.to_string(),
                });
            }

            println!("Columns: {:?}", columns);

            let udt_mappings: HashMap<String, String> = udt_mappings
                .cloned()
                .unwrap_or(HashMap::new())
                .into_iter()
                .map(|(k, v)| (k.to_uppercase(), v))
                .collect();

            let user_types: HashMap<String, (String, String)> = columns
                .iter()
                .filter_map(|col| {
                    udt_mappings
                        .get(&col.udt_name.to_uppercase())
                        .map(|rs_type| {
                            (
                                col.udt_name.clone(),
                                (col.udt_name.clone(), rs_type.clone()),
                            )
                        })
                })
                .collect();

            let struct_name = self.gen_struct_name(table_name);
            let content = self.gen_struct(
                table_name,
                &columns,
                extra_derives,
                extra_annotations,
                &udt_mappings,
            );

            Ok(StructInfo {
                struct_name,
                content,
                user_types,
                columns,
            })
        }
    }

    fn get_mapping_type(&self, sql_type: &str, udt_mappings: &HashMap<String, String>) -> String;

    fn gen_struct(
        &self,
        table_name: &str,
        column_infos: &[ColumnInfo],
        extra_derives: &[&str],
        extra_annotations: &[&str],
        udt_mappings: &HashMap<String, String>,
    ) -> String {
        let mut st = String::new();
        st.push_str("use serde::{Deserialize, Serialize};\n");
        st.push_str("#[derive(sqlx::FromRow, Clone, Debug, PartialEq, Serialize, Deserialize");
        if !extra_derives.is_empty() {
            st.push_str(", ");
            st.push_str(extra_derives.join(", ").as_str());
        }
        st.push_str(")]\n");
        for annotation in extra_annotations {
            st.push_str(annotation);
            st.push('\n');
        }
        st.push_str("pub struct ");
        st.push_str(self.gen_struct_name(table_name).as_str());
        st.push_str(" {\n");
        for c in column_infos {
            let field_name = escape_rs_keyword_name(c.column_name.as_str());
            st.push_str("    pub ");
            st.push_str(field_name.as_str());
            let ctype = self.get_mapping_type(c.udt_name.as_str(), udt_mappings);
            st.push_str(": ");
            if c.is_nullable {
                st.push_str("Option<");
                st.push_str(ctype.as_str());
                st.push('>')
            } else {
                st.push_str(ctype.as_str());
            }
            st.push_str(",\n");
        }
        st.push_str("}\n");
        st
    }

    fn gen_struct_name(&self, table: &str) -> String {
        to_class_case(&to_singular(table))
    }

    fn gen_field_and_value_str(&self, column_infos: &[ColumnInfo], contain_id: bool) -> String {
        let mut ret = String::new();

        let mut fields = vec![];
        let mut values = vec![];
        for c in column_infos {
            if c.column_name == "id" && !contain_id {
                continue;
            }
            if c.is_nullable {
                fields.push(c.column_name.as_str());
                values.push("obj.".to_string() + c.column_name.as_str() + ".unwrap()")
            } else {
                fields.push(c.column_name.as_str());
                values.push("obj.".to_string() + c.column_name.as_str())
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

    fn gen_field_and_batch_values_str(
        &self,
        column_infos: &[ColumnInfo],
        contain_id: bool,
    ) -> String {
        let mut ret = String::new();
        let mut fields = vec![];
        let mut values = vec![];
        for c in column_infos {
            if c.column_name == "id" && !contain_id {
                continue;
            }
            if c.is_nullable {
                fields.push(c.column_name.as_str());
                values.push("obj.".to_string() + c.column_name.as_str() + ".unwrap()")
            } else {
                fields.push(c.column_name.as_str());
                values.push("obj.".to_string() + c.column_name.as_str())
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

    fn gen_insert_returning_id_fn(
        &self,
        _table_name: &str,
        _column_infos: &[ColumnInfo],
    ) -> String {
        String::new()
    }
    fn gen_insert_fn(&self, _table_name: &str, _column_infos: &[ColumnInfo]) -> String {
        String::new()
    }

    fn gen_batch_insert_returning_id_fn(
        &self,
        _table_name: &str,
        _column_infos: &[ColumnInfo],
    ) -> String {
        String::new()
    }

    fn gen_batch_insert_fn(&self, _table_name: &str, _column_infos: &[ColumnInfo]) -> String {
        String::new()
    }

    fn gen_select_sql(&self, table_name: &str, column_infos: &[ColumnInfo]) -> String {
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

    fn gen_select_sql_fn(&self, table_name: &str, column_infos: &[ColumnInfo]) -> String {
        let sql = self.gen_select_sql(table_name, column_infos);
        format!(
            r#"
pub fn select_sql() -> String {{
    "{sql}".to_string()
}}
        "#
        )
    }
    fn gen_select_by_id_fn(&self, _table_name: &str, _column_infos: &[ColumnInfo]) -> String {
        String::new()
    }

    fn gen_delete_by_id_sql(&self, table_name: &str) -> String {
        format!(r#"delete from {table_name} where id="#)
    }

    fn gen_delete_by_id_fn(&self, _table_name: &str) -> String {
        String::new()
    }
}
