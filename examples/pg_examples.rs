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