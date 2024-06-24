pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
    #[tokio::test]
    async fn try_gen() {
        let conn_url = "postgres://postgres:123456@localhost/jixin_message";
        generator::gen_file(conn_url, "test_table")
            .await
            .expect("TODO: panic message");
    }
}

pub mod generator;
pub mod field_to_string;