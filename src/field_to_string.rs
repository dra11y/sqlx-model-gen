use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::postgres::types::{PgInterval, PgTimeTz};
use sqlx::types::Decimal;
use uuid::Uuid;

pub trait FieldToString {
    fn field_to_string(&self) -> String;
}

impl FieldToString for i8 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}
impl FieldToString for i16 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for i32 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for i64 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for i128 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for isize {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}


impl FieldToString for u8 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}
impl FieldToString for u16 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for u32 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for u64 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for u128 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for usize {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for String {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for &str {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for f32 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for f64 {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for bool {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for Vec<u8> {
    fn field_to_string(&self) -> String {
        format!("{:?}", self)
    }
}

impl FieldToString for PgInterval {
    fn field_to_string(&self) -> String {
        // 2 months 3 days 4 hours 5 minutes 6000.3 seconds
        format!("{} months {} days {} seconds", self.months, self.days, self.microseconds as f32 / 1000f32)
    }
}

impl FieldToString for Decimal {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for NaiveDateTime {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for DateTime<chrono::Utc> {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for DateTime<chrono::Local> {
    fn field_to_string(&self) -> String {
        let str = self.to_string();
        // 2024-06-27 10:05:49.96
        let str = &str[0..22];
        str.to_string()
        // 2024-06-27T10:05:49.96
        // let str = str.replace(" ", "T");
        // println!("{str}");
        // str
    }
}

impl FieldToString for NaiveDate {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for NaiveTime {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for Uuid {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}

impl FieldToString for PgTimeTz {
    fn field_to_string(&self) -> String {
        format!("{} {}", self.time, self.offset)
    }
}

impl FieldToString for serde_json::Value {
    fn field_to_string(&self) -> String {
        self.to_string()
    }
}