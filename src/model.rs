use std::time::SystemTime;

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Float(f64),
    Integer(i64),
}

#[derive(Debug, Clone)]
pub struct Observation {
    pub source: String,
    pub unit: String,
    pub completed_at: SystemTime,
    pub reading: Result<Value, String>,
}

#[derive(Debug)]
pub enum Spec {
    Float { min: f64, max: f64 },
    Integer { min: i64, max: i64 },
}

#[derive(Debug)]
pub struct ParameterDescription {
    pub id: String,
    pub spec: Spec,
    pub unit: String,
    pub writable: bool,
}

impl Spec {
    pub fn valid(&self) -> bool {
        match self {
            Self::Float { min, max } => min.is_finite() && max.is_finite() && min <= max,

            Self::Integer { min, max } => min <= max,
        }
    }

    pub fn accepts(&self, value: &Value) -> bool {
        self.valid()
            && match (self, value) {
                (Self::Float { min, max }, Value::Float(value)) => {
                    value.is_finite() && value >= min && value <= max
                }

                (Self::Integer { min, max }, Value::Integer(value)) => value >= min && value <= max,

                _ => false,
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_integer_values() {
        let spec = Spec::Integer { min: 0, max: 100 };

        assert!(spec.accepts(&Value::Integer(0)));
        assert!(spec.accepts(&Value::Integer(50)));
        assert!(spec.accepts(&Value::Integer(100)));

        assert!(!spec.accepts(&Value::Integer(-1)));
        assert!(!spec.accepts(&Value::Integer(101)));
        assert!(!spec.accepts(&Value::Float(50.0)));
    }
}
