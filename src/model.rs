use std::time::{Duration, SystemTime};

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Float(f64),
    Integer(i64),
    Boolean(bool),
}

#[derive(Debug, Clone)]
pub struct Observation {
    pub source: String,
    pub unit: String,
    pub completed_at: SystemTime,
    pub reading: Result<Value, String>,
}

#[derive(Debug, Clone)]
pub enum Spec {
    Float { min: f64, max: f64 },
    Integer { min: i64, max: i64 },
    Boolean,
}

#[derive(Debug, Clone)]
pub struct ParameterDescription {
    pub id: String,
    pub spec: Spec,
    pub unit: String,
    pub writable: bool,
    pub default_poll_period: Option<Duration>,
}

impl Spec {
    pub fn valid(&self) -> bool {
        match self {
            Self::Float { min, max } => min.is_finite() && max.is_finite() && min <= max,

            Self::Integer { min, max } => min <= max,
            Self::Boolean => true,
        }
    }

    pub fn accepts(&self, value: &Value) -> bool {
        self.valid()
            && match (self, value) {
                (Self::Float { min, max }, Value::Float(value)) => {
                    value.is_finite() && value >= min && value <= max
                }

                (Self::Integer { min, max }, Value::Integer(value)) => value >= min && value <= max,
                (Self::Boolean, Value::Boolean(_)) => true,

                _ => false,
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boolean_values_are_strictly_typed() {
        assert!(Spec::Boolean.valid());
        for value in [false, true] {
            assert!(Spec::Boolean.accepts(&Value::Boolean(value)));
            assert!(!Spec::Integer { min: 0, max: 1 }.accepts(&Value::Boolean(value)));
            assert!(!Spec::Float { min: 0.0, max: 1.0 }.accepts(&Value::Boolean(value)));
        }
        for value in [
            Value::Integer(0),
            Value::Integer(1),
            Value::Float(0.0),
            Value::Float(1.0),
        ] {
            assert!(!Spec::Boolean.accepts(&value));
        }
    }

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
