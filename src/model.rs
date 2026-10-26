use std::time::SystemTime;

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Float(f64),
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
}

#[derive(Debug)]
pub struct SignalDescription {
    pub id: String,
    pub spec: Spec,
    pub unit: String,
}

impl Spec {
    pub fn valid(&self) -> bool {
        match self {
            Self::Float { min, max } => min.is_finite() && max.is_finite() && min <= max,
        }
    }

    pub fn accepts(&self, value: &Value) -> bool {
        self.valid()
            && match (self, value) {
                (Self::Float { min, max }, Value::Float(value)) => {
                    value.is_finite() && value >= min && value <= max
                }
            }
    }
}
