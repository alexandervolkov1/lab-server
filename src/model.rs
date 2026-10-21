#[derive(Debug, PartialEq)]
pub enum Value {
    Float(f64),
}

#[derive(Debug)]
pub enum Spec {
    Float { min: f64, max: f64 },
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
