use super::*;
#[derive(Clone)]
struct Ema {
    tau: f64,
    previous: Option<(Instant, f64)>,
}
pub struct EmaFactory;
impl ComponentFactory for EmaFactory {
    fn describe(&self) -> TypeDescription {
        TypeDescription {
            type_id: "ema".into(),
            kind: Kind::Filter,
            inputs: vec!["input".into()],
            outputs: vec![Output {
                name: "value".into(),
                unit: None,
            }],
            parameters: vec![float_parameter("tau", 1., 0.000001, 86_400., "s")],
            proposal: None,
        }
    }
    fn create(&self, p: &Parameters) -> Result<Box<dyn Component>, String> {
        Ok(Box::new(Ema {
            tau: p["tau"].as_f64().ok_or("invalid_tau")?,
            previous: None,
        }))
    }
}
impl Component for Ema {
    fn process(&mut self, inputs: &Inputs) -> Result<Calculation, String> {
        let x = number(inputs, "input")?;
        let at = inputs["input"]
            .metadata
            .as_ref()
            .ok_or("missing_clock")?
            .acquired_at;
        let y = if let Some((old, y)) = self.previous {
            let dt = at
                .checked_duration_since(old)
                .filter(|d| !d.is_zero())
                .ok_or("non_increasing_time")?
                .as_secs_f64();
            let alpha = -(-dt / self.tau).exp_m1();
            (1. - alpha) * y + alpha * x
        } else {
            x
        };
        if !y.is_finite() {
            return Err("non_finite_output".into());
        }
        self.previous = Some((at, y));
        Ok(BTreeMap::from([("value".into(), y)]))
    }
    fn reset(&mut self) {
        self.previous = None;
    }
    fn reconfigure(&self, p: &Parameters) -> Result<Box<dyn Component>, String> {
        let mut next = self.clone();
        next.tau = p["tau"].as_f64().ok_or("invalid_tau")?;
        Ok(Box::new(next))
    }
}
