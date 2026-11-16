use super::*;
#[derive(Clone)]
struct Pid {
    parameters: Parameters,
    integral: f64,
    previous: Option<(Instant, f64)>,
}
pub struct PidFactory;
impl ComponentFactory for PidFactory {
    fn describe(&self) -> TypeDescription {
        TypeDescription {
            type_id: "pid".into(),
            kind: Kind::Controller,
            inputs: vec!["input".into()],
            outputs: ["error", "p", "i", "d", "unconstrained", "output"]
                .into_iter()
                .map(|name| Output {
                    name: name.into(),
                    unit: Some(String::new()),
                })
                .collect(),
            parameters: vec![
                float_parameter("setpoint", 0., -1e12, 1e12, ""),
                float_parameter("kp", 1., 0., 1e12, ""),
                float_parameter("ki", 0., 0., 1e12, "1/s"),
                float_parameter("kd", 0., 0., 1e12, "s"),
                float_parameter("output_min", 0., -1e12, 1e12, ""),
                float_parameter("output_max", 100., -1e12, 1e12, ""),
                float_parameter("max_gap", 10., 0.000001, 86400., "s"),
            ],
            proposal: Some("output".into()),
        }
    }
    fn create(&self, p: &Parameters) -> Result<Box<dyn Component>, String> {
        check(p)?;
        Ok(Box::new(Pid {
            parameters: p.clone(),
            integral: 0.,
            previous: None,
        }))
    }
}
fn check(p: &Parameters) -> Result<(), String> {
    if p["output_min"].as_f64().ok_or("invalid_limits")?
        >= p["output_max"].as_f64().ok_or("invalid_limits")?
    {
        return Err("invalid_limits".into());
    }
    Ok(())
}
impl Pid {
    fn p(&self, name: &str) -> f64 {
        self.parameters[name].as_f64().expect("schema validated")
    }
}
impl Component for Pid {
    fn process(&mut self, inputs: &Inputs) -> Result<Calculation, String> {
        let x = number(inputs, "input")?;
        let at = inputs["input"]
            .metadata
            .as_ref()
            .ok_or("missing_clock")?
            .acquired_at;
        let error = self.p("setpoint") - x;
        let p = self.p("kp") * error;
        let (delta, d) = if let Some((old, previous)) = self.previous {
            let dt = at
                .checked_duration_since(old)
                .filter(|d| !d.is_zero())
                .ok_or("non_increasing_time")?
                .as_secs_f64();
            if dt > self.p("max_gap") {
                return Err("input_gap".into());
            }
            (
                self.p("ki") * error * dt,
                -self.p("kd") * (x - previous) / dt,
            )
        } else {
            (0., 0.)
        };
        let min = self.p("output_min");
        let max = self.p("output_max");
        let candidate = self.integral + delta;
        let candidate_output = p + candidate + d;
        if ![error, p, delta, d, candidate, candidate_output]
            .iter()
            .all(|v| v.is_finite())
        {
            return Err("non_finite_output".into());
        }
        let integral =
            if (candidate_output > max && delta > 0.) || (candidate_output < min && delta < 0.) {
                self.integral
            } else {
                candidate
            };
        let unconstrained = p + integral + d;
        let output = unconstrained.clamp(min, max);
        self.integral = integral;
        self.previous = Some((at, x));
        Ok([
            ("error", error),
            ("p", p),
            ("i", integral),
            ("d", d),
            ("unconstrained", unconstrained),
            ("output", output),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v))
        .collect())
    }
    fn reset(&mut self) {
        self.integral = 0.;
        self.previous = None;
    }
    fn reconfigure(&self, p: &Parameters) -> Result<Box<dyn Component>, String> {
        check(p)?;
        let mut next = self.clone();
        next.parameters = p.clone();
        next.integral = next
            .integral
            .clamp(next.p("output_min"), next.p("output_max"));
        Ok(Box::new(next))
    }
}
