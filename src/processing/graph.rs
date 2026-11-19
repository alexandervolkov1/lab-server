//! Dependency validation, ordering and traversal for processing components.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde_json::json;

use super::schema::identifier;
use super::{Configuration, Engine, MAX_COMPONENTS, TypeDescription, signal};
use crate::model::{ParameterDescription, Spec};
use std::time::Duration;

impl Engine {
    /// Test membership in the current graph without constructing descriptions.
    pub(crate) fn contains_signal(&self, key: &str) -> bool {
        if self.physical.contains_key(key) {
            return true;
        }
        let Some((component, output)) = key
            .strip_prefix("processing/")
            .and_then(|rest| rest.split_once('/'))
        else {
            return false;
        };
        if component.is_empty() || output.is_empty() || output.contains('/') {
            return false;
        }
        self.nodes
            .get(component)
            .is_some_and(|node| node.description.outputs.iter().any(|o| o.name == output))
    }

    pub(super) fn validate_graph(
        &self,
        config: &Configuration,
        desc: &TypeDescription,
    ) -> Result<Vec<(String, String)>, String> {
        Self::validate_inputs(config, desc)?;
        let mut configs: BTreeMap<_, _> = self
            .nodes
            .iter()
            .map(|(k, n)| (k.as_str(), (&n.config, &n.description)))
            .collect();
        configs.insert(&config.name, (config, desc));
        let order = self.graph_order(&configs).map_err(|(_, reason)| reason)?;

        // Reserve the existing v1 describe frame before accepting a larger graph.
        // 256 covers scalar bounds, poll period, field names and punctuation.
        let cost = |key: &str, unit: &str| {
            256 + json!(key).to_string().len() + json!(unit).to_string().len()
        };
        let mut bytes = self
            .physical
            .values()
            .map(|p| cost(&p.id, &p.unit))
            .sum::<usize>();
        let mut units = self
            .physical
            .iter()
            .map(|(k, p)| (k.clone(), p.unit.clone()))
            .collect::<HashMap<_, _>>();
        // Carry the units resolved during budget validation into the committed graph.
        let mut plan = Vec::with_capacity(order.len());
        for name in order {
            let (c, d) = configs[name];
            let inherited = c
                .inputs
                .values()
                .next()
                .and_then(|k| units.get(k))
                .cloned()
                .unwrap_or_default();
            for o in &d.outputs {
                let key = signal(name, &o.name);
                let unit = o.unit.clone().unwrap_or_else(|| inherited.clone());
                bytes = bytes.saturating_add(cost(&key, &unit));
                units.insert(key, unit);
            }
            plan.push((name.to_owned(), inherited));
        }
        if bytes > self.catalog_budget {
            return Err("catalog_capacity".into());
        }
        Ok(plan)
    }

    pub(super) fn install_graph(&mut self, plan: Vec<(String, String)>) {
        self.order = plan
            .into_iter()
            .map(|(name, unit)| {
                self.nodes.get_mut(&name).unwrap().inherited_unit = unit;
                name
            })
            .collect();
    }

    /// Order a set of declarations before adding them through the usual create path.
    pub(crate) fn startup_order<'a>(
        &self,
        configurations: impl IntoIterator<Item = &'a Configuration>,
    ) -> Result<Vec<usize>, (&'a str, &'static str)> {
        let mut components = BTreeMap::new();
        for (index, config) in configurations.into_iter().enumerate() {
            let name = config.name.as_str();
            if !identifier(name)
                || components.len() >= MAX_COMPONENTS
                || components.contains_key(name)
            {
                return Err((name, "invalid_instance"));
            }
            let description = self
                .registry
                .factory(&config.type_id)
                .map_err(|reason| (name, reason))?
                .describe();
            Self::validate_inputs(config, &description).map_err(|reason| (name, reason))?;
            components.insert(name, (index, config, description));
        }
        let graph = components
            .iter()
            .map(|(&name, (_, config, description))| (name, (*config, description)))
            .collect();
        Ok(self
            .graph_order(&graph)?
            .into_iter()
            .map(|name| components[name].0)
            .collect())
    }

    fn validate_inputs(config: &Configuration, desc: &TypeDescription) -> Result<(), &'static str> {
        if config.max_age_ms == 0
            || config.max_age_ms > 86_400_000
            || config.inputs.keys().cloned().collect::<BTreeSet<_>>()
                != desc.inputs.iter().cloned().collect()
        {
            return Err("invalid_inputs");
        }
        Ok(())
    }

    fn graph_order<'a>(
        &self,
        configs: &BTreeMap<&'a str, (&Configuration, &TypeDescription)>,
    ) -> Result<Vec<&'a str>, (&'a str, &'static str)> {
        let mut owners = HashMap::new();
        for (&name, (_, d)) in configs {
            for o in &d.outputs {
                let key = signal(name, &o.name);
                if self.physical.contains_key(&key) || owners.insert(key, name).is_some() {
                    return Err((name, "signal_collision"));
                }
            }
        }
        let mut dependencies = BTreeMap::new();
        for (&name, (c, _)) in configs {
            let mut edges = BTreeSet::new();
            for key in c.inputs.values() {
                if let Some(&owner) = owners.get(key) {
                    edges.insert(owner);
                } else if !self.physical.get(key).is_some_and(|p| {
                    !matches!(p.spec, Spec::Boolean) && p.default_poll_period.is_some()
                }) {
                    return Err((name, "unknown_or_non_numeric_input"));
                }
            }
            dependencies.insert(name, edges);
        }
        let mut order = Vec::new();
        while !dependencies.is_empty() {
            // BTreeMap iteration chooses the lexicographically first ready node.
            let Some(name) = dependencies
                .iter()
                .find(|(_, e)| e.is_empty())
                .map(|(&name, _)| name)
            else {
                // Follow the remaining edges to report a member of the cycle,
                // even when the first blocked node only depends on it.
                let mut visited = BTreeSet::new();
                let mut name = *dependencies.first_key_value().unwrap().0;
                while visited.insert(name) {
                    name = *dependencies[name].first().unwrap();
                }
                return Err((name, "dependency_cycle"));
            };
            dependencies.remove(name);
            for edges in dependencies.values_mut() {
                edges.remove(name);
            }
            order.push(name);
        }
        Ok(order)
    }
    pub fn dependents(&self, name: &str) -> Vec<String> {
        let mut names = vec![name.to_owned()];
        let Some(source) = self.nodes.get(name) else {
            return names;
        };
        let mut reachable: HashSet<_> = source
            .description
            .outputs
            .iter()
            .map(|output| signal(name, &output.name))
            .collect();

        // All consumers of a reachable signal follow its producer in this order.
        for id in self
            .order
            .iter()
            .skip_while(|id| id.as_str() != name)
            .skip(1)
        {
            let node = &self.nodes[id];
            if node
                .config
                .inputs
                .values()
                .any(|key| reachable.contains(key))
            {
                names.push(id.clone());
                reachable.extend(
                    node.description
                        .outputs
                        .iter()
                        .map(|output| signal(id, &output.name)),
                );
            }
        }
        // Invalidation and audit use lexical order, independent of graph order.
        names.sort_unstable();
        names
    }

    pub fn lineage(
        &self,
        physical: &HashMap<String, BTreeSet<String>>,
    ) -> HashMap<String, BTreeSet<String>> {
        let mut domains = physical.clone();
        for name in &self.order {
            let n = &self.nodes[name];
            let ports = n
                .config
                .inputs
                .values()
                .filter_map(|k| domains.get(k))
                .flatten()
                .cloned()
                .collect::<BTreeSet<_>>();
            for o in &n.description.outputs {
                domains.insert(signal(name, &o.name), ports.clone());
            }
        }
        domains
    }

    pub fn catalog(&self) -> HashMap<String, ParameterDescription> {
        let mut catalog = self.physical.clone();
        for name in &self.order {
            let node = &self.nodes[name];
            let unit = node
                .config
                .inputs
                .values()
                .next()
                .and_then(|k| catalog.get(k))
                .map(|p| p.unit.clone())
                .unwrap_or_default();
            for output in &node.description.outputs {
                let id = signal(name, &output.name);
                catalog.insert(
                    id.clone(),
                    ParameterDescription {
                        id,
                        spec: Spec::Float {
                            min: -f64::MAX,
                            max: f64::MAX,
                        },
                        unit: output.unit.clone().unwrap_or_else(|| unit.clone()),
                        writable: false,
                        default_poll_period: Some(Duration::from_millis(1)),
                    },
                );
            }
        }
        catalog
    }
}

#[cfg(test)]
mod tests;
