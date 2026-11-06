use std::time::{Duration, Instant};

pub(super) struct Task {
    pub device: usize,
    pub parameter: usize,
    pub period: Duration,
    pub next_at: Instant,
}

pub(super) struct Scheduler {
    pub tasks: Vec<Task>,
    device_count: usize,
    group_deadline: Option<Instant>,
    next_group_start: usize,
    cursor: usize,
}

impl Scheduler {
    pub fn new(tasks: Vec<Task>, device_count: usize) -> Self {
        Self {
            tasks,
            device_count,
            group_deadline: None,
            next_group_start: 0,
            cursor: 0,
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.tasks.iter().map(|task| task.next_at).min()
    }

    pub fn next_due(&mut self, now: Instant) -> Option<usize> {
        let deadline = self.next_deadline()?;
        if deadline > now {
            return None;
        }
        if self.group_deadline != Some(deadline) {
            // Rotate the FIRST device of each equal-deadline group as well as
            // devices within it. A simple cursor alone repeats the same first
            // device every round when all periods are equal.
            self.group_deadline = Some(deadline);
            self.cursor = self.next_group_start;
            self.next_group_start = (self.next_group_start + 1) % self.device_count;
        }
        let index = self
            .tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| task.next_at == deadline)
            .min_by_key(|(_, task)| {
                (
                    (task.device + self.device_count - self.cursor) % self.device_count,
                    task.parameter,
                )
            })
            .map(|(index, _)| index)?;
        self.cursor = (self.tasks[index].device + 1) % self.device_count;
        Some(index)
    }

    pub fn completed(&mut self, index: usize, now: Instant) -> Option<()> {
        let task = &mut self.tasks[index];
        // Preserve phase, skip missed ticks in O(1), never enqueue catch-up work.
        let remainder =
            now.saturating_duration_since(task.next_at).as_nanos() % task.period.as_nanos();
        let remainder = Duration::new(
            (remainder / 1_000_000_000) as u64,
            (remainder % 1_000_000_000) as u32,
        );
        task.next_at = now.checked_add(task.period - remainder)?;
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_deadline_groups_rotate_the_first_device() {
        let start = Instant::now();
        let period = Duration::from_millis(10);
        let tasks = (0..3)
            .map(|device| Task {
                device,
                parameter: 0,
                period,
                next_at: start,
            })
            .collect();
        let mut scheduler = Scheduler::new(tasks, 3);
        for round in 0..6 {
            let now = start + period * round;
            let mut order = Vec::new();
            while let Some(index) = scheduler.next_due(now) {
                order.push(scheduler.tasks[index].device);
                scheduler.completed(index, now).unwrap();
            }
            assert_eq!(
                order,
                (0..3)
                    .map(|offset| (round as usize + offset) % 3)
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn periods_are_independent_and_missed_ticks_do_not_accumulate() {
        let start = Instant::now();
        let tasks = [10, 30]
            .into_iter()
            .enumerate()
            .map(|(device, ms)| Task {
                device,
                parameter: 0,
                period: Duration::from_millis(ms),
                next_at: start,
            })
            .collect();
        let mut scheduler = Scheduler::new(tasks, 2);
        let mut counts = [0; 2];
        for ms in 0..=90 {
            let now = start + Duration::from_millis(ms);
            while let Some(index) = scheduler.next_due(now) {
                counts[index] += 1;
                scheduler.completed(index, now).unwrap();
            }
        }
        assert_eq!(counts, [10, 4]);
        let late = start + Duration::from_secs(1_000_000);
        for _ in 0..2 {
            let index = scheduler.next_due(late).unwrap();
            scheduler.completed(index, late).unwrap();
        }
        assert!(scheduler.next_due(late).is_none());
        assert_eq!(scheduler.tasks[0].next_at, late + Duration::from_millis(10));
        assert_eq!(scheduler.tasks[1].next_at, late + Duration::from_millis(20));
    }
}
