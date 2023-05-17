use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct Count(AtomicU64);

impl Count {
    pub const fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn inc(&self) -> u64 {
        self.add(1)
    }

    pub fn add(&self, value: u64) -> u64 {
        self.0.fetch_add(value, Ordering::Relaxed)
    }
}

impl Default for Count {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_counter() {
        let counter = Count::new(42);
        assert_eq!(counter.get(), 42);
    }

    #[test]
    fn test_default_counter() {
        let counter = Count::default();
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_increment_counter() {
        let counter = Count::new(0);
        assert_eq!(counter.inc(), 0);
        assert_eq!(counter.get(), 1);
    }

    #[test]
    fn test_add_to_counter() {
        let counter = Count::new(10);
        assert_eq!(counter.add(5), 10);
        assert_eq!(counter.get(), 15);
    }
}
