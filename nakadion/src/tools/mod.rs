pub mod subscription_stats {

    pub trait Instruments {
        /// Tracks the number of unconsumed events.
        fn unconsumed_events(&self, n_unconsumed: usize);
    }
}
