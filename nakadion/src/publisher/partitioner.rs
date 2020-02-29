//! Tool for manually assigning partitions
use std::hash::{BuildHasher, Hash, Hasher};

use crc::crc64::{Digest, ISO};

use crate::api::{MonitoringApi, NakadiApiError};
use crate::nakadi_types::{
    model::{
        event::publishable::{BusinessEventPub, DataChangeEventPub},
        event_type::EventTypeName,
        partition::PartitionId,
    },
    RandomFlowId,
};

/// The default hasher used is [crc64::ISO](https://docs.rs/crc/1.9.0/crc/crc64/constant.ISO.html)
#[derive(Clone)]
pub struct DefaultBuildHasher;

impl BuildHasher for DefaultBuildHasher {
    type Hasher = Digest;

    fn build_hasher(&self) -> Self::Hasher {
        Digest::new(ISO)
    }
}

/// Determines partitions based on hashes
#[derive(Clone)]
pub struct Partitioner<B: BuildHasher + Clone = DefaultBuildHasher> {
    partitions: Vec<PartitionId>,
    build_hasher: B,
}

impl Partitioner<DefaultBuildHasher> {
    /// Create a new instance with the given partitions.
    ///
    /// The order of the given partitions will not be changed.
    ///
    /// ## Panics
    ///
    /// If partitions is empty
    pub fn new(partitions: Vec<PartitionId>) -> Self {
        Self::new_with_hasher(partitions, DefaultBuildHasher)
    }

    /// Create a new instance with the given partitions.
    ///
    /// The partitions will be sorted by first trying to convert
    /// the partitions to numbers and sorting by these. Otherwise
    /// they will be sorted ba their contained string.
    ///
    /// ## Panics
    ///
    /// If partitions is empty
    pub fn new_sorted(partitions: Vec<PartitionId>) -> Self {
        Self::new_sorted_with_hasher(partitions, DefaultBuildHasher)
    }

    /// Create a new instance for the partitions of the
    /// given event type.
    ///
    /// The partitions will be sorted by first trying to convert
    /// the partitions to numbers and sorting by these. Otherwise
    /// they will be sorted ba their contained string.
    ///
    /// Fails if the event type has no partitions.
    pub async fn from_event_type<C>(
        event_type: &EventTypeName,
        api_client: C,
    ) -> Result<Self, NakadiApiError>
    where
        C: MonitoringApi,
    {
        Self::from_event_type_with_hasher(event_type, api_client, DefaultBuildHasher).await
    }
}

impl<B> Partitioner<B>
where
    B: BuildHasher + Clone,
{
    /// Create a new instance with the given partitions and a provided
    /// hashing algorithm.
    ///
    /// The order of the given partitions will not be changed.
    ///
    /// ## Panics
    ///
    /// If partitions is empty
    pub fn new_with_hasher(partitions: Vec<PartitionId>, build_hasher: B) -> Self {
        assert!(!partitions.is_empty(), "partitions may not be empty");
        Self {
            partitions,
            build_hasher,
        }
    }

    /// Create a new instance with the given partitions and a provided
    /// hashing algorithm.
    ///
    /// The partitions will be sorted by first trying to convert
    /// the partitions to numbers and sorting by these. Otherwise
    /// they will be sorted ba their contained string.
    ///
    /// ## Panics
    ///
    /// If partitions is empty
    pub fn new_sorted_with_hasher(partitions: Vec<PartitionId>, build_hasher: B) -> Self {
        create_sorted_partitioner(partitions, build_hasher)
    }

    /// Create a new instance for the partitions of the
    /// given event type and a provided hashing algorithm.
    ///
    /// The partitions will be sorted by first trying to convert
    /// the partitions to numbers and sorting by these. Otherwise
    /// they will be sorted ba their contained string.
    ///
    /// Fails if the event type has no partitions.
    pub async fn from_event_type_with_hasher<C>(
        event_type: &EventTypeName,
        api_client: C,
        build_hasher: B,
    ) -> Result<Self, NakadiApiError>
    where
        C: MonitoringApi,
    {
        let partitions = api_client
            .get_event_type_partitions(event_type, RandomFlowId)
            .await?;

        if partitions.is_empty() {
            // Would be strange if Nakadi allowed this...
            return Err(NakadiApiError::other().with_context(
                "A partitioner can not be created from an event type without partitions",
            ));
        }

        let partitions: Vec<PartitionId> = partitions.into_iter().map(|p| p.partition).collect();

        Ok(create_sorted_partitioner(partitions, build_hasher))
    }

    /// Returns the partition that matches the given key
    fn partition_for_key<H>(&self, partition_key: &H) -> &PartitionId
    where
        H: Hash,
    {
        let mut hasher = self.build_hasher.build_hasher();
        partition_key.hash(&mut hasher);
        partition_for_hash(&self.partitions, hasher.finish())
    }

    /// Determines and assigns partitions
    pub fn assign<E: PartitionKeyExtractable + PartitionAssignable>(self, event: &mut E) {
        let key = event.partition_key();
        let partition = self.partition_for_key(&key);
        event.assign_partition(partition);
    }

    /// Returns the partitions as used by the `Partitioner`
    pub fn partitions(&self) -> &[PartitionId] {
        &self.partitions
    }
}

/// Can return a key for manual partitioning
pub trait PartitionKeyExtractable {
    type Key: Hash;

    /// Returns the key for partitioning
    fn partition_key(&self) -> Self::Key;
}

/// Can be assigned a partition
pub trait PartitionAssignable {
    /// Assign a partition.
    fn assign_partition(&mut self, partition: &PartitionId);
}

impl<D> PartitionAssignable for BusinessEventPub<D> {
    fn assign_partition(&mut self, partition: &PartitionId) {
        self.metadata.partition = Some(partition.clone());
    }
}

impl<D> PartitionAssignable for DataChangeEventPub<D> {
    fn assign_partition(&mut self, partition: &PartitionId) {
        self.metadata.partition = Some(partition.clone());
    }
}

fn create_sorted_partitioner<B: BuildHasher + Clone>(
    mut partitions: Vec<PartitionId>,
    build_hasher: B,
) -> Partitioner<B> {
    let ids_and_ints: Result<Vec<_>, _> = partitions
        .iter()
        .map(|id| id.as_str().parse::<u64>().map(|n| (id, n)))
        .collect();

    if let Ok(mut ids_and_ints) = ids_and_ints {
        ids_and_ints.sort_by_key(|x| x.1);
        Partitioner::new_with_hasher(
            ids_and_ints.into_iter().map(|(p, _)| p.clone()).collect(),
            build_hasher,
        )
    } else {
        partitions.sort();
        Partitioner::new_sorted_with_hasher(partitions, build_hasher)
    }
}

fn partition_for_hash<P>(partitions: &[P], hash: u64) -> &P {
    let idx = hash % (partitions.len() as u64);
    &partitions[idx as usize]
}

#[test]
fn partition_for_hash_works() {
    let partitions = &[1u32, 2, 3];

    assert_eq!(*partition_for_hash(partitions, 0), 1);
    assert_eq!(*partition_for_hash(partitions, 1), 2);
    assert_eq!(*partition_for_hash(partitions, 2), 3);
    assert_eq!(*partition_for_hash(partitions, 3), 1);
    assert_eq!(*partition_for_hash(partitions, 4), 2);
    assert_eq!(*partition_for_hash(partitions, 5), 3);
}

#[test]
fn the_default_hasher_stays_stable() {
    // WARNING! If this test fails behaviour of components
    // using the DefaultBuildHasher will change in a seriously
    // broken way! Do not just adjust the expected hashes
    // unless you really know that nobody is using the hasher.
    let sample = "TestForHasher";
    let mut hasher = DefaultBuildHasher.build_hasher();
    sample.hash(&mut hasher);
    let sample_hash = hasher.finish();
    assert_eq!(sample_hash, 4_641_087_037_712_464_189);

    let sample = 1_238_098;
    let mut hasher = DefaultBuildHasher.build_hasher();
    sample.hash(&mut hasher);
    let sample_hash = hasher.finish();
    assert_eq!(sample_hash, 8_063_443_670_793_191_424);
}
