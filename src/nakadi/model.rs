/// A partition id that comes with a `Cursor`
#[derive(Clone, Debug)]
pub struct PartitionId<'a>(pub &'a [u8]);

