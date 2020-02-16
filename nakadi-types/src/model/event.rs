//! Types for dealing with events
use serde::{Deserialize, Serialize};
use uuid::Uuid;

new_type! {
    #[doc="Identifier for an event.\n\nSometimes also called EID."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct EventId(Uuid);
}
