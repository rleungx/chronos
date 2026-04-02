use crate::{DecodedTso, TsoError};

pub const CUSTOM_EPOCH_UNIX_MS: u64 = 1_767_225_600_000;
pub const PHYSICAL_BITS: u32 = 40;
pub const GENERATOR_ID_BITS: u32 = 13;
pub const SEQUENCE_BITS: u32 = 11;
pub const LOGICAL_BITS: u32 = GENERATOR_ID_BITS + SEQUENCE_BITS;
pub const MAX_GENERATORS: u32 = 1 << GENERATOR_ID_BITS;
pub const SEQUENCE_CAPACITY: u32 = 1 << SEQUENCE_BITS;
pub const MAX_PHYSICAL_MS: u64 = (1u64 << PHYSICAL_BITS) - 1;
pub const GENERATOR_ID_MASK: u64 = (1u64 << GENERATOR_ID_BITS) - 1;
pub const SEQUENCE_MASK: u64 = (1u64 << SEQUENCE_BITS) - 1;
pub const MAX_UNIX_MS: u64 = CUSTOM_EPOCH_UNIX_MS + MAX_PHYSICAL_MS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TsoCapacityEnvelope {
    pub custom_epoch_unix_ms: u64,
    pub max_supported_unix_ms: u64,
    pub lifetime_ms: u64,
    pub max_physical_ms: u64,
    pub max_generators: u32,
    pub per_generator_per_ms_capacity: u32,
    pub cluster_per_ms_capacity_ceiling: u64,
}

impl TsoCapacityEnvelope {
    pub const fn first_unencodable_unix_ms(self) -> u64 {
        self.max_supported_unix_ms + 1
    }

    pub const fn first_unencodable_physical_ms(self) -> u64 {
        self.max_physical_ms + 1
    }

    pub const fn first_generator_id_out_of_range(self) -> u32 {
        self.max_generators
    }

    pub const fn first_sequence_out_of_range(self) -> u32 {
        self.per_generator_per_ms_capacity
    }
}

pub const TSO_CAPACITY_ENVELOPE: TsoCapacityEnvelope = TsoCapacityEnvelope {
    custom_epoch_unix_ms: CUSTOM_EPOCH_UNIX_MS,
    max_supported_unix_ms: MAX_UNIX_MS,
    lifetime_ms: MAX_PHYSICAL_MS + 1,
    max_physical_ms: MAX_PHYSICAL_MS,
    max_generators: MAX_GENERATORS,
    per_generator_per_ms_capacity: SEQUENCE_CAPACITY,
    cluster_per_ms_capacity_ceiling: MAX_GENERATORS as u64 * SEQUENCE_CAPACITY as u64,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsoUnixMsBoundary {
    BeforeCustomEpoch,
    BeyondCapacityEnvelope,
}

pub fn checked_physical_ms_from_unix_ms(unix_ms: u64) -> Result<u64, TsoUnixMsBoundary> {
    if unix_ms < CUSTOM_EPOCH_UNIX_MS {
        return Err(TsoUnixMsBoundary::BeforeCustomEpoch);
    }
    if unix_ms > MAX_UNIX_MS {
        return Err(TsoUnixMsBoundary::BeyondCapacityEnvelope);
    }
    Ok(unix_ms - CUSTOM_EPOCH_UNIX_MS)
}

pub fn encode_tso(physical_ms: u64, generator_id: u32, sequence: u32) -> Result<u64, TsoError> {
    if physical_ms > MAX_PHYSICAL_MS {
        return Err(TsoError::TsoOverflow);
    }
    if generator_id >= MAX_GENERATORS {
        return Err(TsoError::GeneratorIdOutOfRange { generator_id });
    }
    if sequence >= SEQUENCE_CAPACITY {
        return Err(TsoError::TsoOverflow);
    }
    Ok((physical_ms << LOGICAL_BITS) | ((generator_id as u64) << SEQUENCE_BITS) | sequence as u64)
}

pub fn decode_tso(tso: u64) -> DecodedTso {
    let physical_ms = (tso >> LOGICAL_BITS) & MAX_PHYSICAL_MS;
    let generator_id = ((tso >> SEQUENCE_BITS) & GENERATOR_ID_MASK) as u32;
    let sequence = (tso & SEQUENCE_MASK) as u32;
    let logical = (tso & ((1u64 << LOGICAL_BITS) - 1)) as u32;
    DecodedTso {
        physical_ms,
        logical,
        generator_id,
        sequence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_envelope_freezes_current_codec_limits() {
        let envelope = TSO_CAPACITY_ENVELOPE;

        assert_eq!(envelope.custom_epoch_unix_ms, CUSTOM_EPOCH_UNIX_MS);
        assert_eq!(envelope.max_supported_unix_ms, MAX_UNIX_MS);
        assert_eq!(envelope.lifetime_ms, MAX_PHYSICAL_MS + 1);
        assert_eq!(envelope.max_physical_ms, MAX_PHYSICAL_MS);
        assert_eq!(envelope.max_generators, MAX_GENERATORS);
        assert_eq!(envelope.per_generator_per_ms_capacity, SEQUENCE_CAPACITY);
        assert_eq!(
            envelope.cluster_per_ms_capacity_ceiling,
            MAX_GENERATORS as u64 * SEQUENCE_CAPACITY as u64
        );
        assert_eq!(
            envelope.first_unencodable_physical_ms(),
            MAX_PHYSICAL_MS + 1
        );
        assert_eq!(envelope.first_unencodable_unix_ms(), MAX_UNIX_MS + 1);
        assert_eq!(envelope.first_generator_id_out_of_range(), MAX_GENERATORS);
        assert_eq!(envelope.first_sequence_out_of_range(), SEQUENCE_CAPACITY);
    }

    #[test]
    fn checked_physical_ms_from_unix_ms_reports_capacity_boundaries() {
        assert_eq!(
            checked_physical_ms_from_unix_ms(CUSTOM_EPOCH_UNIX_MS - 1),
            Err(TsoUnixMsBoundary::BeforeCustomEpoch)
        );
        assert_eq!(
            checked_physical_ms_from_unix_ms(CUSTOM_EPOCH_UNIX_MS),
            Ok(0)
        );
        assert_eq!(
            checked_physical_ms_from_unix_ms(MAX_UNIX_MS),
            Ok(MAX_PHYSICAL_MS)
        );
        assert_eq!(
            checked_physical_ms_from_unix_ms(MAX_UNIX_MS + 1),
            Err(TsoUnixMsBoundary::BeyondCapacityEnvelope)
        );
    }
}
