use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::{DecodedTso, TsoError};

pub const TIMESTAMP_LAYOUT_FORMAT_VERSION: u32 = 1;
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

/// Immutable description of the cluster-wide timestamp encoding.
///
/// Chronos timestamps always encode a millisecond-relative physical component followed by a
/// generator id and a per-generator sequence. The three bit widths must consume exactly 64 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TimestampLayout {
    format_version: u32,
    epoch_unix_ms: u64,
    physical_bits: u8,
    generator_bits: u8,
    sequence_bits: u8,
}

#[derive(Deserialize)]
struct SerializedTimestampLayout {
    format_version: u32,
    epoch_unix_ms: u64,
    physical_bits: u8,
    generator_bits: u8,
    sequence_bits: u8,
}

impl<'de> Deserialize<'de> for TimestampLayout {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized = SerializedTimestampLayout::deserialize(deserializer)?;
        let layout = Self {
            format_version: serialized.format_version,
            epoch_unix_ms: serialized.epoch_unix_ms,
            physical_bits: serialized.physical_bits,
            generator_bits: serialized.generator_bits,
            sequence_bits: serialized.sequence_bits,
        };
        layout.validate().map_err(serde::de::Error::custom)?;
        Ok(layout)
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TimestampLayoutValidationError {
    #[error("unsupported timestamp layout format version {actual}; expected {expected}")]
    UnsupportedFormatVersion { actual: u32, expected: u32 },
    #[error("timestamp layout physical_bits must be greater than 0")]
    ZeroPhysicalBits,
    #[error("timestamp layout sequence_bits must be greater than 0")]
    ZeroSequenceBits,
    #[error("timestamp layout bit widths must total 64, got {total}")]
    InvalidTotalBits { total: u16 },
    #[error("timestamp layout generator_bits must not exceed 13, got {actual}")]
    TooManyGeneratorBits { actual: u8 },
    #[error("timestamp layout sequence_bits must not exceed 31, got {actual}")]
    TooManySequenceBits { actual: u8 },
    #[error("timestamp layout generator_bits + sequence_bits must not exceed 32, got {actual}")]
    TooManyLogicalBits { actual: u8 },
    #[error("timestamp layout epoch and physical capacity exceed u64 Unix milliseconds")]
    UnixMillisecondHorizonOverflow,
}

pub const DEFAULT_TIMESTAMP_LAYOUT: TimestampLayout = TimestampLayout {
    format_version: TIMESTAMP_LAYOUT_FORMAT_VERSION,
    epoch_unix_ms: CUSTOM_EPOCH_UNIX_MS,
    physical_bits: PHYSICAL_BITS as u8,
    generator_bits: GENERATOR_ID_BITS as u8,
    sequence_bits: SEQUENCE_BITS as u8,
};

impl Default for TimestampLayout {
    fn default() -> Self {
        DEFAULT_TIMESTAMP_LAYOUT
    }
}

impl TimestampLayout {
    pub fn new(
        epoch_unix_ms: u64,
        physical_bits: u8,
        generator_bits: u8,
        sequence_bits: u8,
    ) -> Result<Self, TimestampLayoutValidationError> {
        let layout = Self {
            format_version: TIMESTAMP_LAYOUT_FORMAT_VERSION,
            epoch_unix_ms,
            physical_bits,
            generator_bits,
            sequence_bits,
        };
        layout.validate()?;
        Ok(layout)
    }

    pub fn validate(self) -> Result<(), TimestampLayoutValidationError> {
        if self.format_version != TIMESTAMP_LAYOUT_FORMAT_VERSION {
            return Err(TimestampLayoutValidationError::UnsupportedFormatVersion {
                actual: self.format_version,
                expected: TIMESTAMP_LAYOUT_FORMAT_VERSION,
            });
        }
        if self.physical_bits == 0 {
            return Err(TimestampLayoutValidationError::ZeroPhysicalBits);
        }
        if self.sequence_bits == 0 {
            return Err(TimestampLayoutValidationError::ZeroSequenceBits);
        }
        if self.generator_bits > 13 {
            return Err(TimestampLayoutValidationError::TooManyGeneratorBits {
                actual: self.generator_bits,
            });
        }
        if self.sequence_bits > 31 {
            return Err(TimestampLayoutValidationError::TooManySequenceBits {
                actual: self.sequence_bits,
            });
        }
        let logical_bits = self.generator_bits + self.sequence_bits;
        if logical_bits > 32 {
            return Err(TimestampLayoutValidationError::TooManyLogicalBits {
                actual: logical_bits,
            });
        }
        let total = u16::from(self.physical_bits)
            + u16::from(self.generator_bits)
            + u16::from(self.sequence_bits);
        if total != 64 {
            return Err(TimestampLayoutValidationError::InvalidTotalBits { total });
        }
        if self
            .epoch_unix_ms
            .checked_add(self.max_physical_ms())
            .is_none()
        {
            return Err(TimestampLayoutValidationError::UnixMillisecondHorizonOverflow);
        }
        Ok(())
    }

    pub const fn format_version(self) -> u32 {
        self.format_version
    }

    pub const fn epoch_unix_ms(self) -> u64 {
        self.epoch_unix_ms
    }

    pub const fn physical_bits(self) -> u8 {
        self.physical_bits
    }

    pub const fn generator_bits(self) -> u8 {
        self.generator_bits
    }

    pub const fn sequence_bits(self) -> u8 {
        self.sequence_bits
    }

    pub const fn logical_bits(self) -> u8 {
        self.generator_bits + self.sequence_bits
    }

    pub const fn max_generators(self) -> u32 {
        1u32 << self.generator_bits
    }

    pub const fn sequence_capacity(self) -> u32 {
        1u32 << self.sequence_bits
    }

    pub const fn max_physical_ms(self) -> u64 {
        (1u64 << self.physical_bits) - 1
    }

    pub fn max_unix_ms(self) -> u64 {
        self.epoch_unix_ms + self.max_physical_ms()
    }

    pub fn capacity_envelope(self) -> TsoCapacityEnvelope {
        TsoCapacityEnvelope {
            custom_epoch_unix_ms: self.epoch_unix_ms,
            max_supported_unix_ms: self.max_unix_ms(),
            lifetime_ms: self.max_physical_ms() + 1,
            max_physical_ms: self.max_physical_ms(),
            max_generators: self.max_generators(),
            per_generator_per_ms_capacity: self.sequence_capacity(),
            cluster_per_ms_capacity_ceiling: u64::from(self.max_generators())
                * u64::from(self.sequence_capacity()),
        }
    }

    pub fn checked_physical_ms_from_unix_ms(self, unix_ms: u64) -> Result<u64, TsoUnixMsBoundary> {
        if unix_ms < self.epoch_unix_ms {
            return Err(TsoUnixMsBoundary::BeforeCustomEpoch);
        }
        if unix_ms > self.max_unix_ms() {
            return Err(TsoUnixMsBoundary::BeyondCapacityEnvelope);
        }
        Ok(unix_ms - self.epoch_unix_ms)
    }

    pub fn encode(
        self,
        physical_ms: u64,
        generator_id: u32,
        sequence: u32,
    ) -> Result<u64, TsoError> {
        self.validate()
            .map_err(|error| TsoError::Internal(error.to_string()))?;
        if physical_ms > self.max_physical_ms() {
            return Err(TsoError::TsoOverflow);
        }
        if generator_id >= self.max_generators() {
            return Err(TsoError::GeneratorIdOutOfRange { generator_id });
        }
        if sequence >= self.sequence_capacity() {
            return Err(TsoError::TsoOverflow);
        }
        Ok((physical_ms << self.logical_bits())
            | (u64::from(generator_id) << self.sequence_bits)
            | u64::from(sequence))
    }

    pub fn decode(self, tso: u64) -> DecodedTso {
        let generator_mask = u64::from(self.max_generators() - 1);
        let sequence_mask = u64::from(self.sequence_capacity() - 1);
        let logical_mask = (1u64 << self.logical_bits()) - 1;
        DecodedTso {
            physical_ms: tso >> self.logical_bits(),
            logical: (tso & logical_mask) as u32,
            generator_id: ((tso >> self.sequence_bits) & generator_mask) as u32,
            sequence: (tso & sequence_mask) as u32,
        }
    }
}

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
        self.max_supported_unix_ms.saturating_add(1)
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

// Compatibility helpers retain the original Chronos layout for existing callers.
pub fn checked_physical_ms_from_unix_ms(unix_ms: u64) -> Result<u64, TsoUnixMsBoundary> {
    DEFAULT_TIMESTAMP_LAYOUT.checked_physical_ms_from_unix_ms(unix_ms)
}

pub fn encode_tso(physical_ms: u64, generator_id: u32, sequence: u32) -> Result<u64, TsoError> {
    DEFAULT_TIMESTAMP_LAYOUT.encode(physical_ms, generator_id, sequence)
}

pub fn decode_tso(tso: u64) -> DecodedTso {
    DEFAULT_TIMESTAMP_LAYOUT.decode(tso)
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
    }

    #[test]
    fn custom_layout_round_trips_components() {
        let layout = TimestampLayout::new(0, 46, 7, 11).unwrap();
        let encoded = layout.encode(123_456, 99, 1_337).unwrap();
        let decoded = layout.decode(encoded);

        assert_eq!(decoded.physical_ms, 123_456);
        assert_eq!(decoded.generator_id, 99);
        assert_eq!(decoded.sequence, 1_337);
        assert_eq!(layout.max_generators(), 128);
        assert_eq!(layout.sequence_capacity(), 2_048);
    }

    #[test]
    fn layout_rejects_ambiguous_or_unsupported_widths() {
        assert_eq!(
            TimestampLayout::new(0, 40, 12, 11),
            Err(TimestampLayoutValidationError::InvalidTotalBits { total: 63 })
        );
        assert_eq!(
            TimestampLayout::new(0, 39, 14, 11),
            Err(TimestampLayoutValidationError::TooManyGeneratorBits { actual: 14 })
        );
        assert_eq!(
            TimestampLayout::new(0, 32, 1, 31)
                .unwrap()
                .sequence_capacity(),
            1u32 << 31
        );
        assert_eq!(
            TimestampLayout::new(0, 31, 2, 31),
            Err(TimestampLayoutValidationError::TooManyLogicalBits { actual: 33 })
        );
    }

    #[test]
    fn checked_physical_ms_reports_layout_boundaries() {
        let layout = TimestampLayout::new(1_000, 52, 1, 11).unwrap();
        assert_eq!(
            layout.checked_physical_ms_from_unix_ms(999),
            Err(TsoUnixMsBoundary::BeforeCustomEpoch)
        );
        assert_eq!(layout.checked_physical_ms_from_unix_ms(1_000), Ok(0));
        assert_eq!(
            layout.checked_physical_ms_from_unix_ms(layout.max_unix_ms() + 1),
            Err(TsoUnixMsBoundary::BeyondCapacityEnvelope)
        );
    }
}
