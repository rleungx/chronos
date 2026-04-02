use std::cmp::Ordering as CmpOrdering;

use crate::{decode_tso, TsoError, SEQUENCE_CAPACITY};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cursor {
    pub(crate) physical_ms: u64,
    pub(crate) sequence: u32,
}

pub(crate) fn next_cursor_after(
    tso_floor: u64,
    target_generator_id: u32,
) -> Result<Cursor, TsoError> {
    let floor = decode_tso(tso_floor);
    match target_generator_id.cmp(&floor.generator_id) {
        CmpOrdering::Greater => Ok(Cursor {
            physical_ms: floor.physical_ms,
            sequence: 0,
        }),
        CmpOrdering::Equal => {
            if floor.sequence + 1 < SEQUENCE_CAPACITY {
                Ok(Cursor {
                    physical_ms: floor.physical_ms,
                    sequence: floor.sequence + 1,
                })
            } else {
                let physical_ms = floor
                    .physical_ms
                    .checked_add(1)
                    .ok_or(TsoError::TsoOverflow)?;
                Ok(Cursor {
                    physical_ms,
                    sequence: 0,
                })
            }
        }
        CmpOrdering::Less => {
            let physical_ms = floor
                .physical_ms
                .checked_add(1)
                .ok_or(TsoError::TsoOverflow)?;
            Ok(Cursor {
                physical_ms,
                sequence: 0,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::next_cursor_after;
    use crate::{encode_tso, MAX_PHYSICAL_MS, SEQUENCE_CAPACITY};

    #[test]
    fn next_cursor_advances_within_same_generator_slot() {
        let floor = encode_tso(10, 7, 2).unwrap();
        let cursor = next_cursor_after(floor, 7).unwrap();
        assert_eq!(cursor.physical_ms, 10);
        assert_eq!(cursor.sequence, 3);
    }

    #[test]
    fn next_cursor_rolls_to_next_physical_ms_on_sequence_exhaustion() {
        let floor = encode_tso(10, 7, SEQUENCE_CAPACITY - 1).unwrap();
        let cursor = next_cursor_after(floor, 7).unwrap();
        assert_eq!(cursor.physical_ms, 11);
        assert_eq!(cursor.sequence, 0);
    }

    #[test]
    fn next_cursor_can_advance_past_max_physical_boundary() {
        let floor = encode_tso(MAX_PHYSICAL_MS, 7, SEQUENCE_CAPACITY - 1).unwrap();
        let cursor = next_cursor_after(floor, 7).unwrap();
        assert_eq!(cursor.physical_ms, MAX_PHYSICAL_MS + 1);
        assert_eq!(cursor.sequence, 0);
    }
}
