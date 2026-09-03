//! Versioned codecs for individual compact blocks and random-access 1,000-block ranges.

use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::{Digest, index::RANGE_SIZE, parser::CompactTransaction};

const BLOCK_FORMAT_VERSION: u8 = 1;
const RANGE_FORMAT_VERSION: u8 = 1;
const MAX_RECORD_BYTES: usize = 2_000_000;

const RECORD_FIXED_BYTES: usize = 1 + 5 * size_of::<u32>() + 2 * size_of::<Digest>();

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// The final index record, which gets stored in LMDB.
pub struct CompactBlockRecord {
    pub height: u32,
    pub hash: Digest,
    pub previous_hash: Digest,
    pub time: u32,
    pub transactions: Vec<CompactTransaction>,
    pub end_tree_sizes: TreeSizes,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
/// Cumulative tree sizes per pool, by number of note commitments.
pub struct TreeSizes {
    pub sapling: u32,
    pub orchard: u32,
    pub ironwood: u32,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum CodecError {
    #[error("encoded value is truncated")]
    Truncated,
    #[error("unsupported {kind} format version {version}")]
    Version { kind: &'static str, version: u8 },
    #[error("encoded count, length, or offset is invalid")]
    Length,
    #[error("encoded value has trailing bytes")]
    TrailingBytes,
    #[error("encoded block record is invalid")]
    InvalidRecord,
    #[error("range must contain exactly {RANGE_SIZE} contiguous aligned blocks")]
    InvalidRange,
    #[error("range record index is outside 0..{RANGE_SIZE}")]
    RangeIndex,
}

impl CompactBlockRecord {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        record_options()
            .serialize(&(BLOCK_FORMAT_VERSION, self))
            .map_err(|_| CodecError::Length)
    }

    pub(crate) fn encoded_len_for_transactions(
        transactions: &[CompactTransaction],
    ) -> Result<usize, CodecError> {
        record_options()
            .serialized_size(transactions)
            .ok()
            .and_then(|len| usize::try_from(len).ok())
            .and_then(|len| RECORD_FIXED_BYTES.checked_add(len))
            .filter(|len| *len <= MAX_RECORD_BYTES)
            .ok_or(CodecError::Length)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(CodecError::Length);
        }
        let (version, record) = record_options()
            .deserialize::<(u8, Self)>(bytes)
            .map_err(|_| CodecError::InvalidRecord)?;
        if version != BLOCK_FORMAT_VERSION {
            return Err(CodecError::Version {
                kind: "block",
                version,
            });
        }
        Ok(record)
    }
}

fn record_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .with_limit(MAX_RECORD_BYTES as u64)
        .reject_trailing_bytes()
}

pub fn encode_range(records: &[CompactBlockRecord]) -> Result<Vec<u8>, CodecError> {
    if records.len() != RANGE_SIZE as usize
        || !records[0].height.is_multiple_of(RANGE_SIZE)
        || records.windows(2).any(|pair| {
            pair[0].height.checked_add(1) != Some(pair[1].height)
                || pair[1].previous_hash != pair[0].hash
        })
    {
        return Err(CodecError::InvalidRange);
    }

    // offset table size is (number of records + 1) * 4 bytes
    let offsets_len = records
        .len()
        .checked_add(1)
        .and_then(|len| len.checked_mul(size_of::<u32>()))
        .ok_or(CodecError::Length)?;
    // only preallocate the fixed envelope to avoid a second pass
    let capacity = 1usize
        .checked_add(2 * size_of::<u32>() + 2 * size_of::<Digest>())
        .and_then(|len| len.checked_add(offsets_len))
        .ok_or(CodecError::Length)?;
    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(RANGE_FORMAT_VERSION);
    put_u32(&mut bytes, records[0].height);
    put_u32(
        &mut bytes,
        records.last().expect("range is non-empty").height,
    );
    bytes.extend_from_slice(&records[0].previous_hash);
    bytes.extend_from_slice(&records.last().expect("range is non-empty").hash);
    // reserve offset table
    let offsets_start = bytes.len();
    bytes.resize(offsets_start + offsets_len, 0);
    let body_start = bytes.len();

    // serialize directly into final buffer
    for (index, record) in records.iter().enumerate() {
        let offset = bytes.len() - body_start;
        set_u32(&mut bytes, offsets_start + index * size_of::<u32>(), offset)?;
        let length_offset = bytes.len();
        put_u32(&mut bytes, 0);
        let record_start = bytes.len();
        record_options()
            .serialize_into(&mut bytes, &(BLOCK_FORMAT_VERSION, record))
            .map_err(|_| CodecError::Length)?;
        // after serializing, backpatch the length field
        let record_len = bytes.len() - record_start;
        set_u32(&mut bytes, length_offset, record_len)?;
    }
    // write the 1001st offset to mark end of the body
    let final_offset = bytes.len() - body_start;
    set_u32(
        &mut bytes,
        offsets_start + records.len() * size_of::<u32>(),
        final_offset,
    )?;
    Ok(bytes)
}

pub fn decode_range_record(bytes: &[u8], index: usize) -> Result<CompactBlockRecord, CodecError> {
    RangeDecoder::new(bytes)?.record(index)
}

/// Parses a range envelope once, then decodes selected records without allocating an offset table.
pub(crate) struct RangeDecoder<'a> {
    start: u32,
    first_previous_hash: Digest,
    terminal_hash: Digest,
    offsets: &'a [u8],
    body: &'a [u8],
}

impl<'a> RangeDecoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let version = reader.u8()?;
        if version != RANGE_FORMAT_VERSION {
            return Err(CodecError::Version {
                kind: "range",
                version,
            });
        }
        let start = reader.u32()?;
        let end = reader.u32()?;
        let first_previous_hash = reader.array::<32>()?;
        let terminal_hash = reader.array::<32>()?;
        if !start.is_multiple_of(RANGE_SIZE) || start.checked_add(RANGE_SIZE - 1) != Some(end) {
            return Err(CodecError::InvalidRange);
        }

        let offsets = reader.take((RANGE_SIZE as usize + 1) * size_of::<u32>())?;
        let body = reader.take(reader.remaining())?;
        let decoder = Self {
            start,
            first_previous_hash,
            terminal_hash,
            offsets,
            body,
        };
        if decoder.offset(0) != 0
            || decoder.offset(RANGE_SIZE as usize) != body.len()
            || (1..=RANGE_SIZE as usize)
                .any(|index| decoder.offset(index - 1) > decoder.offset(index))
        {
            return Err(CodecError::Length);
        }
        Ok(decoder)
    }

    pub(crate) fn record(&self, index: usize) -> Result<CompactBlockRecord, CodecError> {
        if index >= RANGE_SIZE as usize {
            return Err(CodecError::RangeIndex);
        }
        let envelope = self
            .body
            .get(self.offset(index)..self.offset(index + 1))
            .ok_or(CodecError::Length)?;
        let mut envelope = Reader::new(envelope);
        let record_len = envelope.len()?;
        let record = CompactBlockRecord::decode(envelope.take(record_len)?)?;
        envelope.finish()?;
        if self.start.checked_add(index as u32) != Some(record.height)
            || (index == 0 && record.previous_hash != self.first_previous_hash)
            || (index + 1 == RANGE_SIZE as usize && record.hash != self.terminal_hash)
        {
            return Err(CodecError::InvalidRange);
        }
        Ok(record)
    }

    fn offset(&self, index: usize) -> usize {
        let start = index * size_of::<u32>();
        u32::from_be_bytes(
            self.offsets[start..start + size_of::<u32>()]
                .try_into()
                .expect("offset table width was checked"),
        ) as usize
    }
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn set_u32(bytes: &mut [u8], offset: usize, value: usize) -> Result<(), CodecError> {
    bytes[offset..offset + size_of::<u32>()].copy_from_slice(
        &u32::try_from(value)
            .map_err(|_| CodecError::Length)?
            .to_be_bytes(),
    );
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self.offset.checked_add(len).ok_or(CodecError::Length)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        Ok(self.take(N)?.try_into().expect("length was checked"))
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn len(&mut self) -> Result<usize, CodecError> {
        Ok(self.u32()? as usize)
    }

    fn finish(self) -> Result<(), CodecError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{CompactSaplingOutput, CompactShieldedAction};

    #[test]
    fn block_and_range_round_trip() {
        let transaction = CompactTransaction {
            index: 2,
            txid: [3; 32],
            sapling_spends: vec![[4; 32]],
            sapling_outputs: vec![CompactSaplingOutput {
                cmu: [5; 32],
                ephemeral_key: [6; 32],
                ciphertext: [7; 52],
            }],
            orchard_actions: vec![action(8)],
            ironwood_actions: vec![action(9)],
        };
        let first = CompactBlockRecord {
            height: 0,
            hash: [1; 32],
            previous_hash: [0; 32],
            time: 10,
            transactions: vec![transaction],
            end_tree_sizes: TreeSizes {
                sapling: 1,
                orchard: 1,
                ironwood: 1,
            },
        };
        let encoded_first = first.encode().unwrap();
        assert_eq!(
            encoded_first.len(),
            CompactBlockRecord::encoded_len_for_transactions(&first.transactions).unwrap()
        );
        assert_eq!(CompactBlockRecord::decode(&encoded_first).unwrap(), first);
        assert_eq!(
            CompactBlockRecord::decode(&[encoded_first, vec![0]].concat()),
            Err(CodecError::InvalidRecord)
        );
        assert_eq!(
            CompactBlockRecord::decode(&vec![0; MAX_RECORD_BYTES + 1]),
            Err(CodecError::Length)
        );
        assert_eq!(
            CompactBlockRecord::decode(&[0xff; 81]),
            Err(CodecError::InvalidRecord)
        );

        let mut records = vec![first];
        for height in 1..RANGE_SIZE {
            let previous_hash = records.last().unwrap().hash;
            records.push(CompactBlockRecord {
                height,
                hash: hash(height),
                previous_hash,
                time: height,
                transactions: Vec::new(),
                end_tree_sizes: TreeSizes::default(),
            });
        }
        let encoded = encode_range(&records).unwrap();
        assert_eq!(decode_range_record(&encoded, 0).unwrap(), records[0]);
        assert_eq!(decode_range_record(&encoded, 537).unwrap(), records[537]);
        assert_eq!(decode_range_record(&encoded, 999).unwrap(), records[999]);
    }

    fn action(byte: u8) -> CompactShieldedAction {
        CompactShieldedAction {
            nullifier: [byte; 32],
            commitment: [byte; 32],
            ephemeral_key: [byte; 32],
            ciphertext: [byte; 52],
        }
    }

    fn hash(height: u32) -> Digest {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }
}
