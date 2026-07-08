use std::{collections::BTreeMap, fmt, iter};

use bit_vec::BitVec;
use enumflags2::BitFlags;
use bytes::BytesMut;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  dds::ddsdata::DDSData,
  messages::submessages::{
    elements::serialized_payload::SerializedPayload,
    submessages::{DATAFRAG_Flags, DataFrag},
  },
  structure::{
    cache_change::ChangeKind,
    sequence_number::{FragmentNumber, SequenceNumber},
    time::Timestamp,
  },
};

// This is for the assembly of a single object
struct AssemblyBuffer {
  buffer_bytes: BytesMut,
  fragment_count: usize,
  received_bitmap: BitVec,

  #[allow(dead_code)] // TODO: Purpose is to use this later for e.g.
  // garbage collection, in case some buffer is not completed within reasonable time.
  created_time: Timestamp,
  modified_time: Timestamp,
}

impl AssemblyBuffer {
  pub fn new(datafrag: &DataFrag) -> Option<Self> {
    let data_size: usize = datafrag.data_size.try_into().ok()?;
    let fragment_size: u16 = datafrag.fragment_size;
    debug!("new AssemblyBuffer data_size={data_size} frag_size={fragment_size}");

    if fragment_size == 0 || fragment_size as usize > data_size {
      error!("Cannot create AssemblyBuffer: fragment_size={fragment_size} data_size={data_size}");
      return None;
    }

    let mut buffer_bytes = BytesMut::with_capacity(data_size);
    buffer_bytes.resize(data_size, 0);

    let fragment_count = usize::from(datafrag.total_number_of_fragments());
    if fragment_count == 0 {
      error!("Cannot create AssemblyBuffer: zero fragment count");
      return None;
    }

    let now = Timestamp::now();

    Some(Self {
      buffer_bytes,
      fragment_count,
      received_bitmap: BitVec::from_elem(fragment_count, false),
      created_time: now,
      modified_time: now,
    })
  }

  /// Returns `false` if the fragment run is invalid or out of bounds.
  pub fn insert_frags(&mut self, datafrag: &DataFrag, frag_size: u16) -> bool {
    // TODO: Sanity checks? E.g. datafrag.fragment_size == frag_size
    // Or is this even guaranteed? Can Writer vary fragment size?
    // Answer: Writer must guarantee constant fragment size per SequenceNumber.
    // So yes, it is guaranteed. RTPS spec v2.5 Section 8.4.14.1.1 "How to select
    // the fragment size" even says that the frag size is fixed per-writer.

    let frag_size = usize::from(frag_size);
    let frags_in_submessage = usize::from(datafrag.fragments_in_submessage);
    if frags_in_submessage == 0 {
      error!("insert_frags: fragments_in_submessage is zero");
      return false;
    }
    let fragment_starting_num: usize = match u32::from(datafrag.fragment_starting_num).try_into() {
      Ok(n) if n >= 1 => n,
      _ => {
        error!(
          "insert_frags: invalid fragment_starting_num {:?}",
          datafrag.fragment_starting_num
        );
        return false;
      }
    };
    let start_frag_from_0 = fragment_starting_num - 1;
    if start_frag_from_0 + frags_in_submessage > self.fragment_count {
      error!(
        "insert_frags: fragment span out of bounds: start={fragment_starting_num} \
         count={frags_in_submessage} total={}",
        self.fragment_count
      );
      return false;
    }

    debug!(
      "insert_frags: datafrag.writer_sn = {:?}, frag_size = {:?}, datafrag.fragment_size = {:?}, \
       datafrag.fragment_starting_num = {:?}, datafrag.fragments_in_submessage = {:?}, \
       datafrag.data_size = {:?}",
      datafrag.writer_sn,
      frag_size,
      datafrag.fragment_size,
      datafrag.fragment_starting_num,
      datafrag.fragments_in_submessage,
      datafrag.data_size
    );

    // unwrap: u32 should fit into usize
    let from_byte = start_frag_from_0 * frag_size;

    // Last fragment might be smaller than fragment size
    // Copy reported number of fragments, or as much data as there is, whichever
    // ends first.
    // And clamp to assembly buffer length to avoid buffer overrun.
    let to_before_byte = std::cmp::min(
      from_byte
        + std::cmp::min(
          frags_in_submessage * frag_size,
          datafrag.serialized_payload.len(),
        ),
      self.buffer_bytes.len(),
    );
    if from_byte > to_before_byte {
      error!(
        "insert_frags: invalid byte range from_byte={from_byte} to_before_byte={to_before_byte}"
      );
      return false;
    }
    let payload_size = to_before_byte - from_byte;

    // sanity check data size
    // Last fragment may be smaller than frags_in_submessage * frag_size
    let last_frag_in_submessage = start_frag_from_0 + frags_in_submessage;
    if last_frag_in_submessage < self.fragment_count
      && datafrag.serialized_payload.len() < frags_in_submessage * frag_size
    {
      error!(
        "Received DATAFRAG too small. fragment_starting_num={} out of fragment_count={}, \
         frags_in_submessage={}, frag_size={} but payload length = {}. Original data_size={}",
        fragment_starting_num,
        self.fragment_count,
        frags_in_submessage,
        frag_size,
        datafrag.serialized_payload.len(),
        datafrag.data_size,
      );
    }

    debug!("insert_frags: from_byte = {from_byte:?}, to_before_byte = {to_before_byte:?}");

    debug!(
      "insert_frags: dataFrag.serializedPayload.len = {:?}",
      datafrag.serialized_payload.len()
    );

    self.buffer_bytes.as_mut()[from_byte..to_before_byte]
      .copy_from_slice(&datafrag.serialized_payload[..payload_size]);

    for f in 0..frags_in_submessage {
      self.received_bitmap.set(start_frag_from_0 + f, true);
    }
    self.modified_time = Timestamp::now();
    true
  }

  pub fn is_complete(&self) -> bool {
    self.received_bitmap.all() // return if all are received
  }
}

// Upper bound on the number of concurrent (incomplete) reassembly buffers kept
// per writer. Fragments belonging to one sample normally arrive back-to-back,
// so only a handful of samples are ever mid-reassembly at once; this cap is far
// above that. Its purpose is to bound memory when samples never complete, e.g.
// under best-effort overload where fragments are dropped: without it, one
// incomplete `AssemblyBuffer` (a full sample-sized allocation) accrues per lost
// sample and is only reclaimed by a 10 s idle timeout, growing to gigabytes.
// When exceeded we evict the oldest (lowest sequence number) buffer, which
// under best effort is lost anyway and under reliable will be re-requested.
const MAX_ASSEMBLY_BUFFERS: usize = 128;

// Assembles fragments from a single (remote) Writer
// So there is only one sequence of SNs
pub(crate) struct FragmentAssembler {
  fragment_size: u16, // number of bytes per fragment. Each writer must select one constant value.
  assembly_buffers: BTreeMap<SequenceNumber, AssemblyBuffer>,
}

impl fmt::Debug for FragmentAssembler {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("FragmentAssembler - fields omitted")
      // insert field printing here, if you really need it.
      .finish()
  }
}

impl FragmentAssembler {
  pub fn new(fragment_size: u16) -> Self {
    debug!("new FragmentAssembler. frag_size = {fragment_size}");
    Self {
      fragment_size,
      assembly_buffers: BTreeMap::new(),
    }
  }

  // Returns completed DDSData, when complete, and disposes the assembly buffer.
  pub fn new_datafrag(
    &mut self,
    datafrag: &DataFrag,
    flags: BitFlags<DATAFRAG_Flags>,
  ) -> Option<DDSData> {
    let writer_sn = datafrag.writer_sn;
    let frag_size = self.fragment_size;

    let sn = datafrag.writer_sn;
    if !self.assembly_buffers.contains_key(&sn) {
      let Some(buf) = AssemblyBuffer::new(datafrag) else {
        error!("new_datafrag: failed to create AssemblyBuffer for {sn:?}");
        return None;
      };
      self.assembly_buffers.insert(sn, buf);
    }

    let Some(assembly_buffer) = self.assembly_buffers.get_mut(&sn) else {
      error!("new_datafrag: AssemblyBuffer missing for {sn:?}");
      return None;
    };

    if !assembly_buffer.insert_frags(datafrag, frag_size) {
      error!("new_datafrag: rejected invalid DATAFRAG for {sn:?}");
      return None;
    }

    if assembly_buffer.is_complete() {
      debug!("new_datafrag: COMPLETED FRAGMENT");
      if let Some(assembly_buffer) = self.assembly_buffers.remove(&writer_sn) {
        // Return what we have assembled.
        let serialized_data_or_key =
          SerializedPayload::from_bytes(&assembly_buffer.buffer_bytes.freeze()).map_or_else(
            |e| {
              error!("Deserializing SerializedPayload from DATAFRAG: {:?}", e);
              None
            },
            Some,
          )?;
        let dds_data = if flags.contains(DATAFRAG_Flags::Key) {
          DDSData::new_disposed_by_key(ChangeKind::NotAliveDisposed, serialized_data_or_key)
        } else {
          // it is data
          DDSData::new(serialized_data_or_key)
        };
        Some(dds_data) // completed data from fragments
      } else {
        error!("Assembly buffer mysteriously lost");
        None
      }
    } else {
      debug!("new_dataFrag: FRAGMENT NOT COMPLETED YET");
      // Bound memory: never keep more than MAX_ASSEMBLY_BUFFERS incomplete
      // reassemblies. Evict the oldest (lowest SN) first; those are the least
      // likely to still complete (their remaining fragments are long gone under
      // best effort, and will be re-requested under reliable).
      while self.assembly_buffers.len() > MAX_ASSEMBLY_BUFFERS {
        self.assembly_buffers.pop_first();
      }
      None
    }
  }

  pub fn garbage_collect_before(&mut self, expire_before: Timestamp) {
    self.assembly_buffers.retain(|sn, ab| {
      let retain = ab.modified_time >= expire_before;
      if !retain {
        info!("AssemblyBuffer dropping {sn:?}");
      }
      retain
    });
  }

  // pub fn partially_received_sequence_numbers_iterator(&self) -> Box<dyn
  // Iterator<Item=SequenceNumber>> {   // Since we should only know about SNs
  // via DATAFRAG messages   // and AssemblyBuffers are removed immediately on
  // completion,   // the list should be just the list of current
  // AssemblyBuffers   self.assembly_buffers.keys()
  // }

  pub fn is_partially_received(&self, sn: SequenceNumber) -> bool {
    self.assembly_buffers.contains_key(&sn)
    // assembly buffers map contains a key (SN) if and only if we have some
    // frags but not all
  }

  pub fn missing_frags_for(
    &self,
    seq: SequenceNumber,
  ) -> Box<dyn '_ + Iterator<Item = FragmentNumber>> {
    match self.assembly_buffers.get(&seq) {
      None => Box::new(iter::empty()),
      Some(ab) => {
        let iter = (0..ab.fragment_count)
          .filter(move |f| !ab.received_bitmap.get(*f).unwrap_or(true))
          .map(|f| FragmentNumber::new((f + 1).try_into().unwrap()));
        Box::new(iter)
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use bytes::Bytes;

  use super::AssemblyBuffer;
  use crate::{
    messages::submessages::submessages::DataFrag, structure::sequence_number::FragmentNumber,
  };

  // Build a DATAFRAG submessage carrying the contiguous run of `k` fragments
  // starting at 1-based `start`, with the given payload bytes.
  fn datafrag(start: u32, k: u16, frag_size: u16, data_size: u32, payload: Vec<u8>) -> DataFrag {
    DataFrag {
      fragment_starting_num: FragmentNumber::new(start),
      fragments_in_submessage: k,
      fragment_size: frag_size,
      data_size,
      serialized_payload: Bytes::from(payload),
      ..Default::default()
    }
  }

  // A DATAFRAG that packs K > 1 fragments in one submessage must reassemble the
  // same bytes as one-fragment-per-submessage would. This exercises the
  // adaptive-packing writer path against the (unchanged) receiver.
  #[test]
  fn reassemble_multi_fragment_datafrag() {
    let frag_size = 1024u16;
    let data_size = 2600u32; // 3 fragments: 1024, 1024, 552
    let whole: Vec<u8> = (0..data_size as usize).map(|i| (i % 251) as u8).collect();

    // First submessage packs fragments 1 and 2 (K = 2, 2048 payload bytes).
    let first = datafrag(1, 2, frag_size, data_size, whole[0..2048].to_vec());
    let mut ab = AssemblyBuffer::new(&first).expect("valid first fragment");
    assert!(!ab.is_complete());
    ab.insert_frags(&first, frag_size);
    assert!(!ab.is_complete(), "still missing the tail fragment");

    // Trailing submessage carries the shorter final fragment 3 (552 bytes).
    let tail = datafrag(3, 1, frag_size, data_size, whole[2048..2600].to_vec());
    ab.insert_frags(&tail, frag_size);
    assert!(ab.is_complete(), "all fragments received");
    assert_eq!(
      &ab.buffer_bytes[..],
      &whole[..],
      "reassembled bytes must match the original sample"
    );
  }

  // A single DATAFRAG carrying the whole sample in one multi-fragment run.
  #[test]
  fn reassemble_single_submessage_all_fragments() {
    let frag_size = 512u16;
    let data_size = 1500u32; // 3 fragments: 512, 512, 476
    let whole: Vec<u8> = (0..data_size as usize).map(|i| (i % 97) as u8).collect();

    let all = datafrag(1, 3, frag_size, data_size, whole.clone());
    let mut ab = AssemblyBuffer::new(&all).expect("valid fragment set");
    ab.insert_frags(&all, frag_size);
    assert!(ab.is_complete());
    assert_eq!(&ab.buffer_bytes[..], &whole[..]);
  }

  // Malformed span (starting at last fragment but claiming multiple fragments)
  // must not panic and must not falsely complete reassembly.
  #[test]
  fn reject_fragment_span_beyond_total() {
    let frag_size = 256u16;
    let data_size = 512u32; // 2 fragments total
    let bad = datafrag(2, 2, frag_size, data_size, vec![0u8; 256]);
    let mut ab = AssemblyBuffer::new(&bad).expect("buffer for valid data_size");
    assert!(!ab.insert_frags(&bad, frag_size));
    assert!(!ab.is_complete());
  }

  #[test]
  fn fragment_assembler_rejects_span_beyond_total() {
    use enumflags2::BitFlags;

    use super::FragmentAssembler;
    use crate::messages::submessages::submessages::DATAFRAG_Flags;

    let frag_size = 256u16;
    let data_size = 512u32;
    let bad = datafrag(2, 2, frag_size, data_size, vec![0u8; 256]);
    let mut fa = FragmentAssembler::new(frag_size);
    assert!(fa
      .new_datafrag(&bad, BitFlags::<DATAFRAG_Flags>::empty())
      .is_none());
  }
}
