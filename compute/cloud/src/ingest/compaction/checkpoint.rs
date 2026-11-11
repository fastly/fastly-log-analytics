use parquet::file::metadata::{
    FileMetaData, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter, RowGroupMetaData,
};

/// A compact's footer grows by one column-chunk entry per column per row group (about 6 KiB
/// per 4096-row group of the request schema), so this bounds a compact's size well before its
/// byte target: 1 MiB is roughly 170 groups, about 700k rows or 28 MiB of request logs.
pub const MAX_FOOTER_BYTES: usize = 1024 * 1024;
pub const MAX_ROW_GROUPS: usize = 512;
/// Covers what an append can add outside its own row groups: the total row count's and the
/// row-group list header's varints.
const APPEND_SLACK: usize = 16;

pub struct Writer {
    metadata: ParquetMetaData,
    pub buffer: Vec<u8>,
    pub flushed: u64,
    /// At least the encoded footer's length, so appends far below the limit skip re-encoding it.
    footer_bound: usize,
    #[cfg(test)]
    encodes: std::cell::Cell<usize>,
}

impl Writer {
    pub fn new(source: &ParquetMetaData) -> Self {
        let mut writer = Self {
            metadata: ParquetMetaData::new(source.file_metadata().clone(), Vec::new()),
            buffer: b"PAR1".to_vec(),
            flushed: 0,
            footer_bound: usize::MAX,
            #[cfg(test)]
            encodes: Default::default(),
        };
        if let Ok(empty) = encoded_len(&ParquetMetaData::new(writer.file_metadata(0), Vec::new())) {
            writer.footer_bound = empty;
        }
        writer
    }

    fn file_metadata(&self, rows: i64) -> FileMetaData {
        let expected = self.metadata.file_metadata();
        FileMetaData::new(
            expected.version(),
            rows,
            Some("fastly-log-analytics resumable compactor".into()),
            expected.key_value_metadata().cloned(),
            expected.schema_descr_ptr(),
            expected.column_orders().cloned(),
        )
    }

    /// Adds rebased row groups, encoding the whole footer only once its bound nears the limit.
    fn extend(&mut self, added: Vec<RowGroupMetaData>) -> Result<(), String> {
        let sum = |groups: &[RowGroupMetaData]| {
            groups
                .iter()
                .try_fold(0i64, |total, group| total.checked_add(group.num_rows()))
                .ok_or("compact_row_count_overflow")
        };
        let added_rows = sum(&added)?;
        let rows = sum(self.metadata.row_groups())?
            .checked_add(added_rows)
            .ok_or("compact_row_count_overflow")?;
        let empty = encoded_len(&ParquetMetaData::new(self.file_metadata(0), Vec::new()))?;
        let part = ParquetMetaData::new(self.file_metadata(added_rows), added);
        let growth = encoded_len(&part)?.saturating_sub(empty) + APPEND_SLACK;
        let added = part.into_builder().take_row_groups();
        let metadata = self.file_metadata(rows);
        let mut groups = std::mem::replace(
            &mut self.metadata,
            ParquetMetaData::new(metadata.clone(), Vec::new()),
        )
        .into_builder()
        .take_row_groups();
        groups.extend(added);
        self.metadata = ParquetMetaData::new(metadata, groups);
        self.footer_bound = self.footer_bound.saturating_add(growth);
        if self.footer_bound > MAX_FOOTER_BYTES {
            self.footer_bound = self.footer()?.len();
            if self.footer_bound > MAX_FOOTER_BYTES {
                return Err("compact_footer_limit".into());
            }
        }
        Ok(())
    }

    /// Whether `source` has this output's schema and key-value metadata.
    pub fn accepts(&self, source: &ParquetMetaData) -> bool {
        let expected = self.metadata.file_metadata();
        let incoming = source.file_metadata();
        expected.schema() == incoming.schema()
            && expected.key_value_metadata() == incoming.key_value_metadata()
    }

    pub fn append(&mut self, bytes: &[u8], source: &ParquetMetaData) -> Result<(), String> {
        if !self.accepts(source) {
            return Err("compact_schema_mismatch".into());
        }
        if self.metadata.num_row_groups() + source.num_row_groups() > MAX_ROW_GROUPS {
            return Err("compact_row_group_limit".into());
        }
        let ordinal = self.metadata.num_row_groups();
        let mut groups = Vec::with_capacity(source.num_row_groups());
        for group in source.row_groups() {
            let mut columns = Vec::new();
            let group_start = self.flushed + self.buffer.len() as u64;
            for column in group.columns() {
                let (offset, length) = column.byte_range();
                let end = offset
                    .checked_add(length)
                    .ok_or("compact_offset_overflow")?;
                let chunk = bytes
                    .get(
                        offset as usize
                            ..usize::try_from(end).map_err(|_| "compact_offset_overflow")?,
                    )
                    .ok_or("compact_column_out_of_bounds")?;
                let next_offset = self.flushed + self.buffer.len() as u64;
                let shift = i64::try_from(next_offset).map_err(|_| "compact_offset_overflow")?
                    - i64::try_from(offset).map_err(|_| "compact_offset_overflow")?;
                let rebase = |value: i64| value.checked_add(shift).ok_or("compact_offset_overflow");
                let metadata = column
                    .clone()
                    .into_builder()
                    .set_data_page_offset(rebase(column.data_page_offset())?)
                    .set_dictionary_page_offset(
                        column.dictionary_page_offset().map(rebase).transpose()?,
                    )
                    .set_index_page_offset(None)
                    .set_bloom_filter_offset(None)
                    .set_bloom_filter_length(None)
                    .set_column_index_offset(None)
                    .set_column_index_length(None)
                    .set_offset_index_offset(None)
                    .set_offset_index_length(None)
                    .build()
                    .map_err(|e| e.to_string())?;
                self.buffer.extend_from_slice(chunk);
                columns.push(metadata);
            }
            groups.push(
                group
                    .clone()
                    .into_builder()
                    .set_column_metadata(columns)
                    .set_file_offset(group_start as i64)
                    .set_ordinal((ordinal + groups.len()) as i32)
                    .build()
                    .map_err(|e| e.to_string())?,
            );
        }
        self.extend(groups)
    }

    pub fn append_group_metadata(
        &mut self,
        source: &ParquetMetaData,
        index: usize,
    ) -> Result<(u64, u64), String> {
        let expected = self.metadata.file_metadata();
        let incoming = source.file_metadata();
        if expected.schema() != incoming.schema()
            || expected.key_value_metadata() != incoming.key_value_metadata()
        {
            return Err("compact_schema_mismatch".into());
        }
        if self.metadata.num_row_groups() >= MAX_ROW_GROUPS {
            return Err("compact_row_group_limit".into());
        }
        let group = source
            .row_groups()
            .get(index)
            .ok_or("compact_row_group_invalid")?;
        let start = group
            .columns()
            .iter()
            .map(|column| column.byte_range().0)
            .min()
            .ok_or("compact_row_group_invalid")?;
        let end = group
            .columns()
            .iter()
            .map(|column| {
                let (offset, length) = column.byte_range();
                offset.checked_add(length).ok_or("compact_offset_overflow")
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max()
            .ok_or("compact_row_group_invalid")?;
        let destination = self.flushed + self.buffer.len() as u64;
        let shift = i64::try_from(destination).map_err(|_| "compact_offset_overflow")?
            - i64::try_from(start).map_err(|_| "compact_offset_overflow")?;
        let rebase = |value: i64| value.checked_add(shift).ok_or("compact_offset_overflow");
        let columns = group
            .columns()
            .iter()
            .map(|column| {
                column
                    .clone()
                    .into_builder()
                    .set_data_page_offset(rebase(column.data_page_offset())?)
                    .set_dictionary_page_offset(
                        column.dictionary_page_offset().map(rebase).transpose()?,
                    )
                    .set_index_page_offset(None)
                    .set_bloom_filter_offset(None)
                    .set_bloom_filter_length(None)
                    .set_column_index_offset(None)
                    .set_column_index_length(None)
                    .set_offset_index_offset(None)
                    .set_offset_index_length(None)
                    .build()
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let rebased = group
            .clone()
            .into_builder()
            .set_column_metadata(columns)
            .set_file_offset(i64::try_from(destination).map_err(|_| "compact_offset_overflow")?)
            .set_ordinal(self.metadata.num_row_groups() as i32)
            .build()
            .map_err(|e| e.to_string())?;
        self.extend(vec![rebased])?;
        Ok((
            start,
            end.checked_sub(start).ok_or("compact_offset_overflow")?,
        ))
    }

    pub fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }

    pub fn footer(&self) -> Result<Vec<u8>, String> {
        #[cfg(test)]
        self.encodes.set(self.encodes.get() + 1);
        encode(&self.metadata)
    }

    pub fn checkpoint(&self) -> Result<Vec<u8>, String> {
        let footer = self.footer()?;
        let mut body = Vec::with_capacity(16 + footer.len() + self.buffer.len());
        body.extend_from_slice(b"FLP1");
        body.extend_from_slice(&self.flushed.to_le_bytes());
        body.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        body.extend_from_slice(&footer);
        body.extend_from_slice(&self.buffer);
        Ok(body)
    }

    pub fn restore(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 24 || &bytes[..4] != b"FLP1" {
            return Err("compact_checkpoint_invalid".into());
        }
        let flushed = u64::from_le_bytes(
            bytes[4..12]
                .try_into()
                .map_err(|_| "compact_checkpoint_invalid")?,
        );
        let length = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| "compact_checkpoint_invalid")?,
        ) as usize;
        if !(8..=MAX_FOOTER_BYTES).contains(&length) || bytes.len() < 16 + length {
            return Err("compact_checkpoint_invalid".into());
        }
        let footer = &bytes[16..16 + length];
        if &footer[length - 4..] != b"PAR1" {
            return Err("compact_checkpoint_invalid".into());
        }
        let metadata = ParquetMetaDataReader::decode_metadata(&footer[..length - 8])
            .map_err(|e| e.to_string())?;
        Ok(Self {
            metadata,
            buffer: bytes[16 + length..].to_vec(),
            flushed,
            footer_bound: length,
            #[cfg(test)]
            encodes: Default::default(),
        })
    }
}

fn encode(metadata: &ParquetMetaData) -> Result<Vec<u8>, String> {
    let mut footer = Vec::new();
    ParquetMetaDataWriter::new(&mut footer, metadata)
        .with_preserve_page_index_locations(false)
        .finish()
        .map_err(|e| e.to_string())?;
    Ok(footer)
}

fn encoded_len(metadata: &ParquetMetaData) -> Result<usize, String> {
    encode(metadata).map(|footer| footer.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::sync::Arc;

    fn source(seed: i64) -> (Bytes, ParquetMetaData) {
        let columns = (0..40)
            .map(|c| {
                let values = (0..64).map(|r| seed * 1_000_003 + c * 7919 + r * r);
                (
                    format!("c{c}"),
                    Arc::new(Int64Array::from_iter_values(values)) as ArrayRef,
                )
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        let bytes = Bytes::from(writer.into_inner().unwrap());
        let metadata = SerializedFileReader::new(bytes.clone())
            .unwrap()
            .metadata()
            .clone();
        (bytes, metadata)
    }

    fn exact(writer: &Writer) -> usize {
        let mut footer = Vec::new();
        ParquetMetaDataWriter::new(&mut footer, writer.metadata())
            .with_preserve_page_index_locations(false)
            .finish()
            .unwrap();
        footer.len()
    }

    #[test]
    fn appends_check_the_footer_limit_without_reencoding_the_whole_footer_each_time() {
        let sources = (0..32).map(source).collect::<Vec<_>>();
        let mut writer = Writer::new(&sources[0].1);
        for (bytes, metadata) in &sources {
            writer.append(bytes, metadata).unwrap();
        }
        assert!(exact(&writer) < MAX_FOOTER_BYTES / 2);
        assert!(
            writer.encodes.get() <= 2,
            "{} full footer encodes for {} appends far below the limit",
            writer.encodes.get(),
            sources.len()
        );
    }

    #[test]
    fn the_footer_limit_rejects_exactly_the_first_append_past_it() {
        let mut writer = Writer::new(&source(0).1);
        let mut appended = 0;
        let error = loop {
            let (bytes, metadata) = source(appended);
            let before = writer.metadata().clone();
            match writer.append(&bytes, &metadata) {
                Ok(()) => {
                    assert!(exact(&writer) <= MAX_FOOTER_BYTES);
                    appended += 1;
                }
                Err(error) => {
                    assert!(exact(&writer) > MAX_FOOTER_BYTES);
                    let mut fits = Writer::new(&metadata);
                    fits.metadata = before;
                    assert!(exact(&fits) <= MAX_FOOTER_BYTES);
                    break error;
                }
            }
        };
        assert_eq!(error, "compact_footer_limit");
        assert!(appended > 32, "{appended}");
    }
}
