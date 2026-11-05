use parquet::file::metadata::{
    FileMetaData, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter,
};

pub const MAX_FOOTER_BYTES: usize = 256 * 1024;
pub const MAX_ROW_GROUPS: usize = 512;

pub struct Writer {
    metadata: ParquetMetaData,
    pub buffer: Vec<u8>,
    pub flushed: u64,
}

impl Writer {
    pub fn new(source: &ParquetMetaData) -> Self {
        Self {
            metadata: ParquetMetaData::new(source.file_metadata().clone(), Vec::new()),
            buffer: b"PAR1".to_vec(),
            flushed: 0,
        }
    }

    pub fn append(&mut self, bytes: &[u8], source: &ParquetMetaData) -> Result<(), String> {
        let expected = self.metadata.file_metadata();
        let incoming = source.file_metadata();
        if expected.schema() != incoming.schema()
            || expected.key_value_metadata() != incoming.key_value_metadata()
        {
            return Err("compact_schema_mismatch".into());
        }
        if self.metadata.num_row_groups() + source.num_row_groups() > MAX_ROW_GROUPS {
            return Err("compact_row_group_limit".into());
        }
        let mut groups = self.metadata.row_groups().to_vec();
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
                    .set_ordinal(groups.len() as i32)
                    .build()
                    .map_err(|e| e.to_string())?,
            );
        }
        let rows = groups.iter().try_fold(0i64, |total, group| {
            total
                .checked_add(group.num_rows())
                .ok_or("compact_row_count_overflow")
        })?;
        let metadata = FileMetaData::new(
            expected.version(),
            rows,
            Some("fastly-log-analytics resumable compactor".into()),
            expected.key_value_metadata().cloned(),
            expected.schema_descr_ptr(),
            expected.column_orders().cloned(),
        );
        self.metadata = ParquetMetaData::new(metadata, groups);
        if self.footer()?.len() > MAX_FOOTER_BYTES {
            return Err("compact_footer_limit".into());
        }
        Ok(())
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
        let mut groups = self.metadata.row_groups().to_vec();
        groups.push(
            group
                .clone()
                .into_builder()
                .set_column_metadata(columns)
                .set_file_offset(i64::try_from(destination).map_err(|_| "compact_offset_overflow")?)
                .set_ordinal(groups.len() as i32)
                .build()
                .map_err(|e| e.to_string())?,
        );
        let rows = self
            .metadata
            .row_groups()
            .iter()
            .try_fold(group.num_rows(), |rows, group| {
                rows.checked_add(group.num_rows())
            })
            .ok_or("compact_row_count_overflow")?;
        let metadata = FileMetaData::new(
            expected.version(),
            rows,
            Some("fastly-log-analytics resumable compactor".into()),
            expected.key_value_metadata().cloned(),
            expected.schema_descr_ptr(),
            expected.column_orders().cloned(),
        );
        self.metadata = ParquetMetaData::new(metadata, groups);
        if self.footer()?.len() > MAX_FOOTER_BYTES {
            return Err("compact_footer_limit".into());
        }
        Ok((
            start,
            end.checked_sub(start).ok_or("compact_offset_overflow")?,
        ))
    }

    pub fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }

    pub fn footer(&self) -> Result<Vec<u8>, String> {
        let mut footer = Vec::new();
        ParquetMetaDataWriter::new(&mut footer, &self.metadata)
            .with_preserve_page_index_locations(false)
            .finish()
            .map_err(|e| e.to_string())?;
        Ok(footer)
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
        })
    }
}
