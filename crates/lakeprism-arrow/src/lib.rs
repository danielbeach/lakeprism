use std::sync::Arc;

use arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
use arrow::array::{ArrayRef, StringArray, StructArray, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use lakeprism_core::{MediaRef, StorageMode};

pub fn media_ref_data_type() -> DataType {
    DataType::Struct(
        vec![
            Arc::new(Field::new("uri", DataType::Utf8, false)),
            Arc::new(Field::new("media_type", DataType::Utf8, false)),
            Arc::new(Field::new("mime_type", DataType::Utf8, true)),
            Arc::new(Field::new("size_bytes", DataType::UInt64, true)),
            Arc::new(Field::new("etag", DataType::Utf8, true)),
            Arc::new(Field::new(
                "modified_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            Arc::new(Field::new("catalog_ref", DataType::Utf8, true)),
            Arc::new(Field::new("storage_mode", DataType::Utf8, false)),
            Arc::new(Field::new_map(
                "metadata",
                "entries",
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
                false,
                false,
            )),
        ]
        .into(),
    )
}

pub fn media_ref_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "media",
        media_ref_data_type(),
        false,
    )]))
}

pub fn media_ref_record_batch(media_refs: &[MediaRef]) -> arrow::error::Result<RecordBatch> {
    let uris = StringArray::from_iter_values(media_refs.iter().map(|media| media.uri.as_str()));
    let media_types =
        StringArray::from_iter_values(media_refs.iter().map(|media| media.media_type.as_str()));
    let mime_types =
        StringArray::from_iter(media_refs.iter().map(|media| media.mime_type.as_deref()));
    let sizes = UInt64Array::from_iter(media_refs.iter().map(|media| media.size_bytes));
    let etags = StringArray::from_iter(media_refs.iter().map(|media| media.etag.as_deref()));
    let modified_at = TimestampMillisecondArray::from_iter(
        media_refs.iter().map(|media| media.modified_at_unix_millis),
    );
    let catalog_refs =
        StringArray::from_iter(media_refs.iter().map(|media| media.catalog_ref.as_deref()));
    let storage_modes =
        StringArray::from_iter_values(media_refs.iter().map(|media| match media.storage_mode {
            StorageMode::External => "external",
            StorageMode::Managed => "managed",
            StorageMode::Inline => "inline",
        }));
    let mut metadata = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".to_owned(),
            key: "key".to_owned(),
            value: "value".to_owned(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    for media in media_refs {
        for (key, value) in &media.metadata {
            metadata.keys().append_value(key);
            metadata.values().append_value(value);
        }
        metadata.append(true)?;
    }

    let fields = match media_ref_data_type() {
        DataType::Struct(fields) => fields,
        _ => unreachable!(),
    };
    let values: Vec<ArrayRef> = vec![
        Arc::new(uris),
        Arc::new(media_types),
        Arc::new(mime_types),
        Arc::new(sizes),
        Arc::new(etags),
        Arc::new(modified_at),
        Arc::new(catalog_refs),
        Arc::new(storage_modes),
        Arc::new(metadata.finish()),
    ];
    let media_array = StructArray::new(fields, values, None);
    RecordBatch::try_new(media_ref_schema(), vec![Arc::new(media_array)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakeprism_core::StorageMode;
    use std::collections::BTreeMap;

    #[test]
    fn media_reference_batches_use_a_portable_struct_column() {
        let mut media =
            MediaRef::new("file:///media/a.mp4", "video", StorageMode::External).unwrap();
        media.modified_at_unix_millis = Some(42);
        media.catalog_ref = Some("local.media.videos".to_owned());
        media.metadata = BTreeMap::from([("camera".to_owned(), "north".to_owned())]);
        let batch = media_ref_record_batch(&[media]).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), media_ref_schema());
        assert_eq!(batch.schema().field(0).data_type(), &media_ref_data_type());
        let schema = batch.schema();
        let media_fields = match schema.field(0).data_type() {
            DataType::Struct(fields) => fields,
            _ => unreachable!(),
        };
        assert_eq!(media_fields.len(), 9);
        assert_eq!(media_fields[5].name(), "modified_at");
        assert_eq!(media_fields[6].name(), "catalog_ref");
        assert_eq!(media_fields[8].name(), "metadata");
    }
}
