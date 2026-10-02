use std::io::{Cursor, Read};
use std::path::Path;

use quick_xml::Reader;
use quick_xml::events::Event;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DocumentError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Pdf(#[from] lopdf::Error),
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),
    #[error(transparent)]
    Xml(#[from] quick_xml::Error),
    #[error(transparent)]
    Escape(#[from] quick_xml::escape::EscapeError),
    #[error("DOCX package does not contain word/document.xml")]
    MissingDocumentXml,
    #[error("document input is {actual_bytes} bytes, exceeding the {maximum_bytes}-byte limit")]
    InputTooLarge {
        actual_bytes: u64,
        maximum_bytes: u64,
    },
    #[error("DOCX document XML is {actual_bytes} bytes, exceeding the {maximum_bytes}-byte limit")]
    DocumentXmlTooLarge {
        actual_bytes: u64,
        maximum_bytes: u64,
    },
    #[error("document has more than the {maximum} permitted {feature} features")]
    FeatureLimitExceeded {
        feature: &'static str,
        maximum: usize,
    },
    #[error(
        "document image {name} is {actual_bytes} bytes, exceeding the {maximum_bytes}-byte limit"
    )]
    ImageTooLarge {
        name: String,
        actual_bytes: u64,
        maximum_bytes: u64,
    },
    #[error(
        "extracted document images total {actual_bytes} bytes, exceeding the {maximum_bytes}-byte limit"
    )]
    TotalImageBytesTooLarge {
        actual_bytes: u64,
        maximum_bytes: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentSection {
    pub ordinal: u32,
    pub heading: Option<String>,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentTable {
    pub ordinal: u32,
    pub rows: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentImage {
    pub ordinal: u32,
    pub name: String,
    pub media_type: Option<String>,
    /// The original DOCX package image bytes, or the encoded PDF image stream.
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DocumentLimits {
    pub max_input_bytes: u64,
    pub max_docx_document_xml_bytes: u64,
    pub max_sections: usize,
    pub max_table_cells: usize,
    pub max_images: usize,
    pub max_image_bytes: u64,
    pub max_total_image_bytes: u64,
}

impl Default for DocumentLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 64 * 1024 * 1024,
            max_docx_document_xml_bytes: 64 * 1024 * 1024,
            max_sections: 16_384,
            max_table_cells: 65_536,
            max_images: 1_024,
            max_image_bytes: 32 * 1024 * 1024,
            max_total_image_bytes: 64 * 1024 * 1024,
        }
    }
}

pub fn extract_pdf_sections(path: impl AsRef<Path>) -> Result<Vec<DocumentSection>, DocumentError> {
    extract_pdf_sections_with_limits(path, DocumentLimits::default())
}

pub fn extract_pdf_sections_with_limits(
    path: impl AsRef<Path>,
    limits: DocumentLimits,
) -> Result<Vec<DocumentSection>, DocumentError> {
    validate_file_size(path.as_ref(), limits.max_input_bytes)?;
    let document = lopdf::Document::load(path)?;
    document
        .get_pages()
        .into_keys()
        .enumerate()
        .map(|(index, page_number)| {
            if index >= limits.max_sections {
                return Err(DocumentError::FeatureLimitExceeded {
                    feature: "sections",
                    maximum: limits.max_sections,
                });
            }
            Ok(DocumentSection {
                ordinal: page_number,
                heading: None,
                text: document.extract_text(&[page_number])?,
            })
        })
        .collect()
}

/// Extracts bounded PDF sections from an already range-fetched object. This is
/// used by remote resolvers after they have enforced their object and byte
/// budgets; it does not perform networking itself.
pub fn extract_pdf_sections_from_bytes_with_limits(
    bytes: &[u8],
    limits: DocumentLimits,
) -> Result<Vec<DocumentSection>, DocumentError> {
    let actual_bytes = bytes.len() as u64;
    if actual_bytes > limits.max_input_bytes {
        return Err(DocumentError::InputTooLarge {
            actual_bytes,
            maximum_bytes: limits.max_input_bytes,
        });
    }
    let document = lopdf::Document::load_mem(bytes)?;
    document
        .get_pages()
        .into_keys()
        .enumerate()
        .map(|(index, page_number)| {
            if index >= limits.max_sections {
                return Err(DocumentError::FeatureLimitExceeded {
                    feature: "sections",
                    maximum: limits.max_sections,
                });
            }
            Ok(DocumentSection {
                ordinal: page_number,
                heading: None,
                text: document.extract_text(&[page_number])?,
            })
        })
        .collect()
}

pub fn extract_docx_sections(
    path: impl AsRef<Path>,
) -> Result<Vec<DocumentSection>, DocumentError> {
    extract_docx_sections_with_limits(path, DocumentLimits::default())
}

pub fn extract_docx_sections_with_limits(
    path: impl AsRef<Path>,
    limits: DocumentLimits,
) -> Result<Vec<DocumentSection>, DocumentError> {
    validate_file_size(path.as_ref(), limits.max_input_bytes)?;
    let bytes = std::fs::read(path)?;
    extract_docx_sections_from_bytes_with_limits(&bytes, limits)
}

pub fn extract_docx_sections_from_bytes(
    bytes: &[u8],
) -> Result<Vec<DocumentSection>, DocumentError> {
    extract_docx_sections_from_bytes_with_limits(bytes, DocumentLimits::default())
}

pub fn extract_docx_sections_from_bytes_with_limits(
    bytes: &[u8],
    limits: DocumentLimits,
) -> Result<Vec<DocumentSection>, DocumentError> {
    let actual_bytes = bytes.len() as u64;
    if actual_bytes > limits.max_input_bytes {
        return Err(DocumentError::InputTooLarge {
            actual_bytes,
            maximum_bytes: limits.max_input_bytes,
        });
    }
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))?;
    let mut document_xml = archive
        .by_name("word/document.xml")
        .map_err(|error| match error {
            zip::result::ZipError::FileNotFound => DocumentError::MissingDocumentXml,
            other => DocumentError::Zip(other),
        })?;
    if document_xml.size() > limits.max_docx_document_xml_bytes {
        return Err(DocumentError::DocumentXmlTooLarge {
            actual_bytes: document_xml.size(),
            maximum_bytes: limits.max_docx_document_xml_bytes,
        });
    }
    let mut xml = String::new();
    document_xml.read_to_string(&mut xml)?;

    let mut reader = Reader::from_str(&xml);
    reader.config_mut().trim_text(true);
    let mut sections = Vec::new();
    let mut in_paragraph = false;
    let mut in_text = false;
    let mut paragraph_text = String::new();

    loop {
        match reader.read_event()? {
            Event::Start(event) if event.name().as_ref() == "w:p" => {
                in_paragraph = true;
                paragraph_text.clear();
            }

            Event::Start(event) if in_paragraph && event.name().as_ref() == "w:t" => {
                in_text = true;
            }
            Event::Text(event) if in_text => {
                paragraph_text.push_str(&quick_xml::escape::unescape(&event)?);
            }
            Event::End(event) if event.name().as_ref() == "w:t" => {
                in_text = false;
            }
            Event::End(event) if event.name().as_ref() == "w:p" => {
                if !paragraph_text.is_empty() {
                    if sections.len() >= limits.max_sections {
                        return Err(DocumentError::FeatureLimitExceeded {
                            feature: "sections",
                            maximum: limits.max_sections,
                        });
                    }
                    sections.push(DocumentSection {
                        ordinal: sections.len() as u32,
                        heading: None,
                        text: paragraph_text.clone(),
                    });
                }
                in_paragraph = false;
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(sections)
}

/// Extracts bounded PDF text-layout tables. A table is recognized only when a
/// page has two or more lines containing cells separated by at least two
/// whitespace characters. This deliberately conservative heuristic does not
/// infer ruled, merged, or image-only tables.
pub fn extract_pdf_tables_with_limits(
    path: impl AsRef<Path>,
    limits: DocumentLimits,
) -> Result<Vec<DocumentTable>, DocumentError> {
    validate_file_size(path.as_ref(), limits.max_input_bytes)?;
    let document = lopdf::Document::load(path)?;
    let mut tables = Vec::new();
    let mut cell_count = 0usize;
    for (page_index, page_number) in document.get_pages().into_keys().enumerate() {
        if page_index >= limits.max_sections {
            return Err(DocumentError::FeatureLimitExceeded {
                feature: "PDF pages",
                maximum: limits.max_sections,
            });
        }
        let rows = document
            .extract_text(&[page_number])?
            .lines()
            .filter_map(pdf_table_cells)
            .collect::<Vec<_>>();
        if rows.len() < 2 {
            continue;
        }
        cell_count = cell_count.saturating_add(rows.iter().map(Vec::len).sum::<usize>());
        if cell_count > limits.max_table_cells {
            return Err(DocumentError::FeatureLimitExceeded {
                feature: "table cells",
                maximum: limits.max_table_cells,
            });
        }
        if tables.len() >= limits.max_sections {
            return Err(DocumentError::FeatureLimitExceeded {
                feature: "tables",
                maximum: limits.max_sections,
            });
        }
        tables.push(DocumentTable {
            ordinal: page_number,
            rows,
        });
    }
    Ok(tables)
}

fn pdf_table_cells(line: &str) -> Option<Vec<String>> {
    let mut cells = Vec::new();
    let mut current = String::new();
    let mut whitespace = 0usize;
    for character in line.trim().chars() {
        if character.is_whitespace() {
            whitespace += 1;
            continue;
        }
        if whitespace >= 2 && !current.is_empty() {
            cells.push(std::mem::take(&mut current));
        } else if whitespace == 1 && !current.is_empty() {
            current.push(' ');
        }
        whitespace = 0;
        current.push(character);
    }
    if !current.is_empty() {
        cells.push(current);
    }
    (cells.len() >= 2).then_some(cells)
}

/// Extracts actual encoded image streams from PDF image XObjects. The returned
/// bytes are only standalone image files for DCT/JPX encoded sources; other
/// PDF image streams retain their PDF encoding and report no media type.
pub fn extract_pdf_images_with_limits(
    path: impl AsRef<Path>,
    limits: DocumentLimits,
) -> Result<Vec<DocumentImage>, DocumentError> {
    validate_file_size(path.as_ref(), limits.max_input_bytes)?;
    let document = lopdf::Document::load(path)?;
    let mut images = Vec::new();
    let mut total_bytes = 0u64;
    for (page_index, (page_number, page_id)) in document.get_pages().into_iter().enumerate() {
        if page_index >= limits.max_sections {
            return Err(DocumentError::FeatureLimitExceeded {
                feature: "PDF pages",
                maximum: limits.max_sections,
            });
        }
        for image in document.get_page_images(page_id)? {
            if images.len() >= limits.max_images {
                return Err(DocumentError::FeatureLimitExceeded {
                    feature: "images",
                    maximum: limits.max_images,
                });
            }
            let image_bytes = image.content.len() as u64;
            let name = format!("page-{page_number}-image-{}-{}", image.id.0, image.id.1);
            if image_bytes > limits.max_image_bytes {
                return Err(DocumentError::ImageTooLarge {
                    name,
                    actual_bytes: image_bytes,
                    maximum_bytes: limits.max_image_bytes,
                });
            }
            total_bytes = total_bytes.saturating_add(image_bytes);
            if total_bytes > limits.max_total_image_bytes {
                return Err(DocumentError::TotalImageBytesTooLarge {
                    actual_bytes: total_bytes,
                    maximum_bytes: limits.max_total_image_bytes,
                });
            }
            images.push(DocumentImage {
                ordinal: images.len() as u32,
                name,
                media_type: pdf_image_media_type(image.filters.as_deref()).map(str::to_owned),
                bytes: image.content.to_vec(),
            });
        }
    }
    Ok(images)
}

fn pdf_image_media_type(filters: Option<&[String]>) -> Option<&'static str> {
    let filters = filters?;
    if filters.iter().any(|filter| filter == "DCTDecode") {
        Some("image/jpeg")
    } else if filters.iter().any(|filter| filter == "JPXDecode") {
        Some("image/jp2")
    } else {
        None
    }
}

/// Extracts DOCX tables as normalized cell text.
pub fn extract_docx_tables_with_limits(
    path: impl AsRef<Path>,
    limits: DocumentLimits,
) -> Result<Vec<DocumentTable>, DocumentError> {
    validate_file_size(path.as_ref(), limits.max_input_bytes)?;
    let bytes = std::fs::read(path)?;
    extract_docx_tables_from_bytes_with_limits(&bytes, limits)
}

pub fn extract_docx_tables_from_bytes_with_limits(
    bytes: &[u8],
    limits: DocumentLimits,
) -> Result<Vec<DocumentTable>, DocumentError> {
    let xml = docx_document_xml(bytes, limits)?;
    let mut reader = Reader::from_str(&xml);
    reader.config_mut().trim_text(true);
    let mut tables = Vec::new();
    let mut rows = Vec::new();
    let mut cells = Vec::new();
    let mut in_table = false;
    let mut in_cell = false;
    let mut in_text = false;
    let mut cell_text = String::new();
    let mut cell_count = 0usize;

    loop {
        match reader.read_event()? {
            Event::Start(event) if event.name().as_ref() == "w:tbl" => {
                in_table = true;
                rows.clear();
            }
            Event::Start(event) if in_table && event.name().as_ref() == "w:tr" => cells.clear(),
            Event::Start(event) if in_table && event.name().as_ref() == "w:tc" => {
                in_cell = true;
                cell_text.clear();
            }
            Event::Start(event) if in_cell && event.name().as_ref() == "w:t" => in_text = true,
            Event::Text(event) if in_text => {
                cell_text.push_str(&quick_xml::escape::unescape(&event)?);
            }
            Event::End(event) if event.name().as_ref() == "w:t" => in_text = false,
            Event::End(event) if in_cell && event.name().as_ref() == "w:tc" => {
                cell_count += 1;
                if cell_count > limits.max_table_cells {
                    return Err(DocumentError::FeatureLimitExceeded {
                        feature: "table cells",
                        maximum: limits.max_table_cells,
                    });
                }
                cells.push(std::mem::take(&mut cell_text));
                in_cell = false;
            }
            Event::End(event) if in_table && event.name().as_ref() == "w:tr" => {
                rows.push(std::mem::take(&mut cells));
            }
            Event::End(event) if event.name().as_ref() == "w:tbl" => {
                if tables.len() >= limits.max_sections {
                    return Err(DocumentError::FeatureLimitExceeded {
                        feature: "tables",
                        maximum: limits.max_sections,
                    });
                }
                tables.push(DocumentTable {
                    ordinal: tables.len() as u32,
                    rows: std::mem::take(&mut rows),
                });
                in_table = false;
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(tables)
}

/// Extracts real embedded DOCX media. Bytes are not decoded or OCR'd.
pub fn extract_docx_images_with_limits(
    path: impl AsRef<Path>,
    limits: DocumentLimits,
) -> Result<Vec<DocumentImage>, DocumentError> {
    validate_file_size(path.as_ref(), limits.max_input_bytes)?;
    let bytes = std::fs::read(path)?;
    extract_docx_images_from_bytes_with_limits(&bytes, limits)
}

pub fn extract_docx_images_from_bytes_with_limits(
    bytes: &[u8],
    limits: DocumentLimits,
) -> Result<Vec<DocumentImage>, DocumentError> {
    if bytes.len() as u64 > limits.max_input_bytes {
        return Err(DocumentError::InputTooLarge {
            actual_bytes: bytes.len() as u64,
            maximum_bytes: limits.max_input_bytes,
        });
    }
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))?;
    let mut names = (0..archive.len())
        .filter_map(|index| {
            archive
                .by_index(index)
                .ok()
                .map(|file| file.name().to_owned())
        })
        .filter(|name| name.starts_with("word/media/") && !name.ends_with('/'))
        .collect::<Vec<_>>();
    names.sort();
    if names.len() > limits.max_images {
        return Err(DocumentError::FeatureLimitExceeded {
            feature: "images",
            maximum: limits.max_images,
        });
    }
    let mut total_bytes = 0u64;
    let mut images = Vec::with_capacity(names.len());
    for name in names {
        let mut file = archive.by_name(&name)?;
        if file.size() > limits.max_image_bytes {
            return Err(DocumentError::ImageTooLarge {
                name,
                actual_bytes: file.size(),
                maximum_bytes: limits.max_image_bytes,
            });
        }
        total_bytes += file.size();
        if total_bytes > limits.max_total_image_bytes {
            return Err(DocumentError::TotalImageBytesTooLarge {
                actual_bytes: total_bytes,
                maximum_bytes: limits.max_total_image_bytes,
            });
        }
        let mut image = Vec::with_capacity(file.size() as usize);
        file.read_to_end(&mut image)?;
        images.push(DocumentImage {
            ordinal: images.len() as u32,
            media_type: media_type_for_name(&name).map(str::to_owned),
            name,
            bytes: image,
        });
    }
    Ok(images)
}

fn docx_document_xml(bytes: &[u8], limits: DocumentLimits) -> Result<String, DocumentError> {
    if bytes.len() as u64 > limits.max_input_bytes {
        return Err(DocumentError::InputTooLarge {
            actual_bytes: bytes.len() as u64,
            maximum_bytes: limits.max_input_bytes,
        });
    }
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))?;
    let mut document_xml = archive
        .by_name("word/document.xml")
        .map_err(|error| match error {
            zip::result::ZipError::FileNotFound => DocumentError::MissingDocumentXml,
            other => DocumentError::Zip(other),
        })?;
    if document_xml.size() > limits.max_docx_document_xml_bytes {
        return Err(DocumentError::DocumentXmlTooLarge {
            actual_bytes: document_xml.size(),
            maximum_bytes: limits.max_docx_document_xml_bytes,
        });
    }
    let mut xml = String::new();
    document_xml.read_to_string(&mut xml)?;
    Ok(xml)
}

fn media_type_for_name(name: &str) -> Option<&'static str> {
    match Path::new(name)
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("jpg" | "jpeg") => Some("image/jpeg"),
        Some("png") => Some("image/png"),
        Some("gif") => Some("image/gif"),
        Some("bmp") => Some("image/bmp"),
        Some("tif" | "tiff") => Some("image/tiff"),
        Some("webp") => Some("image/webp"),
        Some("emf") => Some("image/emf"),
        Some("wmf") => Some("image/wmf"),
        _ => None,
    }
}

fn validate_file_size(path: &Path, maximum_bytes: u64) -> Result<(), DocumentError> {
    let actual_bytes = std::fs::metadata(path)?.len();
    if actual_bytes > maximum_bytes {
        return Err(DocumentError::InputTooLarge {
            actual_bytes,
            maximum_bytes,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn docx_extraction_preserves_paragraph_boundaries() {
        let cursor = Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file(
                "word/document.xml",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer
            .write_all(
                br#"<w:document xmlns:w="urn:test"><w:body><w:p><w:r><w:t>First</w:t></w:r></w:p><w:p><w:r><w:t>Second</w:t></w:r></w:p></w:body></w:document>"#,
            )
            .unwrap();
        let bytes = writer.finish().unwrap().into_inner();

        let sections = extract_docx_sections_from_bytes(&bytes).unwrap();

        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].text, "First");
        assert_eq!(sections[1].text, "Second");
    }

    #[test]
    fn docx_extraction_rejects_expanded_xml_beyond_the_limit() {
        let cursor = Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file(
                "word/document.xml",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer
            .write_all(br#"<w:document xmlns:w="urn:test"><w:body><w:p><w:r><w:t>Too large</w:t></w:r></w:p></w:body></w:document>"#)
            .unwrap();
        let bytes = writer.finish().unwrap().into_inner();

        let result = extract_docx_sections_from_bytes_with_limits(
            &bytes,
            DocumentLimits {
                max_input_bytes: bytes.len() as u64,
                max_docx_document_xml_bytes: 8,
                ..Default::default()
            },
        );

        assert!(matches!(
            result,
            Err(DocumentError::DocumentXmlTooLarge { .. })
        ));
    }

    #[test]
    fn malformed_docx_corpus_returns_errors_without_panicking() {
        let corpus: &[&[u8]] = &[
            b"",
            b"PK",
            b"not a zip archive",
            b"PK\x03\x04\x00\x00\x00\x00",
            &[0xff; 32],
        ];
        for input in corpus {
            let outcome = std::panic::catch_unwind(|| {
                extract_docx_sections_from_bytes_with_limits(input, DocumentLimits::default())
            });
            assert!(outcome.is_ok(), "malformed input must not panic");
        }
    }

    #[test]
    fn docx_table_and_image_extraction_are_bounded_and_return_real_payloads() {
        let cursor = Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file(
                "word/document.xml",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer
            .write_all(
                br#"<w:document xmlns:w="urn:test"><w:body><w:tbl><w:tr><w:tc><w:p><w:r><w:t>Header</w:t></w:r></w:p></w:tc><w:tc><w:p><w:r><w:t>Value</w:t></w:r></w:p></w:tc></w:tr></w:tbl></w:body></w:document>"#,
            )
            .unwrap();
        writer
            .start_file(
                "word/media/image1.png",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer.write_all(b"real embedded image bytes").unwrap();
        let bytes = writer.finish().unwrap().into_inner();

        let tables =
            extract_docx_tables_from_bytes_with_limits(&bytes, DocumentLimits::default()).unwrap();
        let images =
            extract_docx_images_from_bytes_with_limits(&bytes, DocumentLimits::default()).unwrap();

        assert_eq!(
            tables[0].rows,
            vec![vec!["Header".to_string(), "Value".to_string()]]
        );
        assert_eq!(images[0].media_type.as_deref(), Some("image/png"));
        assert_eq!(images[0].bytes, b"real embedded image bytes");
        assert!(matches!(
            extract_docx_images_from_bytes_with_limits(
                &bytes,
                DocumentLimits {
                    max_images: 0,
                    ..Default::default()
                }
            ),
            Err(DocumentError::FeatureLimitExceeded {
                feature: "images",
                maximum: 0
            })
        ));
    }

    #[test]
    fn pdf_table_heuristic_requires_clear_cell_boundaries() {
        assert_eq!(
            pdf_table_cells("Name  Value"),
            Some(vec!["Name".to_string(), "Value".to_string()])
        );
        assert_eq!(pdf_table_cells("one ordinary sentence"), None);
    }

    #[test]
    fn pdf_image_xobjects_are_extracted_with_bounded_encoded_bytes() {
        use lopdf::{Document, Object, Stream, dictionary};

        let path = std::env::current_dir()
            .unwrap()
            .join(format!("lakeprism-pdf-image-{}.pdf", std::process::id()));
        let mut document = Document::with_version("1.5");
        let pages_id = document.new_object_id();
        let image_id = document.add_object(Stream::new(
            dictionary! {
                "Type" => "XObject",
                "Subtype" => "Image",
                "Width" => 1,
                "Height" => 1,
                "ColorSpace" => "DeviceRGB",
                "BitsPerComponent" => 8,
                "Filter" => "DCTDecode",
            },
            vec![1, 2, 3],
        ));
        let resources_id = document.add_object(dictionary! {
            "XObject" => dictionary! { "Im0" => image_id },
        });
        let page_id = document.add_object(dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "Resources" => resources_id,
            "MediaBox" => vec![0.into(), 0.into(), 10.into(), 10.into()],
        });
        document.objects.insert(
            pages_id,
            Object::Dictionary(dictionary! {
                "Type" => "Pages",
                "Kids" => vec![page_id.into()],
                "Count" => 1,
            }),
        );
        let catalog_id = document.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });
        document.trailer.set("Root", catalog_id);
        document.save(&path).unwrap();

        let images = extract_pdf_images_with_limits(&path, DocumentLimits::default()).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].media_type.as_deref(), Some("image/jpeg"));
        assert_eq!(images[0].bytes, vec![1, 2, 3]);
    }
}
