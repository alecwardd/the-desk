//! Text search over catalog field descriptors.

use super::types::{DeskCatalog, FieldDescriptor};

/// Case-insensitive substring search over field id, name, description, and domain.
pub fn search_catalog(catalog: &DeskCatalog, query: &str) -> Vec<FieldDescriptor> {
    let q = query.trim().to_ascii_lowercase();
    if q.is_empty() {
        return Vec::new();
    }
    catalog
        .fields
        .iter()
        .filter(|f| {
            f.id.to_ascii_lowercase().contains(&q)
                || f.name.to_ascii_lowercase().contains(&q)
                || f.description.to_ascii_lowercase().contains(&q)
                || f.domain_id.to_ascii_lowercase().contains(&q)
                || f.rust_field.to_ascii_lowercase().contains(&q)
        })
        .cloned()
        .collect()
}
