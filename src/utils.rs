use mimir::rubber::Rubber;

/// Get creation date of an index as a timestamp.
pub fn get_index_creation_date(rubber: &mut Rubber, index: &str) -> Option<u64> {
    let query = format!("/_cat/indices/{}?h=creation.date", index);

    rubber
        .get(&query)
        .map_err(|err| warn!("could not query ES: {:?}", err))
        .ok()
        .and_then(|res| {
            res.text()
                .map_err(|err| warn!("could not load ES response: {:?}", err))
                .ok()
        })
        .and_then(|text| {
            text.trim()
                .parse()
                .map_err(|err| warn!("invalid index creation timestamp: {:?}", err))
                .ok()
        })
}
