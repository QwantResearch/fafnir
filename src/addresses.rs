use itertools::Itertools;
use mimir::rubber::Rubber;
use mimir::Poi;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::labels::format_addr_name_and_label;
use mimirsbrunn::labels::format_street_label;
use mimirsbrunn::utils::find_country_codes;
use reqwest::StatusCode;
use serde::Deserialize;
use std::ops::Deref;
use std::sync::Arc;

// Prefixes used in ids for Address objects derived from OSM tags
const FAFNIR_ADDR_NAMESPACE: &str = "addr_poi:";
const FAFNIR_STREET_NAMESPACE: &str = "street_poi:";

/// Check if a mimir address originates from OSM data.
pub fn is_addr_derived_from_tags(addr: &mimir::Address) -> bool {
    match addr {
        mimir::Address::Addr(addr) => addr.id.starts_with(FAFNIR_ADDR_NAMESPACE),
        mimir::Address::Street(st) => st.id.starts_with(FAFNIR_STREET_NAMESPACE),
    }
}

pub enum CurPoiAddress {
    /// No address was searched yet for this POI
    NotFound,
    /// A search already was performed, but the result was empty
    None { coord: mimir::Coord },
    /// An address has already been found
    Some {
        coord: mimir::Coord,
        address: Box<mimir::Address>,
    },
}

/// Get current value of address associated with a POI in the ES database if
/// any, together with current coordinates of the POI that have been used to
/// perform a reverse
pub fn get_current_addr(rubber: &mut Rubber, poi_index: &str, osm_id: &str) -> CurPoiAddress {
    let query = format!(
        "{}/poi/{}/_source?_source_include=address,coord",
        poi_index, osm_id
    );

    #[derive(Deserialize)]
    struct FetchPOI {
        coord: mimir::Coord,
        address: Option<mimir::Address>,
    }

    rubber
        .get(&query)
        .map_err(|err| warn!("query to elasticsearch failed: {:?}", err))
        .ok()
        .and_then(|res| {
            if res.status() != StatusCode::NOT_FOUND {
                res.json()
                    .map_err(|err| {
                        warn!(
                            "failed to parse ES response while reading old address for {}: {:?}",
                            osm_id, err
                        )
                    })
                    .ok()
                    .map(|poi_json: FetchPOI| {
                        let coord = poi_json.coord;

                        if let Some(address) = poi_json.address {
                            CurPoiAddress::Some {
                                coord,
                                address: Box::new(address),
                            }
                        } else {
                            CurPoiAddress::None { coord }
                        }
                    })
            } else {
                None
            }
        })
        .unwrap_or(CurPoiAddress::NotFound)
}

fn build_new_addr(
    house_number_tag: &str,
    street_tag: &str,
    poi: &Poi,
    admins: Vec<Arc<mimir::Admin>>,
) -> mimir::Address {
    let postcodes = poi
        .properties
        .iter()
        .find(|p| ["addr:postcode", "contact:postcode"].contains(&p.key.as_str()))
        .map_or_else(
            || {
                admins
                    .iter()
                    .filter(|admin| admin.zip_codes.len() == 1)
                    .sorted_by_key(|admin| admin.zone_type)
                    .map(|admin| admin.zip_codes.clone())
                    .next()
                    .unwrap_or_else(Vec::new)
            },
            |p| vec![p.value.to_owned()],
        );
    let country_codes = find_country_codes(iter_admins(&admins));
    let street_label = format_street_label(street_tag, iter_admins(&admins), &country_codes);
    let (addr_name, addr_label) = format_addr_name_and_label(
        house_number_tag,
        street_tag,
        iter_admins(&admins),
        &country_codes,
    );
    let weight = admins.iter().find(|a| a.is_city()).map_or(0., |a| a.weight);
    if !house_number_tag.is_empty() {
        mimir::Address::Addr(mimir::Addr {
            id: format!("{}{}", FAFNIR_ADDR_NAMESPACE, &poi.id),
            house_number: house_number_tag.into(),
            name: addr_name,
            street: mimir::Street {
                id: format!("street_poi:{}", &poi.id),
                name: street_tag.to_string(),
                label: street_label,
                administrative_regions: admins,
                weight,
                zip_codes: postcodes.clone(),
                coord: poi.coord,
                country_codes: country_codes.clone(),
                ..Default::default()
            },
            label: addr_label,
            coord: poi.coord,
            approx_coord: poi.approx_coord.clone(),
            weight,
            zip_codes: postcodes,
            distance: None,
            country_codes,
            context: None,
        })
    } else {
        mimir::Address::Street(mimir::Street {
            id: format!("{}{}", FAFNIR_STREET_NAMESPACE, &poi.id),
            name: street_tag.to_string(),
            label: street_label,
            administrative_regions: admins,
            weight,
            zip_codes: postcodes,
            coord: poi.coord,
            country_codes,
            ..Default::default()
        })
    }
}

/// Build mimir Address from Poi,using osm address tags (if present)
/// or using reverse geocoding.
///
/// We also search for the admins that contains the coordinates of the poi
/// and add them as the address's admins.
///
/// If try_skip_reverse is set to try, it will reuse the address already
/// attached to a POI in the ES database.
pub fn find_address(
    poi: &Poi,
    geofinder: &AdminGeoFinder,
    rubber: &mut Rubber,
    poi_index: &str,
    try_skip_reverse: bool,
) -> Option<mimir::Address> {
    if poi
        .properties
        .iter()
        .any(|p| p.key == "poi_class" && p.value == "locality")
    {
        // We don't want to add address on hamlets.
        return None;
    }
    let osm_addr_tag = ["addr:housenumber", "contact:housenumber"]
        .iter()
        .filter_map(|k| {
            poi.properties
                .iter()
                .find(|p| &p.key == k)
                .map(|p| &p.value)
        })
        .next();

    let osm_street_tag = ["addr:street", "contact:street"]
        .iter()
        .filter_map(|k| {
            poi.properties
                .iter()
                .find(|p| &p.key == k)
                .map(|p| &p.value)
        })
        .next();

    match (osm_addr_tag, osm_street_tag) {
        (Some(house_number_tag), Some(street_tag)) => Some(build_new_addr(
            house_number_tag,
            street_tag,
            poi,
            geofinder.get(&poi.coord),
        )),
        (None, Some(street_tag)) => {
            if let Ok(addrs) = rubber.get_address(&poi.coord) {
                for addr in addrs.into_iter() {
                    if let Some(address) = addr.address() {
                        match address {
                            mimir::Address::Street(_) => continue,
                            mimir::Address::Addr(ref a) => {
                                if a.street.name != *street_tag {
                                    continue;
                                }
                            }
                        }
                        return Some(address);
                    }
                }
            }
            Some(build_new_addr(
                "",
                street_tag,
                poi,
                geofinder.get(&poi.coord),
            ))
        }
        _ => {
            if try_skip_reverse {
                // Fetch the address already attached to the POI to avoid computing an unnecessary
                // reverse.
                let changed_coords = |old_coord: mimir::Coord| {
                    (old_coord.lon() - poi.coord.lon()).abs() > 1e-6
                        || (old_coord.lat() - poi.coord.lat()).abs() > 1e-6
                };

                match get_current_addr(rubber, poi_index, &poi.id) {
                    CurPoiAddress::None { coord } if !changed_coords(coord) => return None,
                    CurPoiAddress::Some { coord, address }
                        if !is_addr_derived_from_tags(&address) && !changed_coords(coord) =>
                    {
                        return Some(*address);
                    }
                    _ => {}
                }
            }

            rubber
                .get_address(&poi.coord)
                .map_err(|e| warn!("`get_address` returned ES error for {}: {}", poi.id, e))
                .ok()
                .and_then(|addrs| addrs.into_iter().next())
                .map(|addr| {
                    addr.address()
                        .expect("`get_address` returned a non-address object")
                })
        }
    }
}

pub fn iter_admins(admins: &[Arc<mimir::Admin>]) -> impl Iterator<Item = &mimir::Admin> + Clone {
    admins.iter().map(|a| a.deref())
}
