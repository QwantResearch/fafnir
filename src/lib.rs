extern crate fallible_iterator;
extern crate log;
extern crate mimir;
extern crate mimirsbrunn;
extern crate postgres;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate slog_scope;
extern crate itertools;
extern crate num_cpus;
extern crate par_map;
extern crate rayon;

use fallible_iterator::FallibleIterator;
use mimir::rubber::{IndexSettings, Rubber};
use mimir::{Coord, Poi, PoiType, Property};
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::utils::{format_international_poi_label, format_label};
use postgres::rows::Row;
use postgres::Connection;
use rayon::ThreadPoolBuilder;
use rayon::prelude::{ParallelIterator, ParallelSliceMut};
use std::collections::HashMap;
use std::sync::Arc;

const PG_BATCH_SIZE: i32 = 5000;

// constants that define POI weights for ranking
const WEIGHT_TAG_WIKIDATA: f64 = 100.0;
const WEIGHT_TAG_NAMES: [f64; 3] = [0.0, 30.0, 50.0];
const MAX_WEIGHT: f64 = WEIGHT_TAG_NAMES[2] + WEIGHT_TAG_WIKIDATA;

fn properties_from_row(row: &Row) -> Result<Vec<Property>, String> {
    let properties = row
        .get_opt::<_, HashMap<_, _>>("tags")
        .unwrap()
        .map_err(|err| {
            warn!("Unable to get tags: {:?}", err);
            err.to_string()
        })?
        .into_iter()
        .map(|(k, v)| Property {
            key: k,
            value: v.unwrap_or_else(|| "".to_string()),
        })
        .collect::<Vec<Property>>();

    Ok(properties)
}

fn build_names(
    langs: &[String],
    properties: &Vec<Property>,
) -> Result<mimir::I18nProperties, String> {
    const NAME_TAG_PREFIX: &str = "name:";

    let properties = properties
        .iter()
        .filter_map(|property| {
            if property.key.starts_with(&NAME_TAG_PREFIX) {
                let lang = property.key[NAME_TAG_PREFIX.len()..].to_string();
                if langs.contains(&lang) {
                    Some(mimir::Property {
                        key: lang,
                        value: property.value.to_string(),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    Ok(mimir::I18nProperties(properties))
}

fn build_poi_properties(
    row: &Row,
    name: &str,
    mut properties: Vec<Property>,
) -> Result<Vec<Property>, String> {
    let poi_subclass = row.get_opt("subclass").unwrap().map_err(|e| {
        warn!("impossible to get poi_subclass for {} because {}", name, e);
        e.to_string()
    })?;

    let poi_class = row.get_opt("class").unwrap().map_err(|e| {
        warn!("impossible to get poi_class for {} because {}", name, e);
        e.to_string()
    })?;

    properties.push(Property {
        key: "poi_subclass".to_string(),
        value: poi_subclass,
    });

    properties.push(Property {
        key: "poi_class".to_string(),
        value: poi_class,
    });

    Ok(properties)
}

/// Read the osm address tags and build a mimir address from them
///
/// For the moment we read mostly `addr:city` or `addr:country`
/// if available we also read `addr:postcode`
///
/// We also search for the admins that contains the coordinates of the poi
/// and add them as the address's admins.
///
/// For the moment we do not read `addr:city` or `addr:country` as it could
/// lead to inconsistency with the admins hierarchy
fn build_new_addr(
    addr_tag: &str,
    street_tag: &str,
    poi: &Poi,
    admins: Vec<Arc<mimir::Admin>>,
) -> mimir::Address {
    let postcode = poi
        .properties
        .iter()
        .find(|p| &p.key == "addr:postcode")
        .map(|p| p.value.clone());
    let postcodes = postcode.map_or(vec![], |p| vec![p]);
    let street_label = format_label(&admins, street_tag);
    let label = format!("{} {}", addr_tag, street_label);
    let addr_name = format!("{} {}", addr_tag, street_tag);
    let weight = admins.iter().find(|a| a.is_city()).map_or(0., |a| a.weight);
    mimir::Address::Addr(mimir::Addr {
        id: format!("addr_poi:{}", &poi.id),
        house_number: addr_tag.into(),
        name: addr_name,
        street: mimir::Street {
            id: format!("street_poi:{}", &poi.id),
            name: street_tag.to_string(),
            label: street_label,
            administrative_regions: admins,
            weight: weight,
            zip_codes: postcodes.clone(),
            coord: poi.coord.clone(),
            distance: None,
        },
        label: label,
        coord: poi.coord.clone(),
        weight: weight,
        zip_codes: postcodes,
        distance: None,
    })
}

fn find_address(
    poi: &Poi,
    geofinder: &AdminGeoFinder,
    rubber: &mut Rubber,
) -> Option<mimir::Address> {
    let osm_addr_tag = poi
        .properties
        .iter()
        .find(|p| &p.key == "addr:housenumber")
        .map(|p| &p.value);
    let osm_street_tag = poi
        .properties
        .iter()
        .find(|p| &p.key == "addr:street")
        .map(|p| &p.value);

    match (osm_addr_tag, osm_street_tag) {
        (Some(addr_tag), Some(street_tag)) => Some(build_new_addr(
            addr_tag,
            street_tag,
            poi,
            geofinder.get(&poi.coord),
        )),
        _ => rubber
            .get_address(&poi.coord, None) // No timeout value here for now.
            .ok()
            .and_then(|addrs| addrs.into_iter().next())
            .map(|addr| addr.address().unwrap()),
    }
}

fn locate_poi<'a>(
    poi: &'a mut Poi,
    geofinder: &AdminGeoFinder,
    rubber: &mut Rubber,
    langs: &[String],
) -> Option<&'a Poi> {
    let poi_address = find_address(&poi, geofinder, rubber);

    // if we have an address, we take the address's admin as the poi's admin
    // else we lookup the admin by the poi's coordinates
    let admins = poi_address
        .as_ref()
        .map(|a| match a {
            mimir::Address::Street(ref s) => s.administrative_regions.clone(),
            mimir::Address::Addr(ref s) => s.street.administrative_regions.clone(),
        })
        .unwrap_or_else(|| geofinder.get(&poi.coord));

    if admins.is_empty() {
        debug!("The poi {} is not on any admins", &poi.name);
        return None;
    }

    if poi_address.is_none() {
        debug!(
            "The poi {} doesn't have any address (admins: {:?})",
            &poi.name, &admins
        );
    }

    let zip_codes = match poi_address {
        Some(mimir::Address::Street(ref s)) => s.zip_codes.clone(),
        Some(mimir::Address::Addr(ref a)) => a.zip_codes.clone(),
        _ => vec![],
    };

    poi.administrative_regions = admins;
    poi.address = poi_address;
    poi.label = format_label(&poi.administrative_regions, &poi.name);
    poi.labels = format_international_poi_label(
        &poi.administrative_regions,
        &poi.names,
        &poi.name,
        &poi.label,
        langs,
    );
    poi.zip_codes = zip_codes;
    Some(poi)
}

fn build_poi(row: Row, langs: &[String]) -> Option<Poi> {
    let id = row.get("id");
    let name: String = row.get("name");

    let class: String = row.get("class");
    let subclass: String = row.get("subclass");

    let poi_type_id: String = format!("class_{}:subclass_{}", class, subclass);
    let poi_type_name: String = format!("class_{} subclass_{}", class, subclass);

    let lat = row
        .get_opt("lat")?
        .map_err(|e| warn!("impossible to get lat for {} because {}", name, e))
        .ok()?;
    let lon = row
        .get_opt("lon")?
        .map_err(|e| warn!("impossible to get lon for {} because {}", name, e))
        .ok()?;

    let poi_coord = Coord::new(lon, lat);

    if !poi_coord.is_valid() {
        // Ignore PoI if its coords from db are invalid.
        // Especially, NaN values may exist because of projection
        // transformations around poles.
        warn!("Got invalid coord for {} lon={},lat={}", id, lon, lat);
        return None;
    }

    let row_properties = properties_from_row(&row).unwrap_or_else(|_| vec![]);

    let names =
        build_names(langs, &row_properties).unwrap_or_else(|_| mimir::I18nProperties::default());

    let properties = build_poi_properties(&row, &name, row_properties).unwrap_or_else(|_| vec![]);

    // Add a weight if the "wikidata" tag exists for this POI
    let weight_wikidata = properties
        .iter()
        .find(|p| &p.key == "wikidata")
        .map_or(0., |_p| WEIGHT_TAG_WIKIDATA);

    // Count the number of tags "name:" for this POI.
    // The more tags, the more the POI is important
    let names_count = properties
        .iter()
        .filter(|p| p.key.starts_with("name:"))
        .count();

    // Depending on the number of tags "name" we choose different weights
    let weight_names = if names_count < 5 {
        WEIGHT_TAG_NAMES[0]
    } else if names_count < 9 {
        WEIGHT_TAG_NAMES[1]
    } else {
        WEIGHT_TAG_NAMES[2]
    };

    // The total weight for POI is simply the normalized sum of above weights
    let total_weight = (weight_names + weight_wikidata) / MAX_WEIGHT;

    Some(Poi {
        id: id,
        coord: poi_coord,
        poi_type: PoiType {
            id: poi_type_id,
            name: poi_type_name,
        },
        label: "".into(),
        administrative_regions: vec![],
        properties: properties,
        name,
        weight: total_weight,
        zip_codes: vec![],
        address: None,
        names,
        labels: mimir::I18nProperties::default(),
        distance: None,
    })
}

pub fn load_and_index_pois(
    es: String,
    conn: Connection,
    dataset: String,
    nb_threads: usize,
    bounding_box: Option<String>,
    nb_shards: usize,
    nb_replicas: usize,
    langs: Vec<String>,
) -> Result<(), mimirsbrunn::Error> {
    let rubber = &mut mimir::rubber::Rubber::new(&es);
    let admins = rubber.get_admins_from_dataset(&dataset).map_err(|err| {
        error!(
            "Administratives regions not found in es db for dataset {}.",
            dataset
        );
        err
    })?;
    let admins_geofinder = admins.into_iter().collect();

    let bbox_filter = bounding_box
        .map(|b| {
            format!(
                "and ST_MakeEnvelope({}, 4326) && st_transform(geometry, 4326)",
                b
            )
        })
        .unwrap_or_else(|| "".into());

    let query = format!(
        "
        SELECT id, lon, lat, class, name, tags, source, mapping_key, subclass FROM
        (
            SELECT
                geometry,
                global_id_from_imposm(osm_id) as id,
                st_x(st_transform(geometry, 4326)) as lon,
                st_y(st_transform(geometry, 4326)) as lat,
                poi_class(subclass, mapping_key) AS class,
                name,
                mapping_key,
                subclass,
                tags,
                'osm_poi_point' as source
                FROM osm_poi_point
                WHERE name <> ''
            UNION ALL
            SELECT
                geometry,
                global_id_from_imposm(osm_id) as id,
                st_x(st_transform(geometry, 4326)) as lon,
                st_y(st_transform(geometry, 4326)) as lat,
                poi_class(subclass, mapping_key) AS class,
                name,
                mapping_key,
                subclass,
                tags,
                'osm_poi_polygon' as source
                FROM osm_poi_polygon WHERE name <> ''
            UNION ALL
            SELECT
                geometry,
                global_id_from_imposm(osm_id) as id,
                st_x(st_transform(geometry, 4326)) as lon,
                st_y(st_transform(geometry, 4326)) as lat,
                'aerodrome' AS class,
                name,
                'aerodrome' as mapping_key,
                'airport' as subclass,
                tags,
                'osm_aerodrome_label_point' as source
                FROM osm_aerodrome_label_point WHERE name <> ''
        ) as unionall
        WHERE (unionall.mapping_key,unionall.subclass) not in
        (('highway','bus_stop'),
         ('barrier','gate'),
         ('amenity','waste_basket'),
         ('amenity','post_box'),
         ('tourism','information'),
         ('amenity','recycling'),
         ('barrier','lift_gate'),
         ('barrier','bollard'),
         ('barrier','cycle_barrier'),
         ('amenity','bicycle_rental'),
         ('tourism','artwork'),
         ('amenity','toilets'),
         ('leisure','playground'),
         ('amenity','telephone'),
         ('amenity','taxi'),
         ('leisure','pitch'),
         ('amenity','shelter'),
         ('barrier','sally_port'),
         ('barrier','stile'),
         ('amenity','ferry_terminal'),
         ('amenity','post_office'),
         ('railway','subway_entrance'),
         ('railway','train_station_entrance'))
         {}",
        bbox_filter
    );

    let stmt = conn.prepare(&query).unwrap();
    let trans = conn.transaction().unwrap();

    let rows = stmt.lazy_query(&trans, &[], PG_BATCH_SIZE).unwrap();

    let index_settings = IndexSettings {
        nb_shards: nb_shards,
        nb_replicas: nb_replicas,
    };

    rubber.initialize_templates()?;
    let poi_index = rubber.make_index(&dataset, &index_settings).unwrap();

    ThreadPoolBuilder::new().num_threads(nb_threads).build_global().expect("Failed to build thread pool");
    let mut rows = rows.iterator()
        .filter_map(|r| {
            r.map_err(|r| warn!("Impossible to load the row {:?}", r))
                .ok()
        })
        .filter_map(|p| {
            build_poi(p, &langs)
                .ok_or_else(|| warn!("Problem occurred in build_poi()"))
                .ok()
        })
        .collect::<Vec<_>>();
    {
        let poi_index = &poi_index;
        rows.par_chunks_mut(1000)
            .for_each(move |p| {
                let mut rub = Rubber::new(&es);
                let pois = p
                    .into_iter()
                    .filter_map(|poi| locate_poi(poi, &admins_geofinder, &mut rub, &langs));
                let mut rub2 = Rubber::new(&es);
                match rub2.bulk_index(&poi_index, pois) {
                    Err(e) => panic!("Failed to bulk insert pois because: {}", e),
                    Ok(nb) => info!("Nb of indexed pois: {}", nb),
                }
            });
    }

    rubber.publish_index(&dataset, poi_index).unwrap();
    Ok(())
}
