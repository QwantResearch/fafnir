extern crate bragi;
extern crate fafnir;
extern crate hyper;
extern crate mimir;
extern crate mimirsbrunn;
extern crate postgres;
extern crate rs_es;
extern crate serde_json;
extern crate slog;
#[macro_use]
extern crate slog_scope;
#[macro_use]
extern crate approx;

pub mod docker_wrapper;
pub mod fafnir_tests;

use crate::docker_wrapper::*;
use hyper::client::response::Response;
use mimir::rubber::{IndexSettings, IndexVisibility};
use postgres::row;
use postgres::{tls, Client};
use serde_json::value::Value;
use std::process::Command;

// Dataset name used for tests.
static DATASET: &'static str = "test";

pub struct PostgresWrapper<'a> {
    docker_wrapper: &'a PostgresDocker,
}

impl<'a> PostgresWrapper<'a> {
    pub fn host(&self) -> String {
        self.docker_wrapper.host()
    }

    pub fn get_conn(&self) -> Client {
        Client::connect(
            &format!("postgres://test@{}/test", &self.host()),
            tls::NoTls,
        )
        .unwrap_or_else(|err| {
            panic!(
                "Unable to connect to postgres: {} with ip: {}",
                err,
                &self.host()
            );
        })
    }

    pub fn get_rows(&self, table: &str) -> Vec<row::Row> {
        let mut conn = self.get_conn();
        conn.query(&*format!("SELECT * FROM {}", table), &[])
            .unwrap()
    }

    pub fn new(docker_wrapper: &PostgresDocker) -> PostgresWrapper {
        let pg_wrapper = PostgresWrapper {
            docker_wrapper: docker_wrapper,
        };
        pg_wrapper
    }
}

/// Code below comes from https://github.com/CanalTP/mimirsbrunn/tree/master/tests
trait ToJson {
    fn to_json(self) -> Value;
}

impl ToJson for Response {
    fn to_json(self) -> Value {
        match serde_json::from_reader(self) {
            Ok(v) => v,
            Err(e) => {
                panic!("could not get json value from response: {:?}", e);
            }
        }
    }
}

pub struct ElasticSearchWrapper<'a> {
    docker_wrapper: &'a ElasticsearchDocker,
    pub rubber: mimir::rubber::Rubber,
}

impl<'a> ElasticSearchWrapper<'a> {
    pub fn index<I, T>(&mut self, dataset: &str, objects: I)
    where
        T: mimir::MimirObject + std::marker::Send + 'static,
        I: Iterator<Item = T>,
    {
        let index_settings = IndexSettings {
            nb_shards: 1,
            nb_replicas: 0,
        };
        let index = self.rubber.make_index(dataset, &index_settings).unwrap();
        let _nb = self.rubber.bulk_index(&index, objects).unwrap();
        self.rubber
            .publish_index(dataset, index, IndexVisibility::Public)
            .unwrap();
        self.refresh();
    }

    pub fn get_pois(&mut self) -> Vec<mimir::Poi> {
        self.rubber.get_all_objects_from_index(&"test").unwrap()
    }

    pub fn host(&self) -> String {
        self.docker_wrapper.host()
    }

    pub fn init(&mut self) {
        self.rubber.delete_index(&"_all".to_string()).unwrap();
    }

    pub fn refresh(&self) {
        info!("Refreshing ES indexes");

        let res = hyper::client::Client::new()
            .get(&format!("{}/_refresh", self.host()))
            .send()
            .unwrap();
        assert!(res.status == hyper::Ok, "Error ES refresh: {:?}", res);
    }

    pub fn new(docker_wrapper: &ElasticsearchDocker) -> ElasticSearchWrapper {
        let mut es_wrapper = ElasticSearchWrapper {
            docker_wrapper: docker_wrapper,
            rubber: mimir::rubber::Rubber::new(&docker_wrapper.host()),
        };
        es_wrapper.init();
        es_wrapper
    }

    /// simple search on an index
    /// assert that the result is OK and transform it to a json Value
    pub fn search(&self, word: &str) -> serde_json::Value {
        let res = self
            .rubber
            .get(&format!("munin/_search?q={}&size=100", word))
            .unwrap();
        assert!(res.status().is_success());
        res.json().expect("failed to parse json")
    }

    pub fn search_on_global_stop_index(&self, word: &str) -> serde_json::Value {
        let res = self
            .rubber
            .get(&format!("munin_global_stops/_search?q={}", word))
            .unwrap();
        assert!(res.status().is_success());
        res.json().expect("failed to parse json")
    }

    pub fn search_and_filter<'b, F>(
        &self,
        word: &str,
        predicate: F,
    ) -> Box<dyn Iterator<Item = mimir::Place> + 'b>
    where
        F: 'b + FnMut(&mimir::Place) -> bool,
    {
        self.search_and_filter_on_index(word, predicate, false)
    }

    fn search_and_filter_on_index<'b, F>(
        &self,
        word: &str,
        predicate: F,
        search_on_global_stops: bool,
    ) -> Box<dyn Iterator<Item = mimir::Place> + 'b>
    where
        F: 'b + FnMut(&mimir::Place) -> bool,
    {
        use serde_json::map::{Entry, Map};
        fn into_object(json: Value) -> Option<Map<String, Value>> {
            match json {
                Value::Object(o) => Some(o),
                _ => None,
            }
        }
        fn get(json: Value, key: &str) -> Option<Value> {
            into_object(json).and_then(|mut json| match json.entry(key.to_string()) {
                Entry::Occupied(o) => Some(o.remove()),
                _ => None,
            })
        }
        let json = if search_on_global_stops {
            self.search_on_global_stop_index(word)
        } else {
            self.search(word)
        };
        get(json, "hits")
            .and_then(|json| get(json, "hits"))
            .and_then(|hits| {
                match hits {
                    Value::Array(v) => {
                        Some(Box::new(v
                            .into_iter()
                            .filter_map(into_object)
                            .filter_map(|obj| obj
                                .get("_type")
                                .and_then(|doc_type| doc_type.as_str())
                                .map(|doc_type| doc_type.into())
                                .and_then(|doc_type: String| {
                                    // The real object is contained in the _source section.
                                    obj.get("_source").and_then(|src| {
                                        let v = src.clone();
                                        match doc_type.as_ref() {
                                            "addr" => convert(v, mimir::Place::Addr),
                                            "street" => convert(v, mimir::Place::Street),
                                            "admin" => convert(v, mimir::Place::Admin),
                                            "poi" => convert(v, mimir::Place::Poi),
                                            "stop" => convert(v, mimir::Place::Stop),
                                            _ => {
                                                panic!("unknown ES return value, _type field = {}", doc_type);
                                            }
                                        }
                                    })
                                })
                            )
                            .filter(predicate),
                        ) as Box<dyn Iterator<Item = mimir::Place>>)
                    }
                    _ => None,
                }
            })
            .unwrap_or(Box::new(None.into_iter()) as Box<dyn Iterator<Item = mimir::Place>>)
    }
}

fn convert<T>(v: serde_json::Value, f: fn(T) -> mimir::Place) -> Option<mimir::Place>
where
    for<'de> T: serde::Deserialize<'de>,
{
    serde_json::from_value::<T>(v)
        .map_err(|err| warn!("Impossible to load ES result: {}", err))
        .ok()
        .map(f)
}

fn launch_and_assert(
    cmd: &'static str,
    args: Vec<std::string::String>,
    es_wrapper: &ElasticSearchWrapper,
) {
    let mut command = Command::new(cmd);
    command.args(&args).env("RUST_BACKTRACE", "1");
    let output = command.output().unwrap();

    if !output.status.success() {
        eprintln!("=== stdout for {}", cmd);
        eprintln!("{}", String::from_utf8(output.stdout).unwrap());
        eprintln!("=== stderr for {}", cmd);
        eprintln!("{}", String::from_utf8(output.stderr).unwrap());
        eprintln!("===");
        panic!("`{}` failed {}", cmd, output.status);
    }

    es_wrapper.refresh();
}

#[test]
fn main_test() {
    let _guard = mimir::logger_init();

    let mut el_docker = ElasticsearchDocker::new().unwrap();
    let pg_docker = PostgresDocker::new().unwrap();

    fafnir_tests::main_test(
        ElasticSearchWrapper::new(&mut el_docker),
        PostgresWrapper::new(&pg_docker),
    );
    fafnir_tests::bbox_test(
        ElasticSearchWrapper::new(&mut el_docker),
        PostgresWrapper::new(&pg_docker),
    );
    fafnir_tests::test_with_langs(
        ElasticSearchWrapper::new(&mut el_docker),
        PostgresWrapper::new(&pg_docker),
    );
    fafnir_tests::test_address_format(
        ElasticSearchWrapper::new(&mut el_docker),
        PostgresWrapper::new(&pg_docker),
    );
    fafnir_tests::test_current_country_label(
        ElasticSearchWrapper::new(&mut el_docker),
        PostgresWrapper::new(&pg_docker),
    );
}
