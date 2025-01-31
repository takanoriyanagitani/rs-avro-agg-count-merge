use std::collections::BTreeMap;

use std::io;
use std::io::BufReader;
use std::io::Read;

use std::fs::File;

use apache_avro::types::Value;
use apache_avro::Reader;

pub const KEY_NAME_DEFAULT: &str = "key";
pub const CNT_NAME_DEFAULT: &str = "cnt";

#[derive(Default)]
pub struct Stat {
    raw: BTreeMap<String, i64>,
}

#[derive(Default)]
pub struct StatItem {
    pub key: String,
    pub cnt: i64,
}

pub fn value2count(v: &Value) -> Result<i64, io::Error> {
    match v {
        Value::Int(i) => Ok((*i).into()),
        Value::Long(i) => Ok(*i),
        Value::Float(i) => Ok((*i) as i64),
        Value::Double(i) => Ok((*i) as i64),
        Value::Union(_, bv) => {
            let v: &Value = bv;
            value2count(v)
        }
        _ => Err(io::Error::other(format!("invalid value type: {v:#?}"))),
    }
}

impl StatItem {
    pub fn from_record(
        keyname: &str,
        cntname: &str,
        r: Vec<(String, Value)>,
    ) -> Result<Self, io::Error> {
        let mut ret: StatItem = StatItem::default();
        for pair in r {
            let (key, val) = pair;

            match val {
                Value::String(s) => {
                    if key.eq(keyname) {
                        ret.key = s;
                    }
                }
                _ => {
                    if key.eq(cntname) {
                        let cnt: i64 = value2count(&val)?;
                        ret.cnt = cnt;
                    }
                }
            }
        }
        Ok(ret)
    }
}

impl IntoIterator for Stat {
    type Item = (String, i64);
    type IntoIter = std::collections::btree_map::IntoIter<String, i64>;

    fn into_iter(self) -> Self::IntoIter {
        self.raw.into_iter()
    }
}

impl Stat {
    pub fn into_merged<I>(mut self, pairs: I) -> Result<Self, io::Error>
    where
        I: Iterator<Item = Result<(String, i64), io::Error>>,
    {
        for rslt in pairs {
            let pair: (String, i64) = rslt?;
            let (key, cnt) = pair;
            match self.raw.get_mut(&key) {
                None => {
                    self.raw.insert(key, cnt);
                }
                Some(m) => {
                    *m += cnt;
                }
            }
        }
        Ok(self)
    }

    pub fn merge_records<I>(self, keyname: &str, cntname: &str, rows: I) -> Result<Self, io::Error>
    where
        I: Iterator<Item = Result<Vec<(String, Value)>, io::Error>>,
    {
        let pairs = rows.map(|rslt| {
            rslt.and_then(|v| StatItem::from_record(keyname, cntname, v).map(|s| (s.key, s.cnt)))
        });
        self.into_merged(pairs)
    }

    pub fn merge_values<I>(self, keyname: &str, cntname: &str, vals: I) -> Result<Self, io::Error>
    where
        I: Iterator<Item = Result<Value, io::Error>>,
    {
        let rows = vals.map(|rslt| {
            rslt.and_then(|v| match v {
                Value::Record(row) => Ok(row),
                _ => Err(io::Error::other("invalid value type")),
            })
        });
        self.merge_records(keyname, cntname, rows)
    }

    pub fn merge_avro<R>(
        self,
        keyname: &str,
        cntname: &str,
        rdr: Reader<R>,
    ) -> Result<Self, io::Error>
    where
        R: Read,
    {
        self.merge_values(
            keyname,
            cntname,
            rdr.map(|rslt| rslt.map_err(io::Error::other)),
        )
    }

    pub fn merge_reader<R>(self, keyname: &str, cntname: &str, rdr: R) -> Result<Self, io::Error>
    where
        R: Read,
    {
        let br = BufReader::new(rdr);
        let r: Reader<_> = Reader::new(br).map_err(io::Error::other)?;
        self.merge_avro(keyname, cntname, r)
    }

    pub fn merge_file(self, keyname: &str, cntname: &str, f: File) -> Result<Self, io::Error> {
        self.merge_reader(keyname, cntname, f)
    }

    pub fn merge_avro_files<I>(
        self,
        keyname: &str,
        cntname: &str,
        mut filenames: I,
    ) -> Result<Self, io::Error>
    where
        I: Iterator<Item = String>,
    {
        filenames.try_fold(self, |state, filename| {
            let f: File = File::open(filename)?;
            state.merge_file(keyname, cntname, f)
        })
    }
}

#[derive(Clone)]
pub struct MergeConfig {
    pub keyname: String,
    pub cntname: String,
}

impl MergeConfig {
    pub fn filenames2stat<I>(&self, filenames: I) -> Result<Stat, io::Error>
    where
        I: Iterator<Item = String>,
    {
        Stat::default().merge_avro_files(&self.keyname, &self.cntname, filenames)
    }
}

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            keyname: KEY_NAME_DEFAULT.into(),
            cntname: CNT_NAME_DEFAULT.into(),
        }
    }
}
