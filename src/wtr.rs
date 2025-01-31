use std::io;

use std::io::BufWriter;
use std::io::Write;

use apache_avro::types::Record;
use apache_avro::Schema;
use apache_avro::Writer;

use crate::stat::MergeConfig;
use crate::stat::Stat;

pub fn record2writer<W>(r: Record, w: &mut Writer<W>) -> Result<(), io::Error>
where
    W: Write,
{
    w.append(r).map_err(io::Error::other)?;
    Ok(())
}

pub fn stat2avro2writer<W>(
    keyname: &str,
    cntname: &str,
    s: Stat,
    mut wtr: Writer<W>,
    schema: &Schema,
) -> Result<(), io::Error>
where
    W: Write,
{
    for pair in s.into_iter() {
        let (key, cnt) = pair;

        let orec: Option<Record> = Record::new(schema);
        let mut rec: Record = orec.ok_or_else(|| {
            io::Error::other(format!(
                "unable to create a record: the Option was None(key={key})"
            ))
        })?;

        rec.put(keyname, key);
        rec.put(cntname, cnt);
        record2writer(rec, &mut wtr)?;
    }
    wtr.flush().map_err(io::Error::other)?;
    Ok(())
}

pub struct WriterConfig {
    pub merge: MergeConfig,
    pub schema: String,
}

impl WriterConfig {
    pub fn keyname(&self) -> &str {
        &self.merge.keyname
    }

    pub fn cntname(&self) -> &str {
        &self.merge.cntname
    }

    pub fn schema_str(&self) -> &str {
        self.schema.as_str()
    }

    pub fn to_parsed_schema(&self) -> Result<Schema, io::Error> {
        Schema::parse_str(self.schema_str()).map_err(io::Error::other)
    }

    pub fn write_stat<W>(
        &self,
        stat: Stat,
        wtr: Writer<W>,
        schema: &Schema,
    ) -> Result<(), io::Error>
    where
        W: Write,
    {
        stat2avro2writer(self.keyname(), self.cntname(), stat, wtr, schema)
    }

    pub fn stat2writer<W>(&self, s: Stat, mut w: W) -> Result<(), io::Error>
    where
        W: Write,
    {
        let schema: Schema = self.to_parsed_schema()?;
        let wtr: Writer<_> = Writer::new(&schema, &mut w);
        self.write_stat(s, wtr, &schema)?;
        w.flush()
    }

    pub fn stat2stdout(&self, s: Stat) -> Result<(), io::Error> {
        let o = io::stdout();
        let mut ol = o.lock();
        let bw = BufWriter::new(&mut ol);
        self.stat2writer(s, bw)?;
        ol.flush()
    }
}
