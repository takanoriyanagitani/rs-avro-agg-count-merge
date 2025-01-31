use std::process::ExitCode;

use std::io;
use std::io::Read;

use std::fs::File;

use rs_avro_agg_count_merge::SCHEMA_SIZE_LIMIT_DEFAULT;

use rs_avro_agg_count_merge::stat::MergeConfig;
use rs_avro_agg_count_merge::stat::Stat;
use rs_avro_agg_count_merge::stat::CNT_NAME_DEFAULT;
use rs_avro_agg_count_merge::stat::KEY_NAME_DEFAULT;

use rs_avro_agg_count_merge::wtr::WriterConfig;

fn names_from_args() -> impl Iterator<Item = String> {
    std::env::args().skip(1)
}

fn env_val_by_key(key: &'static str) -> impl FnMut() -> Result<String, io::Error> {
    move || std::env::var(key).map_err(|e| io::Error::other(format!("invalid env var {key}: {e}")))
}

fn schema_filename() -> Result<String, io::Error> {
    env_val_by_key("ENV_SCHEMA_FILENAME")()
}

fn filename2string_limited(limit: u64) -> impl FnMut(String) -> Result<String, io::Error> {
    move |filename: String| {
        let f: File = File::open(filename)?;
        let mut taken = f.take(limit);
        let mut buf: String = String::new();
        taken.read_to_string(&mut buf)?;
        Ok(buf)
    }
}

fn schema_content() -> Result<String, io::Error> {
    let schema_filename: String = schema_filename()?;
    filename2string_limited(SCHEMA_SIZE_LIMIT_DEFAULT)(schema_filename)
}

fn merge_key() -> Result<String, io::Error> {
    env_val_by_key("ENV_MERGE_KEY_NAME")().or_else(|_| Ok(KEY_NAME_DEFAULT.into()))
}

fn merge_cnt() -> Result<String, io::Error> {
    env_val_by_key("ENV_MERGE_CNT_NAME")().or_else(|_| Ok(CNT_NAME_DEFAULT.into()))
}

fn merge_cfg() -> Result<MergeConfig, io::Error> {
    let key: String = merge_key()?;
    let cnt: String = merge_cnt()?;
    Ok(MergeConfig {
        keyname: key,
        cntname: cnt,
    })
}

fn wtr_cfg() -> Result<WriterConfig, io::Error> {
    let cfg: MergeConfig = merge_cfg()?;
    let schema: String = schema_content()?;
    Ok(WriterConfig { merge: cfg, schema })
}

fn args2names2stat2avro2stdout() -> Result<(), io::Error> {
    let wcfg: WriterConfig = wtr_cfg()?;

    let mcfg: MergeConfig = wcfg.merge.clone();
    let filenames = names_from_args();
    let stat: Stat = mcfg.filenames2stat(filenames)?;

    wcfg.stat2stdout(stat)
}

fn sub() -> Result<(), io::Error> {
    args2names2stat2avro2stdout()
}

fn main() -> ExitCode {
    sub().map(|_| ExitCode::SUCCESS).unwrap_or_else(|e| {
        eprintln!("{e}");
        ExitCode::FAILURE
    })
}
