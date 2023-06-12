use std::{path::{PathBuf, Path}, io::Write, str::FromStr, convert::Infallible, ffi::OsString};

use chrono::{NaiveDate, Duration};
use clap::Parser;
use itertools::Itertools;
use oco_lite_matchup::{error::MatchupError, config::{RunMultiConfig, RunOneArgs}};

fn main() -> Result<(), MatchupError> {
    let args = MainArgs::parse();
    let mut cfg = Vec::new();

    for (oco2_date, oco3_dates) in MatchupIter::new(args.start_date, args.end_date, args.ndays_buffer) {
        let oco2_dir = args.oco2_dir_structure.dir_for_date(oco2_date);
        let oco2_file = find_nc4_file(&oco2_dir)?;
        let oco2_file = if let Some(f) = oco2_file {
            f
        }else{
            eprintln!("Skipping matchup for {oco2_date} due to missing OCO-2 file");
            continue;
        };

        let oco3_files = oco3_dates.iter()
            .filter_map(|&d| {
                let oco3_dir = args.oco3_dir_structure.dir_for_date(d);
                find_nc4_file(&oco3_dir).transpose()
            }).collect::<Result<Vec<_>, _>>()?;
        if oco3_files.len() < oco3_dates.len() {
            eprintln!("Skipping matchup for {oco2_date} due to at least one missing OCO-3 file");
            continue;
        }

        let output_file = oco2_date.format(&args.outfile_format).to_string();

        let this_args = RunOneArgs {
            output_file: PathBuf::from(output_file),
            oco2_lite_file: oco2_file,
            oco3_lite_files: oco3_files,
            flag0_only: args.flag0_only,
            save_full_matches_as: None,
            read_full_matches: None,
        };

        cfg.push(this_args);

    }

    let cfg = RunMultiConfig{ matchups: cfg };
    let cfg_str = toml::to_string_pretty(&cfg)?;
    let mut f = std::fs::File::create(args.config_file)?;
    write!(f, "{}", cfg_str)?;
    Ok(())
}


struct MatchupIter {
    curr_date: NaiveDate,
    end_date: NaiveDate,
    ndays_buffer: u32
}

impl MatchupIter {
    fn new(start_date: NaiveDate, end_date: NaiveDate, ndays_buffer: u32) -> Self {
        Self { curr_date: start_date, end_date, ndays_buffer }
    }
}

impl Iterator for MatchupIter {
    type Item = (NaiveDate, Vec<NaiveDate>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_date > self.end_date {
            return None
        }

        let n = self.ndays_buffer as i64;
        let oco3_dates = (-n..=n)
            .map(|d| {
                let dur = Duration::days(d);
                self.curr_date + dur
            }).collect_vec();

        let tup = (self.curr_date, oco3_dates);
        self.curr_date += Duration::days(1);
        Some(tup)
    }
}

/// Create a TOML file appropriate to pass to the `multi` subcommand of oco-lite-matchup
#[derive(Debug, Parser)]
struct MainArgs {
    /// A string that gives the directory structure that OCO-2 files are found in. This can
    /// include format substrings recognized by chrono for date formatting; the most common 
    /// are %Y for four-digit year, %m for two-digit month, and %d for two-digit day. For 
    /// example, the string "/data/%Y/%m/%d/lite" indicates that the data are in year/month/day
    /// directories under "/data" with a "lite" subdirectory for each day directory. Note that 
    /// at present, this tool only supports directory structures where there is one .nc4 file
    /// per directory. See https://docs.rs/chrono/latest/chrono/format/strftime/index.html for
    /// the full list of chrono format specifiers.
    #[arg(value_parser = DirStructure::from_str)]
    oco2_dir_structure: DirStructure,

    /// Same as oco2_dir_structure, but for OCO-3 files.
    #[arg(value_parser = DirStructure::from_str)]
    oco3_dir_structure: DirStructure,

    /// First OCO-2 date to search for matchups, in YYYY-MM-DD format. The output config file 
    /// will contain one [[matchups]] section for each date between start_date and end_date (inclusive).
    start_date: NaiveDate,

    /// Last OCO-2 date to search for matchups, in YYYY-MM-DD format.
    end_date: NaiveDate,

    /// Number of days on either side of the OCO-2 file to include OCO-3 files from in the matchups.
    /// That is, 0 will only match OCO-3 data from the lite file with the same date's OCO-2 file, while
    /// a value of 1 will include 3 OCO-3 files (day before, same day, and day after the OCO-2 file).
    ndays_buffer: u32,

    /// Path to write the configuration file as.
    config_file: PathBuf,

    /// Pattern to use for the match output netCDF files. Date formatting patterns (e.g. %Y, %m, %d) 
    /// recognized by chrono can be used to insert the OCO-2 date in the file name.
    #[arg(default_value = "oco_lite_matches_%Y%m%d.nc4")]
    outfile_format: String,

    /// Use this flag to make the config only include good quality soundings when calculating the matches
    #[clap(short='0', long)]
    pub flag0_only: bool,
}

#[derive(Debug, Clone)]
struct DirStructure {
    pattern: String
}

impl Default for DirStructure {
    fn default() -> Self {
        Self { pattern: "%Y/%m/%d".to_owned() }
    }
}

impl FromStr for DirStructure {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { pattern: s.to_owned() })
    }
}

impl DirStructure {
    pub fn dir_for_date(&self, date: NaiveDate) -> PathBuf {
        PathBuf::from(date.format(&self.pattern).to_string())
    }
}



fn find_nc4_file(dir: &Path) -> Result<Option<PathBuf>, MatchupError> {
    let mut files = Vec::new();
    if !dir.exists() {
        eprintln!("Directory {} does not exist", dir.display());
        return Ok(None)
    }


    let nc4_ext = OsString::from("nc4");
    for entry in std::fs::read_dir(dir)? {
        let p = entry?.path();
        if p.extension() == Some(&nc4_ext) {
            files.push(p);
        }
    }

    if files.len() == 1 {
        Ok(Some(files.pop().unwrap()))
    } else if files.is_empty() {
        Ok(None)
    } else {
        Err(MatchupError::InternalError("Case of multiple .nc4 files in a single directory not implemented".to_owned()))
    }
}