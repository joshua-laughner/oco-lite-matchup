use std::{path::{PathBuf, Path}, io::Write, str::FromStr, convert::Infallible, ffi::OsString};

use chrono::{NaiveDate, Duration};
use clap::Parser;
use itertools::Itertools;
use oco_lite_matchup::{error::MatchupError, config::{RunMultiConfig, RunOneArgs}};

fn main() -> Result<(), MatchupError> {
    let args = MainArgs::parse();
    let mut cfg = Vec::new();

    if args.second_dir_structure.is_none() && !args.oco3_self_cross {
        return Err(MatchupError::ArgumentError("May only omit --second-dir when the --oco3-self-cross flag is present".to_string()));
    }

    let second_dir_structure = args.second_dir_structure.unwrap_or_else(|| args.first_dir_structure.clone());
    for (first_date, second_dates) in MatchupIter::new(args.start_date, args.end_date, args.ndays_buffer) {
        let first_dir = args.first_dir_structure.dir_for_date(first_date);
        let first_file = find_nc4_file(&first_dir)?;
        let first_file = if let Some(f) = first_file {
            f
        }else{
            eprintln!("Skipping matchup for {first_date} due to missing OCO-2 file");
            continue;
        };

        let second_files = second_dates.iter()
            .filter_map(|&d| {
                let oco3_dir = second_dir_structure.dir_for_date(d);
                find_nc4_file(&oco3_dir).transpose()
            }).collect::<Result<Vec<_>, _>>()?;
        if second_files.len() < second_dates.len() {
            eprintln!("Skipping matchup for {first_date} due to at least one missing OCO-3 file");
            continue;
        }

        let output_file = first_date.format(&args.outfile_format).to_string();

        let this_args = RunOneArgs {
            output_file: PathBuf::from(output_file),
            oco2_lite_file: first_file,
            oco3_lite_files: second_files,
            flag0_only: args.flag0_only,
            oco3_self_cross: args.oco3_self_cross,
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
    /// A string that gives the directory structure that lite files are found in. This can
    /// include format substrings recognized by chrono for date formatting; the most common 
    /// are %Y for four-digit year, %m for two-digit month, and %d for two-digit day. For 
    /// example, the string "/data/%Y/%m/%d/lite" indicates that the data are in year/month/day
    /// directories under "/data" with a "lite" subdirectory for each day directory. Note that 
    /// at present, this tool only supports directory structures where there is one .nc4 file
    /// per directory. See https://docs.rs/chrono/latest/chrono/format/strftime/index.html for
    /// the full list of chrono format specifiers. Without --oco2-self-cross, this must be
    /// the directory structure for OCO-2 lite files. With --oco2-self-cross, this will be
    /// the OCO-3 lite file directory structure.
    #[arg(long="first-dir", value_parser = DirStructure::from_str)]
    first_dir_structure: DirStructure,

    /// If --oco3-self-cross is not specified, then this must be the OCO-3 lite file directory
    /// structure. In that case, it has the same format as --first-dir. If --oco3-self-cross
    /// is given, then this can be omitted.
    #[arg(long="second-dir", value_parser = DirStructure::from_str)]
    second_dir_structure: Option<DirStructure>,

    /// First OCO-2 date to search for matchups, in YYYY-MM-DD format. The output config file 
    /// will contain one [[matchups]] section for each date between start_date and end_date (inclusive).
    #[clap(long="start")]
    start_date: NaiveDate,

    /// Last OCO-2 date to search for matchups, in YYYY-MM-DD format.
    #[clap(long="end")]
    end_date: NaiveDate,

    /// Number of days on either side of the OCO-2 file to include OCO-3 files from in the matchups.
    /// That is, 0 will only match OCO-3 data from the lite file with the same date's OCO-2 file, while
    /// a value of 1 will include 3 OCO-3 files (day before, same day, and day after the OCO-2 file).
    #[clap(long="ndays")]
    ndays_buffer: u32,

    /// Path to write the configuration file as.
    #[clap(long="config-file")]
    config_file: PathBuf,

    /// Pattern to use for the match output netCDF files. Date formatting patterns (e.g. %Y, %m, %d) 
    /// recognized by chrono can be used to insert the OCO-2 date in the file name.
    #[arg(long="out-fmt", default_value = "oco_lite_matches_%Y%m%d.nc4")]
    outfile_format: String,

    /// Use this flag to make the config only include good quality soundings when calculating the matches
    #[clap(short='0', long)]
    pub flag0_only: bool,

    /// Use this option to indicate that we want to look for OCO-3 self crossings. This changes the match
    /// rules to avoid all points next to each other counting as a "match" and modifies the output format
    /// to reflect what's being matched. This also eliminates the need for the OCO3_DIR_STRUCTURE 
    #[clap(long)]
    pub oco3_self_cross: bool
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