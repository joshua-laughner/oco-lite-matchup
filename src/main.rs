use std::io::Read;
use std::path::Path;

use clap::{Parser, Subcommand};
use oco_lite_matchup::error::{self, MatchupError};
use oco_lite_matchup::config::{RunOneArgs, RunMultiArgs, RunMultiConfig};
use oco_lite_matchup::oco::{self, OcoGeo};
use rayon::prelude::*;
use serde::Serialize;



const MAX_DELTA_TIME_SECONDS: f64 = 43_200.0; // 12 hours

// TODO: Modify to accept multiple OCO-2 lite files (for different modes? not sure if needed)
// TODO: Make distance and time input parameters
fn main() -> Result<(), error::MatchupError> {
    let args = MainArgs::parse();
    println!("Initializing thread pool with {} threads", args.nprocs);
    rayon::ThreadPoolBuilder::new().num_threads(args.nprocs).build_global().expect("Failed to set up the thread pool");

    match args.command {
        Commands::One(subargs) => {
            driver_one_oco2_file(
                &subargs.oco2_lite_file, 
                &subargs.oco3_lite_files, 
                &subargs.output_file, 
                subargs.flag0_only, 
                subargs.save_full_matches_as.as_deref(), 
                subargs.read_full_matches.as_deref(),
                true
            )
        },

        Commands::Multi(subargs) => {
            let mut buf = String::new();
            let mut f = std::fs::File::open(&subargs.config_file)?;
            f.read_to_string(&mut buf)?;
            let cfg: RunMultiConfig = toml::from_str(&buf)?;
            driver_multi_oco2_file(&cfg.matchups)
        }
    }
    
}

fn driver_one_oco2_file<P: AsRef<Path>>(
    oco2_lite_file: &Path,
    oco3_lite_files: &[P],
    output_file: &Path,
    flag0_only: bool,
    save_full_matches_as: Option<&Path>,
    read_full_matches: Option<&Path>,
    show_progress: bool
) -> Result<(), MatchupError> {
    let matched_soundings = if let Some(full_matches_in) = read_full_matches {
        println!("Reading previous matched soundings from {}", full_matches_in.display());
        let ds = netcdf::open(full_matches_in)
            .map_err(|e| MatchupError::from_nc_error(e, full_matches_in.to_owned()))?;
        let grp = ds.group("matches")
            .map_err(|e| MatchupError::from_nc_error(e, full_matches_in.to_owned()))?
            .ok_or_else(|| MatchupError::NetcdfMissingGroup { file: Some(full_matches_in.to_owned()), grpname: "matches".to_owned() })?;
        oco::OcoMatches::from_nc_group(&grp)?
    } else {
        println!("Looking for matches between OCO-2 and -3");
        let full_matches = find_matches(oco2_lite_file, oco3_lite_files, flag0_only, show_progress)?;
        if let Some(full_match_file) = save_full_matches_as {
            println!("Saving full match netCDF file: {}", full_match_file.display());
            full_matches.save_netcdf(&full_match_file)?;
        }
        full_matches.matches
    };

    println!("Grouping OCO-2 and -3 matches");
    matches_to_groups(matched_soundings, output_file)?;
    println!("Done");
    Ok(())
}

fn driver_multi_oco2_file(matchups: &[RunOneArgs]) -> Result<(), MatchupError> {
    let errs: Vec<MatchupError> = matchups.par_iter()
        .filter_map(|m| {
            let res = driver_one_oco2_file(
                &m.oco2_lite_file, 
                &m.oco3_lite_files, 
                &m.output_file,
                m.flag0_only, 
                m.save_full_matches_as.as_deref(),
                m.read_full_matches.as_deref(),
                false
            );

            if let Err(e) = res {
                Some(e)
            } else {
                None
            }
        }).collect();  

    if errs.is_empty() {
        Ok(())
    } else {
        Err(MatchupError::MultipleErrors(errs))
    }
}

fn find_matches<P: AsRef<Path>>(oco2_lite_file: &Path, oco3_lite_files: &[P], flag0_only: bool, show_progress: bool) -> Result<Output, MatchupError> {
    let oco2_locs = oco::OcoGeo::load_lite_file(oco2_lite_file, flag0_only)?;
    let oco3_locs = oco3_lite_files.into_iter()
        .fold(Ok(OcoGeo::default()), |acc: Result<OcoGeo, MatchupError>, el| {
            let acc = acc?;
            let next_locs = oco::OcoGeo::load_lite_file(el.as_ref(), flag0_only)?;
            Ok(acc.extend(next_locs))
        })?;

    let n_oco3_files = oco3_locs.file_index.iter().max()
        .and_then(|&n| Some(n+1)).unwrap_or(0);
    println!("Comparing {} OCO-2 soundings to {} OCO-3 soundings across {} files", 
             oco2_locs.num_soundings(), oco3_locs.num_soundings(), n_oco3_files);

    let matches = oco::match_oco3_to_oco2_parallel(&oco2_locs, &oco3_locs, 100.0, MAX_DELTA_TIME_SECONDS, show_progress);
    Ok(Output {
        oco2_locations: oco2_locs,
        oco3_locations: oco3_locs,
        matches
    })
}

fn matches_to_groups(matched_soundings: oco::OcoMatches, nc_file: &Path) -> Result<(), MatchupError> {
    let groups = oco::identify_groups_from_matched_soundings(matched_soundings);
    let mut ds = netcdf::create(nc_file)
        .map_err(|e| MatchupError::from_nc_error(e, nc_file.to_owned()))?;
    groups.to_nc_group(&mut ds, None)?;
    Ok(())
}

#[derive(Debug, Parser)]

struct MainArgs {
    /// The number of processors to use for matching OCO-2 and OCO-3 soundings.
    #[clap(short='n', long, default_value="8")]
    nprocs: usize,

    #[command(subcommand)]
    command: Commands
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run a matchup between a single OCO-2 file and one or more OCO-3 files
    One(RunOneArgs),
    /// Run a matchup between multiple OCO-2 files and their corresponding OCO-3 files
    /// as specified in a TOML file.
    Multi(RunMultiArgs)
}

#[derive(Debug, Serialize)]
struct Output {
    oco2_locations: oco::OcoGeo,
    oco3_locations: oco::OcoGeo,
    matches: oco::OcoMatches
}

impl Output {
    fn save_netcdf(&self, nc_file: &Path) -> Result<(), MatchupError> {
        println!("Creating netCDF file {}", nc_file.display());
        let mut ds = netcdf::create(nc_file)
            .map_err(|e| MatchupError::from_nc_error(e, nc_file.to_owned()))?;

        println!("Saving OCO-2 locations");
        let mut oco2_grp = ds.add_group("oco2_locations")
            .map_err(|e| MatchupError::from_nc_error(e, nc_file.to_owned()))?;
        self.oco2_locations.to_nc_group(&mut oco2_grp)?;

        println!("Saving OCO-3 locations");
        let mut oco3_grp = ds.add_group("oco3_locations")
            .map_err(|e| MatchupError::from_nc_error(e, nc_file.to_owned()))?;
        self.oco3_locations.to_nc_group(&mut oco3_grp)?;

        println!("Saving match groups");
        let mut match_grp = ds.add_group("matches")
            .map_err(|e| MatchupError::from_nc_error(e, nc_file.to_owned()))?;
        self.matches.to_nc_group(&mut match_grp)?;

        println!("Done saving full match file {}", nc_file.display());
        Ok(())
    }
}
