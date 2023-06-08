use std::path::{PathBuf, Path};

use clap::Parser;
use error::MatchupError;
use oco::OcoGeo;
use serde::Serialize;

mod error;
mod utils;
mod oco;

const MAX_DELTA_TIME_SECONDS: f64 = 43_200.0; // 12 hours

// TODO: Modify to accept multiple OCO-3 lite files
//  --> In principle this is mostly there, just need to tweak the group saving to store the file inds and test
//      that the extending an OcoGeo struct works correctly.
// TODO: Modify to accept multiple OCO-2 lite files (for different modes? not sure if needed)
fn main() -> Result<(), error::MatchupError> {
    let args = Args::parse();
    println!("Initializing thread pool with {} threads", args.nprocs);
    rayon::ThreadPoolBuilder::new().num_threads(args.nprocs).build_global().expect("Failed to set up the thread pool");

    let matched_soundings = if let Some(full_matches_in) = &args.read_full_matches {
        println!("Reading previous matched soundings from {}", full_matches_in.display());
        let ds = netcdf::open(full_matches_in)
            .map_err(|e| MatchupError::from_nc_error(e, full_matches_in.clone()))?;
        let grp = ds.group("matches")
            .map_err(|e| MatchupError::from_nc_error(e, full_matches_in.clone()))?
            .ok_or_else(|| MatchupError::NetcdfMissingGroup { file: Some(full_matches_in.clone()), grpname: "matches".to_owned() })?;
        oco::OcoMatches::from_nc_group(&grp)?
    } else {
        println!("Looking for matches between OCO-2 and -3");
        let full_matches = find_matches(&args.oco2_lite_file, args.get_all_oco3_files(), args.flag0_only)?;
        if let Some(full_match_file) = &args.save_full_matches_as {
            println!("Saving full match netCDF file: {}", full_match_file.display());
            full_matches.save_netcdf(&full_match_file)?;
        }
        full_matches.matches
    };

    println!("Grouping OCO-2 and -3 matches");
    matches_to_groups(matched_soundings, &args.output_file, &args.oco2_lite_file, &args.oco3_lite_file)?;
    println!("Done");
    Ok(())
}

fn find_matches(oco2_lite_file: &Path, oco3_lite_files: Vec<&Path>, flag0_only: bool) -> Result<Output, MatchupError> {
    let oco2_locs = oco::OcoGeo::load_lite_file(oco2_lite_file, flag0_only)?;
    let oco3_locs = oco3_lite_files.into_iter()
        .fold(Ok(OcoGeo::default()), |acc: Result<OcoGeo, MatchupError>, el| {
            let acc = acc?;
            let next_locs = oco::OcoGeo::load_lite_file(el, flag0_only)?;
            Ok(acc.extend(next_locs))
        })?;

    let matches = oco::match_oco3_to_oco2_parallel(&oco2_locs, &oco3_locs, 100.0, MAX_DELTA_TIME_SECONDS);
    Ok(Output {
        oco2_locations: oco2_locs,
        oco3_locations: oco3_locs,
        matches
    })
}

fn matches_to_groups(matched_soundings: oco::OcoMatches, nc_file: &Path, oco2_lite_file: &Path, oco3_lite_file: &Path) -> Result<(), MatchupError> {
    let groups = oco::identify_groups_from_matched_soundings(matched_soundings);
    let mut ds = netcdf::create(nc_file)
        .map_err(|e| MatchupError::from_nc_error(e, nc_file.to_owned()))?;
    groups.to_nc_group(&mut ds, None, oco2_lite_file, oco3_lite_file)?;
    Ok(())
}

#[derive(Debug, Parser)]
struct Args {
    /// Path to write the output netCDF file containing the matched groups of soundings
    output_file: PathBuf,

    /// Path to the OCO-2 lite file to match up with OCO-3
    oco2_lite_file: PathBuf,
    
    /// Path to the OCO-3 lite file for the same day as the OCO-2 one. In most cases, you should also
    /// specify --oco3-lite-file-before and --oco3-lite-file-after unless there is not an OCO-3 lite
    /// file for the day before or after.
    oco3_lite_file: PathBuf,

    /// Path to the OCO-3 lite file for the day before the OCO-2 one.
    #[clap(short='b', long)]
    oco3_lite_file_before: Option<PathBuf>,
    
    /// Path to the OCO-3 lite file for the day after the OCO-2 one.
    #[clap(short='a', long)]
    oco3_lite_file_after: Option<PathBuf>,
    
    /// Set this flag to only include good quality soundings when calculating the matches
    #[clap(short='0', long)]
    flag0_only: bool,

    /// Give this argument with a path to save a netCDF file containing an exact map of OCO-2 to OCO-3 soundings.
    /// Note: this can be 100s of MB
    #[clap(short='f', long)]
    save_full_matches_as: Option<PathBuf>,

    /// Give this argument with a path to a file written out with the --save-full-matches-as command to
    /// read in the full matches rather than calculating them from the OCO-2/3 lite files.
    #[clap(short='i', long)]
    read_full_matches: Option<PathBuf>,

    /// The number of processors to use for matching OCO-2 and OCO-3 soundings. The default is 8.
    #[clap(short='n', long, default_value="8")]
    nprocs: usize
}

impl Args {
    fn get_all_oco3_files<'a>(&'a self) -> Vec<&'a Path> {
        let mut files = Vec::new();
        if let Some(p) = &self.oco3_lite_file_before { files.push(p.as_path()); } 
        files.push(&self.oco3_lite_file);
        if let Some(p) = &self.oco3_lite_file_after { files.push(p.as_path()); }
        files
    }
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
