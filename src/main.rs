use std::path::{PathBuf, Path};

use clap::Parser;
use error::MatchupError;
use serde::Serialize;

mod error;
mod utils;
mod oco;

// TODO: Try to sort the output of groups
// TODO: Store OCO-2/3 file paths and checksums in groups file
// TODO: Verify output of groups
// TODO: Modify to accept multiple OCO-3 lite files
// TODO: Modify to accept multiple OCO-2 lite files (for different modes)
fn main() -> Result<(), error::MatchupError> {
    let args = Args::parse();

    let matched_soundings = if let Some(full_matches_in) = &args.read_full_matches {
        let ds = netcdf::open(full_matches_in)
            .map_err(|e| MatchupError::from_nc_error(e, full_matches_in.clone()))?;
        let grp = ds.group("matches")
            .map_err(|e| MatchupError::from_nc_error(e, full_matches_in.clone()))?
            .ok_or_else(|| MatchupError::NetcdfMissingGroup { file: full_matches_in.clone(), grpname: "matches".to_owned() })?;
        oco::OcoMatches::from_nc_group(&grp)?
    } else {
        let full_matches = find_matches(&args.oco2_lite_file, &args.oco3_lite_file, args.flag0_only)?;
        if let Some(full_match_file) = &args.save_full_matches_as {
            full_matches.save_netcdf(&full_match_file)?;
        }
        full_matches.matches
    };


    matches_to_groups(matched_soundings, &args.output_file, &args.oco2_lite_file, &args.oco3_lite_file)?;
    Ok(())
}

fn find_matches(oco2_lite_file: &Path, oco3_lite_file: &Path, flag0_only: bool) -> Result<Output, MatchupError> {
    let oco2_locs = oco::OcoGeo::load_lite_file(oco2_lite_file, flag0_only)?;
    let oco3_locs = oco::OcoGeo::load_lite_file(oco3_lite_file, flag0_only)?;

    let matches = oco::match_oco3_to_oco2(&oco2_locs, &oco3_locs, 100.0);
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
    /// Path to the OCO-2 lite file to match up with OCO-3
    oco2_lite_file: PathBuf,
    
    /// Path to the OCO-3 lite file to match up with OCO-2
    oco3_lite_file: PathBuf,
    
    /// Path to write the output netCDF file containing the matched groups of soundings
    output_file: PathBuf,
    
    /// Set this flag to only include good quality soundings when calculating the matches
    #[clap(short='0', long="flag0-only")]
    flag0_only: bool,

    /// Give this argument with a path to save a netCDF file containing an exact map of OCO-2 to OCO-3 soundings.
    /// Note: this can be 100s of MB
    #[clap(short='f', long="save-full-matches-as")]
    save_full_matches_as: Option<PathBuf>,

    /// Give this argument with a path to a file written out with the --save-full-matches-as command to
    /// read in the full matches rather than calculating them from the OCO-2/3 lite files.
    #[clap(short='i', long="read-full-matches")]
    read_full_matches: Option<PathBuf>
}

#[derive(Debug, Serialize)]
struct Output {
    oco2_locations: oco::OcoGeo,
    oco3_locations: oco::OcoGeo,
    matches: oco::OcoMatches
}

impl Output {
    fn save_netcdf(&self, nc_file: &Path) -> Result<(), MatchupError> {
        let mut ds = netcdf::create(nc_file)
            .map_err(|e| MatchupError::from_nc_error(e, nc_file.to_owned()))?;

        let mut oco2_grp = ds.add_group("oco2_locations")
            .map_err(|e| MatchupError::from_nc_error(e, nc_file.to_owned()))?;
        self.oco2_locations.to_nc_group(&mut oco2_grp)?;

        let mut oco3_grp = ds.add_group("oco3_locations")
            .map_err(|e| MatchupError::from_nc_error(e, nc_file.to_owned()))?;
        self.oco3_locations.to_nc_group(&mut oco3_grp)?;

        let mut match_grp = ds.add_group("matches")
            .map_err(|e| MatchupError::from_nc_error(e, nc_file.to_owned()))?;
        self.matches.to_nc_group(&mut match_grp)?;

        Ok(())
    }
}
