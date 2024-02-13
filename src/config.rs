use std::path::PathBuf;

use clap::Args;
use serde::{Serialize, Deserialize};


#[derive(Debug, Args, Serialize, Deserialize)]
pub struct RunOneArgs {
    /// Path to write the output netCDF file containing the matched groups of soundings
    pub output_file: PathBuf,

    /// Path to the OCO-2 lite file to match up with OCO-3
    pub oco2_lite_file: PathBuf,
    
    /// Path to the OCO-3 lite file(s) to match up with OCO-2. You must specify at least one.
    #[clap(required = true)]
    pub oco3_lite_files: Vec<PathBuf>,
    
    /// Set this flag to only include good quality soundings when calculating the matches
    #[clap(short='0', long)]
    pub flag0_only: bool,

    /// Set this flag to indicate that we are looking for OCO-3 self crossings, rather than 
    /// OCO-2/-3 crossings. This will set a minimum time between soundings nearby to be
    /// considered a match and change the output variable names to reflect more accurately
    /// what is being searched for.
    #[clap(long)]
    pub oco3_self_cross: bool,

    /// Give this argument with a path to save a netCDF file containing an exact map of OCO-2 to OCO-3 soundings.
    /// Note: this can be 100s of MB
    #[clap(short='f', long)]
    pub save_full_matches_as: Option<PathBuf>,

    /// Give this argument with a path to a file written out with the --save-full-matches-as command to
    /// read in the full matches rather than calculating them from the OCO-2/3 lite files.
    #[clap(short='i', long)]
    pub read_full_matches: Option<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunMultiConfig {
    pub matchups: Vec<RunOneArgs>
}

#[derive(Debug, Args)]
pub struct RunMultiArgs {
    /// Path to the TOML configuration file that specifies how to run multiple matchups
    pub config_file: PathBuf
}