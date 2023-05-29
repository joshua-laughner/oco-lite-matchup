use std::{path::PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum MatchupError {
    #[error("Error reading netCDF file {file}: {nc_error}")]
    NetcdfError{nc_error: netcdf::error::Error, file: PathBuf},
    #[error("No group named '{grpname}' in {file}")]
    NetcdfMissingGroup{file: PathBuf, grpname: String},
    #[error("No variable named '{varname}' in {file}")]
    NetcdfMissingVar{file: PathBuf, varname: String},
    #[error("Wrong type for attribute {attname} on variable {varname} in file {file}: expected a {expected}")]
    NetcdfWrongAttrType{file: PathBuf, varname: String, attname: String, expected: &'static str},
    #[error("Error in shape of variable '{varname}' in {file}: {nd_error}")]
    NetcdfShapeError{file: PathBuf, varname: String, nd_error: ndarray::ShapeError},
    #[error("Internal error in matchup code, cause: {0}")]
    InternalError(String),
}

impl MatchupError {
    pub fn from_nc_error(nc_error: netcdf::error::Error, file: PathBuf) -> Self {
        Self::NetcdfError { nc_error, file }
    }

    pub fn from_shape_error(nd_error: ndarray::ShapeError, file: PathBuf, varname: String) -> Self {
        Self::NetcdfShapeError { file, varname, nd_error }
    }
}