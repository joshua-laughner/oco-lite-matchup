use std::{path::PathBuf, fmt::Display};

#[derive(Debug, thiserror::Error)]
pub enum MatchupError {
    NetcdfError{nc_error: netcdf::error::Error, file: Option<PathBuf>},
    NetcdfMissingGroup{file: Option<PathBuf>, grpname: String},
    NetcdfMissingVar{file: Option<PathBuf>, varname: String},
    NetcdfWrongAttrType{file: Option<PathBuf>, varname: String, attname: String, expected: &'static str},
    NetcdfShapeError{file: Option<PathBuf>, varname: String, nd_error: ndarray::ShapeError},
    IOError(std::io::Error),
    InternalError(String),
}

impl MatchupError {
    pub fn from_nc_error(nc_error: netcdf::error::Error, file: PathBuf) -> Self {
        Self::NetcdfError { nc_error, file: Some(file) }
    }

    pub fn from_shape_error(nd_error: ndarray::ShapeError, file: PathBuf, varname: String) -> Self {
        Self::NetcdfShapeError { file: Some(file), varname, nd_error }
    }

    pub fn set_file(self, p: PathBuf) -> Self {
        match self {
            MatchupError::NetcdfError { nc_error, file: _ } => Self::NetcdfError { nc_error, file: Some(p) },
            MatchupError::NetcdfMissingGroup { file: _, grpname } => Self::NetcdfMissingGroup { file: Some(p), grpname },
            MatchupError::NetcdfMissingVar { file: _, varname } => Self::NetcdfMissingVar { file: Some(p), varname },
            MatchupError::NetcdfWrongAttrType { file: _, varname, attname, expected } => Self::NetcdfWrongAttrType { file: Some(p), varname, attname, expected },
            MatchupError::NetcdfShapeError { file: _, varname, nd_error } => Self::NetcdfShapeError { file: Some(p), varname, nd_error },
            MatchupError::IOError(e) => Self::IOError(e),
            MatchupError::InternalError(s) => Self::InternalError(s),
        }
    }
}

impl Display for MatchupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MatchupError::NetcdfError { nc_error, file } => {
                if let Some(p) = file {
                    write!(f, "Error reading netCDF file {}: {nc_error}", p.display())
                } else {
                    write!(f, "Error reading netCDF file: {nc_error}")
                }
            },
            MatchupError::NetcdfMissingGroup { file, grpname } => {
                if let Some(p) = file {
                    write!(f, "No group named '{grpname}' in {}", p.display())
                } else {
                    write!(f, "No group named '{grpname}'")
                }
            },
            MatchupError::NetcdfMissingVar { file, varname } => {
                if let Some(p) = file {
                    write!(f, "No variable named '{varname}' in {}", p.display())
                } else {
                    write!(f, "No variable named '{varname}'")
                }
            },
            MatchupError::NetcdfWrongAttrType { file, varname, attname, expected } => {
                if let Some(p) = file {
                    write!(f, "Wrong type for attribute {attname} on variable {varname} in file {}: expected a {expected}", p.display())
                } else {
                    write!(f, "Wrong type for attribute {attname} on variable {varname}: expected a {expected}")
                }
            },
            MatchupError::NetcdfShapeError { file, varname, nd_error } => {
                if let Some(p) = file {
                    write!(f, "Error in shape of variable '{varname}' in {}: {nd_error}", p.display())
                } else {
                    write!(f, "Error in shape of variable '{varname}': {nd_error}")
                }
            },
            MatchupError::IOError(e) => write!(f, "Error reading a file: {e}"),
            MatchupError::InternalError(s) => write!(f, "Internal error in matchup code, cause: {s}"),
        }
    }
}

impl From<netcdf::error::Error> for MatchupError {
    fn from(value: netcdf::error::Error) -> Self {
        Self::NetcdfError { nc_error: value, file: None }
    }
}

impl From<std::io::Error> for MatchupError {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value)
    }
}