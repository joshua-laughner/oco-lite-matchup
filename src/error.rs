use std::{path::PathBuf, fmt::Display};

/// The general error type used by this crate.
/// 
/// Several error variants can contain an optional path to a file related to the
/// error (i.e. a file being read or written to). If the path cannot be stored in
/// the error at the time of the error, this will be `None.
#[derive(Debug, thiserror::Error)]
pub enum MatchupError {
    /// A general error from reading or writing to a netCDF file. It contains
    /// the original netcdf crate error.
    NetcdfError{nc_error: netcdf::error::Error, file: Option<PathBuf>},

    /// An error to use if a group expected to exist in the netCDF file was missing.
    NetcdfMissingGroup{file: Option<PathBuf>, grpname: String},

    /// An error to use if a variable expected to exist in the netCDF file was missing.
    NetcdfMissingVar{file: Option<PathBuf>, varname: String},

    /// An error to use if trying to read an attribute from a netCDF file but it is of
    /// the wrong type (i.e. expected string and got a number)
    NetcdfWrongAttrType{file: Option<PathBuf>, varname: String, attname: String, expected: &'static str},

    /// An error to use if trying to read a variable from a netCDF file as an array with
    /// a specific number of dimensions, but it has the wrong number of dimensions.
    NetcdfShapeError{file: Option<PathBuf>, varname: String, nd_error: ndarray::ShapeError},

    /// An error variant wrapping an IO error for non-netCDF read/write errors.
    IOError(std::io::Error),

    /// An error variant indicating a problem parsing a configuration file.
    ConfigError(toml::de::Error),

    /// An error variant indicating a problem creating a configuration file.
    ConfigWriteError(toml::ser::Error),

    /// An error variant to use when an assumption about how different parts of this
    /// program work together is broken.
    InternalError(String),

    /// An error variant representing multiple instances of this error type, e.g. if
    /// running functions in parallel and >1 return different errors.
    MultipleErrors(Vec<Self>)
}

impl MatchupError {
    /// Create a` NetcdfError` variant from a [`netcdf::error::Error`] and a path to the
    /// netCDF file being read/written to.
    /// 
    /// Use this when you want to attach a netCDF file path to the error, otherwise you
    /// should use `?` or `.into()` on the netCDF error to create the [`MatchupError`]
    pub fn from_nc_error(nc_error: netcdf::error::Error, file: PathBuf) -> Self {
        Self::NetcdfError { nc_error, file: Some(file) }
    }

    /// Create a `NetcdfShapeError` varian from a [`ndarray::ShapeError`], a path to the
    /// netCDF file being read from, and the variable name being read. If you do not/can not
    /// want to include the path and variable, you can use the `?` shortcut.
    pub fn from_shape_error(nd_error: ndarray::ShapeError, file: PathBuf, varname: String) -> Self {
        Self::NetcdfShapeError { file: Some(file), varname, nd_error }
    }

    /// For variants that include a path to a file, set that path to `p`.
    /// 
    /// Useful when the error is raised at a lower level that does not know the file path and you
    /// want to attach the file path to the error from higher up in the call stack.
    pub fn set_file(self, p: PathBuf) -> Self {
        match self {
            MatchupError::NetcdfError { nc_error, file: _ } => Self::NetcdfError { nc_error, file: Some(p) },
            MatchupError::NetcdfMissingGroup { file: _, grpname } => Self::NetcdfMissingGroup { file: Some(p), grpname },
            MatchupError::NetcdfMissingVar { file: _, varname } => Self::NetcdfMissingVar { file: Some(p), varname },
            MatchupError::NetcdfWrongAttrType { file: _, varname, attname, expected } => Self::NetcdfWrongAttrType { file: Some(p), varname, attname, expected },
            MatchupError::NetcdfShapeError { file: _, varname, nd_error } => Self::NetcdfShapeError { file: Some(p), varname, nd_error },
            MatchupError::IOError(e) => Self::IOError(e),
            MatchupError::ConfigError(_) => self,
            MatchupError::ConfigWriteError(_) => self,
            MatchupError::InternalError(s) => Self::InternalError(s),
            MatchupError::MultipleErrors(_) => self
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
            MatchupError::ConfigError(e) => write!(f, "Error reading configuration: {e}"),
            MatchupError::ConfigWriteError(e) => write!(f, "Error writing configuration: {e}"),
            MatchupError::InternalError(s) => write!(f, "Internal error in matchup code, cause: {s}"),
            MatchupError::MultipleErrors(errs) => {
                writeln!(f, "{} matchups had errors. The errors were:", errs.len())?;
                for (i, e) in errs.iter().enumerate() {
                    writeln!(f, "{}. {e}", i+1)?;
                }
                Ok(())
            }
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

impl From<toml::de::Error> for MatchupError {
    fn from(value: toml::de::Error) -> Self {
        Self::ConfigError(value)
    }
}

impl From<toml::ser::Error> for MatchupError {
    fn from(value: toml::ser::Error) -> Self {
        Self::ConfigWriteError(value)
    }
}