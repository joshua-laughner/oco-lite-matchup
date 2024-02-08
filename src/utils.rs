use std::{path::{PathBuf, Path}, io::Read, ops::{Add, AddAssign}};

use chrono::NaiveDate;
use ndarray::{Array1, ArrayView1, Ix1};

use crate::error::MatchupError;

/// Radius of Earth in kilometers
pub const EARTH_RADIUS_STD: f32 = 6378.137;

/// Converstion factor from degrees to radians.
pub const DEG2RAD: f32 = std::f32::consts::PI / 180.0;

// fn nc_file_name(ds: &netcdf::File) -> String {
//     ds.path()
//       .and_then(|p| Ok(p.display().to_string()))
//       .unwrap_or_else(|_| "?".to_owned())
// }

/// Get the netCDF file path associated with a [`netcdf::File`]
/// 
/// If the path cannot be determined, a path of "?" is returned. This function
/// is intended for use when a path is needed to display or for an error.
pub(crate) fn nc_file(ds: &netcdf::File) -> PathBuf {
    ds.path()
      .unwrap_or_else(|_| PathBuf::from("?"))
}


/// Load a 1D variable from an opened netCDF file
/// 
/// This reads the full variable named `varname` from `ds` and converts to a 1D
/// array. It returns an error if:
/// 
/// * the variable doesn't exist,
/// * reading the values form it fails (i.e. incompatible type), or
/// * the data cannot be made into a 1D array
/// 
/// Note that you may need to provide the type of the individual variable values
/// as a generic parameter, e.g.:
/// 
/// ```
/// let ds = netcdf::open("demo.nc").unwrap();
/// let data = load_nc_var::<f32>(ds, "sounding_id").unwrap();
/// ```
/// 
/// If there isn't enough information for the compiler to infer the type of the
/// values returned, the `::<f32>` or other type annotation will be needed.
/// 
/// # See also
/// [`load_nc_var_from_file`] - opens the netCDF file and loads the variable in one step.
pub fn load_nc_var<T: netcdf::NcPutGet>(ds: &netcdf::File, varname: &str) -> Result<ndarray::Array1<T>, MatchupError> {
    let file = nc_file(ds);
    let var = ds.variable(varname)
        .ok_or_else(|| MatchupError::NetcdfMissingVar { file: Some(file.clone()), varname: varname.to_owned() })?;
    let data = var.values_arr::<T, _>(netcdf::extent::Extents::All)
        .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?
        .into_dimensionality::<Ix1>()
        .map_err(|e| MatchupError::from_shape_error(e, file.clone(), varname.to_owned()))?;
    Ok(data)
}

/// Load a 1D netcdf file given only a path to the netCDF file.
/// 
/// Has the same behavior as [`load_nc_var`] except it takes a path to the netCDF file
/// rather than the opened [`netcdf::File`].
pub fn load_nc_var_from_file<T: netcdf::NcPutGet>(file: &Path, varname: &str) -> Result<ndarray::Array1<T>, MatchupError> {
    let ds = netcdf::open(file)
        .map_err(|e| MatchupError::from_nc_error(e, file.to_owned()))?;
    load_nc_var(&ds, varname)
}

/// Read a string or string array attribute from a netCDF file, returning a default value if attribute cannot be read
/// 
/// # Parameters
/// * `nc_var` - handle to the netCDF variable from which to get the attribute
/// * `attr_name` - name of the attribute to read
/// * `default` - default value to return if the real value cannot be read.
/// 
/// # Returns
/// Returns the value of the attribute. If it was an array of strings, then they are joined
/// with newlines. If the attribute doesn't exist on `nc_var` or cannot be read, then the
/// `default` is returned. Returns an `Err` only if the attribute exists but is not a string type.
pub fn get_str_attr_with_default(nc_var: &netcdf::Variable, attr_name: &str, default: String) -> Result<String, MatchupError> {
    let nc_attr = if let Some(a) = nc_var.attribute(attr_name) {
        a
    }else{
        return Ok(default)
    };
    
    let value = if let Ok(v) = nc_attr.value() {
        v
    }else{
        return Ok(default)
    };

    match value {
        netcdf::AttrValue::Str(v) => Ok(v),
        netcdf::AttrValue::Strs(v) => {
            let s = v.join("\n");
            Ok(s)
        },
        _ => Err(MatchupError::NetcdfWrongAttrType { file: None, varname: nc_var.name(), attname: attr_name.to_owned(), expected: "String" })
    }
}

/// Write a 1D array to a netCDF file as a new variable
/// 
/// # Parameters
/// * `grp` - handle to the netCDF group into which the variable will be written
/// * `data` - array of data to write
/// * `name` - name to give the variables
/// * `dims` - a slice (of length 1) giving the dimension to assign to this variable
/// * `units` - a string to write as the "units" attribute, pass `None` to skip writing this attribute
/// * `description` - similar to "units" but for the "description" attribute
/// 
/// # Returns
/// An empty tuple if successful. An `Err` is returned if:
/// * the variable cannot be created (e.g. already exists),
/// * writing the values failes, or
/// * writing either attribute fails
pub fn write_nc_var<T: netcdf::NcPutGet>(
    grp: &mut netcdf::GroupMut, 
    data: ArrayView1<T>, 
    name: &str,
    dims: &[&str], 
    units: Option<&str>, 
    description: Option<&str>
) -> Result<(), MatchupError> {
    let mut var = grp.add_variable::<T>(name, dims)?;
    var.put_values(data.as_slice().unwrap(), netcdf::extent::Extents::All)?;

    if let Some(units) = units {
        var.add_attribute("units", units)?;
    }

    if let Some(description) = description {
        var.add_attribute("description", description)?;
    }

    Ok(())
}

/// Write a series of strings to a 1D string variable in a netCDF file.
/// 
/// # Parameters
/// * `grp` - handle to the netCDF group into which the variable will be written
/// * `data` - slice of strings to write
/// * `name` - name to give the variables
/// * `dims` - a slice (of length 1) giving the dimension to assign to this variable
/// * `units` - a string to write as the "units" attribute, pass `None` to skip writing this attribute
/// * `description` - similar to "units" but for the "description" attribute
/// 
/// # Returns
/// An empty tuple if successful. An `Err` is returned if:
/// * the variable cannot be created (e.g. already exists),
/// * writing the values failes, or
/// * writing either attribute fails
pub fn write_string_nc_var<T: AsRef<str>>(
    grp: &mut netcdf::GroupMut,
    data: &[T],
    name: &str,
    dim: &str,
    units: Option<&str>,
    description: Option<&str>
) -> Result<(), MatchupError> {

    if let Some(d) = grp.dimension(dim) {
        if d.len() != data.len() {
            return Err(MatchupError::InternalError(format!("Inconsistent length between dimension '{dim}' and data passed for variable '{name}'")));
        }
    } else {
        grp.add_dimension(dim, data.len())?;
    }

    let mut var = grp.add_string_variable(name, &[dim])?;
    for (i, s) in data.iter().enumerate() {
        var.put_string(s.as_ref(), i)?;
    }

    if let Some(units) = units {
        var.add_attribute("units", units)?;
    }

    if let Some(description) = description {
        var.add_attribute("description", description)?;
    }

    Ok(())
}

/// Return an array that has only good-quality OCO-2/3 data
/// 
/// Given a 1D array `arr` representing a variable in an OCO lite file and an array
/// `flags` with the same shape that contains the quality flags, this will return a
/// new owned array with only the values from `arr` where `flags` is 0.
pub fn filter_by_quality<T: Copy>(arr: ArrayView1<T>, flags: ArrayView1<u8>) -> Array1<T> {
    let it = arr.into_iter()
                 .zip(flags.iter())
                 .filter_map(|(v, f)| {
                    if *f == 0 {
                        Some(*v)
                    }else{
                        None
                    }
                 });
    Array1::<T>::from_iter(it)
}

/// Calculate the great circle distance in kilometers between two locations on Earth.
pub fn great_circle_distance(lon1: f32, lat1: f32, lon2: f32, lat2: f32) -> f32 {
    let lon1 = lon1 * DEG2RAD;
    let lat1 = lat1 * DEG2RAD;
    let lon2 = lon2 * DEG2RAD;
    let lat2 = lat2 * DEG2RAD;

    let dlon = (lon2 - lon1).abs();
    let dlat = (lat2 - lat1).abs();

    let inner = ((dlat/2.0).sin()).powi(2) + (1.0 - (dlat/2.0).sin().powi(2) - ((lat1 + lat2)/2.0).sin().powi(2)) * (dlon/2.0).sin().powi(2);
    let central_angle = 2.0 * inner.sqrt().asin();
    central_angle * EARTH_RADIUS_STD
}

/// Calculate the SHA256 checksum of a file.
/// 
/// The checksum is returned as a string of hexadecimal digits (i.e. the
/// same form as the command line program `sha256sum`). This returns an
/// error if the file cannot be opened or an error occurs while reading
/// any chunk of the file.
/// 
/// # Notes
/// This reads in 10 MB of the file at a time to avoid using up too much
/// memory.
pub fn file_sha256(file: &Path) -> std::io::Result<String> {
    use sha2::Digest;

    let f = std::fs::File::open(file)?;
    let mut reader = std::io::BufReader::new(f);
    let mut buffer = Vec::new();
    buffer.resize(10_000_000, 0);
    let mut hasher = sha2::Sha256::new();

    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[0..n]);
    }

    let checksum = hex::encode(hasher.finalize());
    Ok(checksum)
}

/// A structure used to compute a mean of values provided in sequence.
#[derive(Debug, Clone, Copy)]
pub struct RunningMean<F: num_traits::Float + num_traits::NumAssign> {
    val: F,
    weight: F
}

impl<F: num_traits::Float + num_traits::NumAssign> RunningMean<F> {
    /// Create a new [`RunningMean`] with no values. Same as `RunningMean::default()`.
    pub fn new() -> Self {
        Self { val: F::zero(), weight: F::zero() }
    }

    /// Create a new [`RunningMean`] initialized with a slice of values.
    /// 
    /// That is, if the mean is calculated immediately after creating this
    /// instance, it will be the mean of the slice.
    pub fn from_slice(s: &[F]) -> Self {
        let mut me = Self::new();
        for &v in s {
            me.add_value(v);
        }
        me
    }

    /// Add a new value to the running mean with a weight of 1.
    pub fn add_value(&mut self, v: F) {
        self.val += v;
        self.weight += F::one();
    }

    /// Add a new value to the running mean with a custom weight.
    pub fn add_value_with_weight(&mut self, v: F, w: F) {
        self.val += v;
        self.weight += w;
    }

    /// Return the current mean of the values.
    /// 
    /// If the total weight is 0 (i.e. no values had been added), returns a `None`.
    pub fn mean(&self) -> Option<F> {
        if self.weight.is_zero() {
            None
        }else{
            Some(self.val / self.weight)
        }
    }
}

impl <F: num_traits::Float + num_traits::NumAssign> Default for RunningMean<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: num_traits::Float + num_traits::NumAssign> Add for RunningMean<F> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self { val: self.val + rhs.val, weight: self.weight + rhs.weight }
    }
}

impl<F: num_traits::Float + num_traits::NumAssign> AddAssign for RunningMean<F> {
    fn add_assign(&mut self, rhs: Self) {
        self.val += rhs.val;
        self.weight += rhs.weight;
    }
}

/// An enum that determines how progress is displayed.
#[derive(Debug, Clone)]
pub enum ShowProgress {
    /// A progress bar will be created and displayed
    Yes,
    /// A progress bar will not be shown
    No,
    /// A progress bar will be shown as part of a multi-progress bar.
    Multi(std::sync::Arc<indicatif::MultiProgress>)
}

impl ShowProgress {
    /// Print out a message to the screen without messing up any running multi-progress bars.
    pub fn println<I: AsRef<str>>(&self, msg: I) {
        match self {
            ShowProgress::Multi(mbar) => { let _ = mbar.println(msg); },
            _ => println!("{}", msg.as_ref())
        };
    }
}

pub fn sid_to_date(sid: u64) -> Option<NaiveDate> {
    let sid = format!("{}", sid);
    NaiveDate::parse_from_str(&sid[..8], "%Y%m%d").ok()
}