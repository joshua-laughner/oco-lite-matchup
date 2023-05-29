use std::path::{PathBuf, Path};

use ndarray::{Array1, ArrayView1, Ix1};

use crate::error::MatchupError;

pub const EARTH_RADIUS_STD: f32 = 6378.137;
pub const DEG2RAD: f32 = std::f32::consts::PI / 180.0;

// fn nc_file_name(ds: &netcdf::File) -> String {
//     ds.path()
//       .and_then(|p| Ok(p.display().to_string()))
//       .unwrap_or_else(|_| "?".to_owned())
// }

pub(crate) fn nc_file(ds: &netcdf::File) -> PathBuf {
    ds.path()
      .unwrap_or_else(|_| PathBuf::from("?"))
}

pub fn load_nc_var<T: netcdf::NcPutGet>(ds: &netcdf::File, varname: &str) -> Result<ndarray::Array1<T>, MatchupError> {
    let file = nc_file(ds);
    let var = ds.variable(varname)
        .ok_or_else(|| MatchupError::NetcdfMissingVar { file: file.clone(), varname: varname.to_owned() })?;
    let data = var.values_arr::<T, _>(netcdf::extent::Extents::All)
        .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?
        .into_dimensionality::<Ix1>()
        .map_err(|e| MatchupError::from_shape_error(e, file.clone(), varname.to_owned()))?;
    Ok(data)
}

pub fn load_nc_var_from_file<T: netcdf::NcPutGet>(file: &Path, varname: &str) -> Result<ndarray::Array1<T>, MatchupError> {
    let ds = netcdf::open(file)
        .map_err(|e| MatchupError::from_nc_error(e, file.to_owned()))?;
    load_nc_var(&ds, varname)
}

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
        _ => Err(MatchupError::NetcdfWrongAttrType { file: PathBuf::from("?"), varname: nc_var.name(), attname: attr_name.to_owned(), expected: "String" })
    }
}

pub fn write_nc_var<T: netcdf::NcPutGet>(
    grp: &mut netcdf::GroupMut, 
    data: ArrayView1<T>, 
    name: &str,
    dims: &[&str], 
    units: Option<&str>, 
    description: Option<&str>
) -> Result<(), MatchupError> {
    let file = PathBuf::from("output");

    let mut var = grp.add_variable::<T>(name, dims)
        .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
    var.put_values(data.as_slice().unwrap(), netcdf::extent::Extents::All)
        .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;

    if let Some(units) = units {
        var.add_attribute("units", units)
            .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
    }

    if let Some(description) = description {
        var.add_attribute("description", description)
            .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
    }

    Ok(())
}

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