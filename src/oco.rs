use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use indicatif::{ProgressBar, ProgressStyle};
use ndarray::{Array1, Ix1, Ix2};
use netcdf::extent::Extents;
use serde::Serialize;

use crate::error::MatchupError;
use crate::utils::{load_nc_var, write_nc_var, filter_by_quality, great_circle_distance, load_nc_var_from_file, get_str_attr_with_default, self};

#[derive(Debug, Serialize)]
pub struct OcoGeo {
    pub lite_file: PathBuf,
    pub longitude: Array1<f32>,
    pub latitude: Array1<f32>,
    pub quality: Array1<u8>
}

impl OcoGeo {
    pub fn load_lite_file(lite_file: &Path, flag0_only: bool) -> Result<Self, MatchupError> {
        let ds = netcdf::open(lite_file)
        .map_err(|e| MatchupError::from_nc_error(e, lite_file.to_owned()))?;
        
        let longitude = load_nc_var(&ds, "longitude")?;
        let latitude = load_nc_var(&ds, "latitude")?;
        let quality = load_nc_var(&ds, "xco2_quality_flag")?;

        if flag0_only {
            let longitude = filter_by_quality(longitude.view(), quality.view());
            let latitude = filter_by_quality(latitude.view(), quality.view());
            let quality = filter_by_quality(quality.view(), quality.view());
            Ok(OcoGeo { lite_file: lite_file.to_owned(), longitude, latitude, quality })
        }else{
            Ok(OcoGeo { lite_file: lite_file.to_owned(), longitude, latitude, quality })
        }

    }

    pub fn to_nc_group(&self, grp: &mut netcdf::GroupMut) -> Result<(), MatchupError> {
        grp.add_dimension("sounding", self.num_soundings() as usize)
            .map_err(|e| MatchupError::from_nc_error(e, PathBuf::from("output")))?;
        grp.add_attribute("source_lite_file", self.lite_file.display().to_string().as_str())
            .map_err(|e| MatchupError::from_nc_error(e, PathBuf::from("output")))?;
        write_nc_var(grp, self.longitude.view(), "longitude", &["sounding"], Some("degrees_east"), None)?;
        write_nc_var(grp, self.latitude.view(), "latitude", &["sounding"], Some("degrees_north"), None)?;
        write_nc_var(grp, self.quality.view(), "quality_flag", &["sounding"], None, Some("0 = good, 1 = bad"))?;

        Ok(())
    }

    pub fn num_soundings(&self) -> u64 {
        self.longitude.len() as u64
    }

    pub fn iter_latlon(&self) -> std::iter::Zip<ndarray::iter::Iter<'_, f32, Ix1>, ndarray::iter::Iter<'_, f32, Ix1>> {
        self.longitude.iter().zip(self.latitude.iter())
    }
}

#[derive(Debug, Serialize)]
pub struct OcoMatches {
    indices: HashMap<u64, Vec<u64>>,
    distances: HashMap<u64, Vec<f32>>
}

impl OcoMatches {
    fn oco2_index_varname() -> &'static str {
        "oco2_index"
    }

    fn oco3_index_varname() -> &'static str {
        "oco3_index"
    }

    fn dist_varname() -> &'static str {
        "distance"
    }

    pub fn from_nc_group(grp: &netcdf::Group) -> Result<Self, MatchupError> {
        let out_file = PathBuf::from("?");
        
        let oco2_ind_var = grp.variable(Self::oco2_index_varname())
            .ok_or_else(|| MatchupError::NetcdfMissingVar { file: out_file.clone(), varname: Self::oco2_index_varname().to_owned() })?;
        // let oco2_ind_fill_val = oco2_ind_var.fill_value()
        //     .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?
        //     .unwrap_or(u64::MAX);
        let oco2_inds = oco2_ind_var
            .values_arr::<u64, _>(netcdf::extent::Extents::All)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?
            .into_dimensionality::<Ix1>()
            .map_err(|e| MatchupError::from_shape_error(e, out_file.clone(), Self::oco2_index_varname().to_owned()))?;


        let oco3_ind_var = grp.variable(Self::oco3_index_varname())
            .ok_or_else(|| MatchupError::NetcdfMissingVar { file: out_file.clone(), varname: Self::oco3_index_varname().to_owned() })?;
        let oco3_ind_fill_val = oco3_ind_var.fill_value()
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?
            .unwrap_or(u64::MAX);
        let oco3_inds = oco3_ind_var
            .values_arr::<u64, _>(netcdf::extent::Extents::All)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?
            .into_dimensionality::<Ix2>()
            .map_err(|e| MatchupError::from_shape_error(e, out_file.clone(), Self::oco3_index_varname().to_owned()))?;

        let dist_var = grp.variable(Self::dist_varname())
            .ok_or_else(|| MatchupError::NetcdfMissingVar { file: out_file.clone(), varname: Self::dist_varname().to_owned() })?;
        let dist_fill_val = dist_var.fill_value()
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?
            .unwrap_or(f32::MIN);
        let dist_arr = dist_var
            .values_arr::<f32, _>(netcdf::extent::Extents::All)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?
            .into_dimensionality::<Ix2>()
            .map_err(|e| MatchupError::from_shape_error(e, out_file.clone(), Self::dist_varname().to_owned()))?;

        let mut indices = HashMap::new();
        let mut distances = HashMap::new();

        ndarray::Zip::from(&oco2_inds).and(oco3_inds.rows()).and(dist_arr.rows()).for_each(|&oco2i, oco3i, dist| {
            let oco3i: Vec<u64> = oco3i.iter().filter_map(|&v| if v != oco3_ind_fill_val {Some(v)} else {None} ).collect();
            let dist: Vec<f32> = dist.iter().filter_map(|&v| if v != dist_fill_val { Some(v) } else { None }).collect();
            indices.insert(oco2i, oco3i);
            distances.insert(oco2i, dist);
        });

        Ok(Self { indices, distances })
    }

    pub fn to_nc_group(&self, grp: &mut netcdf::GroupMut) -> Result<(), MatchupError> {
        let file = PathBuf::from("?");

        // Vlen types have weird lifetime issues, so we're doing 2D arrays.
        let n_oco2 = self.indices.len();
        if n_oco2 != self.distances.len() {
            return Err(MatchupError::InternalError(format!(
                "Inconsistent number of OCO-2 soundings in indices {} and distances {}", n_oco2, self.distances.len()
            )))
        }
        let max_oco3 = self.calc_match_dim()?;

        grp.add_dimension("oco2_match", n_oco2)
            .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
        grp.add_dimension("oco3_match", max_oco3)
            .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;

        let ordered_keys = Self::get_sorted_keys(&self.indices);
        if ordered_keys != Self::get_sorted_keys(&self.distances) {
            return Err(MatchupError::InternalError(
                "OcoMatches instance has inconsistent keys for `indices` and `distances`".to_owned()
            ))
        }

        write_nc_var(grp, ordered_keys.view(), Self::oco2_index_varname(), &["oco2_match"], None, 
                     Some("0-based index along the 'sounding_id' dimension of the OCO-2 lite file"))?;

        Self::write_variable(grp, &self.indices, ordered_keys.as_slice().unwrap(), Self::oco3_index_varname(), None, 
                             Some("0-based index along the 'sounding_id' dimension of the OCO-3 lite file"), u64::MAX)?;
        Self::write_variable(grp, &self.distances, ordered_keys.as_slice().unwrap(), Self::dist_varname(), Some("km"), 
                             Some("Great-circle distance between the OCO-2 and OCO-3 soundings"), f32::MIN)?;
        Ok(())
    }

    fn write_variable<T: netcdf::NcPutGet>(
        grp: &mut netcdf::GroupMut,
        data: &HashMap<u64, Vec<T>>,
        keys: &[u64],
        varname: &str,
        // shape: (usize, usize),
        units: Option<&str>,
        description: Option<&str>,
        fill_value: T
    ) -> Result<(), MatchupError>{
        let file = PathBuf::from("?");

        let mut var = grp.add_variable::<T>(varname, &["oco2_match", "oco3_match"])
            .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
        var.set_fill_value(fill_value)
            .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;

        for (i, k) in keys.iter().enumerate() {
            let row = data.get(k).ok_or_else(|| MatchupError::InternalError(
                format!("OcoMatches::write_variable received a key {k} not in the HashMap for variable {varname}")
            ))?;
            let extents: Extents = [i..i+1, 0..row.len()].into();
            var.put_values(&row, extents)
                .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
        }

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

    fn calc_match_dim(&self) -> Result<usize, MatchupError> {        
        let max_ninds = self.indices
            .values()
            .map(|v| v.len())
            .max()
            .unwrap_or(0);

        let max_ndist = self.distances
            .values()
            .map(|v| v.len())
            .max()
            .unwrap_or(0);

        if max_ninds != max_ndist {
            Err(MatchupError::InternalError(format!("Inconsistent maximum lengths of match indices {max_ninds} and distances {max_ndist}")))
        }else{
            Ok(max_ninds)
        }
    }

    fn get_sorted_keys<T>(data: &HashMap<u64, T>) -> Array1<u64> {
        let mut keys: Vec<u64> = data.keys()
            .map(|k| *k)
            .collect();
        keys.sort();
        Array1::from_vec(keys)
    }
    
}


pub struct OcoMatchGroups {
    match_sets: Vec<(HashSet<u64>, HashSet<u64>)>
}

impl OcoMatchGroups {
    pub fn to_nc_group(&self, ds: &mut netcdf::MutableFile, group_name: Option<&str>, oco2_lite_file: &Path, oco3_lite_file: &Path) -> Result<(), MatchupError> {
        let out_file = utils::nc_file(ds);
        let mut grp = self.setup_nc_group(ds, group_name, oco2_lite_file, oco3_lite_file)?;
        let [vi2, vs2, vi3, vs3] = Self::nc_varnames();

        let oco2_lite_sids = load_nc_var_from_file::<u64>(oco2_lite_file, "sounding_id")?;
        let oco3_lite_sids = load_nc_var_from_file::<u64>(oco3_lite_file, "sounding_id")?;

        for (i, (oco2_inds, oco3_inds)) in self.match_sets.iter().enumerate() {
            let oco2min = *oco2_inds.iter().min().expect("Expected at least one OCO-2 index in every hash set");
            let oco2max = *oco2_inds.iter().max().expect("Expected at least one OCO-2 index in every hash set");
            let oco3min = *oco3_inds.iter().min().expect("Expected at least one OCO-3 index in every hash set");
            let oco3max = *oco3_inds.iter().max().expect("Expected at least one OCO-3 index in every hash set");

            let oco2min_sid = oco2_lite_sids[oco2min as usize];
            let oco2max_sid = oco2_lite_sids[oco2max as usize];
            let oco3min_sid = oco3_lite_sids[oco3min as usize];
            let oco3max_sid = oco3_lite_sids[oco3max as usize];

            let extents: Extents = [i..i+1, 0..2].into();
            {
                grp.variable_mut(vi2).expect("OCO-2 index variable must be initialized first")
                .put_values(&[oco2min, oco2max], &extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }
            
            {
                grp.variable_mut(vs2).expect("OCO-2 sounding ID variable must be initialized first")
                .put_values(&[oco2min_sid, oco2max_sid], &extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }

            {
                grp.variable_mut(vi3).expect("OCO-3 index variable must be initialized first")
                .put_values(&[oco3min, oco3max], &extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }

            {
                grp.variable_mut(vs3).expect("OCO-3 sounding ID variable must be initialized first")
                .put_values(&[oco3min_sid, oco3max_sid], &extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }
        }


        Ok(())
    }

    fn match_group_dim() -> &'static str {
        "match_group"
    }

    fn start_end_dim() -> &'static str {
        "start_end"
    }

    fn nc_varnames() -> [&'static str; 4] {
        ["oco2_index", "oco2_sounding_id", "oco3_index", "oco3_sounding_id"]
    }

    fn setup_nc_group<'f>(&'f self, ds: &'f mut netcdf::MutableFile, group_name: Option<&str>, oco2_lite_file: &Path, oco3_lite_file: &Path) -> Result<netcdf::GroupMut, MatchupError> {
        // Get the units and long name from the OCO lite files
        let oco2_ds = netcdf::open(oco2_lite_file)
            .map_err(|e| MatchupError::from_nc_error(e, oco2_lite_file.to_owned()))?;
        let oco2_sid_var = oco2_ds
            .variable("sounding_id")
            .ok_or_else(|| MatchupError::NetcdfMissingVar { file: oco2_lite_file.to_owned(), varname: "sounding_id".to_owned() })?;
        let oco2_sid_units = get_str_attr_with_default(&oco2_sid_var, "units", "YYYYMMDDhhmmssmf".to_owned())?;
        let oco2_sid_longname = get_str_attr_with_default(&oco2_sid_var, "long_name", "OCO-2 sounding ID from UTC time".to_owned())?;

        let oco3_ds = netcdf::open(oco3_lite_file)
            .map_err(|e| MatchupError::from_nc_error(e, oco2_lite_file.to_owned()))?;
        let oco3_sid_var = oco3_ds
            .variable("sounding_id")
            .ok_or_else(|| MatchupError::NetcdfMissingVar { file: oco3_lite_file.to_owned(), varname: "sounding_id".to_owned() })?;
        let oco3_sid_units = get_str_attr_with_default(&oco3_sid_var, "units", "YYYYMMDDhhmmssmf".to_owned())?;
        let oco3_sid_longname = get_str_attr_with_default(&oco3_sid_var, "long_name", "OCO-2 sounding ID from UTC time".to_owned())?;

        // Make the group and variables
        let out_file = utils::nc_file(ds);
        let mut grp = if let Some(group_name) = group_name {
            ds.add_group(group_name)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?
        }else{
            ds.root_mut().ok_or_else(|| MatchupError::NetcdfError { nc_error: "Cannot get root group".into(), file: out_file.clone() })?
        };

        let n_groups = self.match_sets.len();
        grp.add_dimension(Self::match_group_dim(), n_groups)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
        grp.add_dimension(Self::start_end_dim(), 2)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;

        let [vi2, vs2, vi3, vs3] = Self::nc_varnames();
        let var_info = [
            (vi2, None, Some("0-based index for the sounding in the OCO-2 lite file")),
            (vs2, Some(oco2_sid_units.as_str()), Some(&oco2_sid_longname)),
            (vi3, None, Some("0-based index for the sounding in the OCO-3 lite file")),
            (vs3, Some(oco3_sid_units.as_str()), Some(&oco3_sid_longname))
        ];

        for (varname, units, descr) in var_info {
            let mut var = grp.add_variable::<u64>(varname, &[Self::match_group_dim(), Self::start_end_dim()])
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;

            if let Some(units) = units {
                var.add_attribute("units", units)
                    .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }

            if let Some(descr) = descr {
                var.add_attribute("description", descr)
                    .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }
        }

        Ok(grp)
    }
}

pub fn match_oco3_to_oco2(oco2: &OcoGeo, oco3: &OcoGeo, max_dist: f32) -> OcoMatches {
    let mut indices: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut distances: HashMap<u64, Vec<f32>> = HashMap::new();

    let pb2 = setup_oco2_progress(oco2.num_soundings());

    // TODO: remove progress bar for OCO-3 (not really needed) and try parallelizing this
    for (idx_oco2, (&lon_oco2, &lat_oco2)) in oco2.iter_latlon().enumerate() {
        let idx_oco2 = idx_oco2 as u64;
        pb2.set_position(idx_oco2 + 1);
        for (idx_oco3, (&lon_oco3, &lat_oco3)) in oco3.iter_latlon().enumerate() {
            let idx_oco3 = idx_oco3 as u64;
            let this_dist = great_circle_distance(lon_oco2, lat_oco2, lon_oco3, lat_oco3);

            if this_dist <= max_dist {
                indices.entry(idx_oco2 as u64).or_default().push(idx_oco3);
                distances.entry(idx_oco2 as u64).or_default().push(this_dist);
            }
        }
    }

    pb2.finish();

    OcoMatches { indices, distances }
}

fn setup_oco2_progress(n_oco2: u64) -> ProgressBar {
    let oco2_style = ProgressStyle::with_template(
        "OCO-2: {human_pos}/{human_len} {wide_bar} ETA = {eta}"
    ).unwrap();

    let pb2 = ProgressBar::new(n_oco2);
    pb2.set_style(oco2_style);
    pb2
}

pub fn identify_groups_from_matched_soundings(matched_soundings: OcoMatches) -> OcoMatchGroups {
    let mut match_sets: Vec<(HashSet<u64>, HashSet<u64>)> = Vec::new();

    for (oco2_idx, oco3_row) in matched_soundings.indices.into_iter() {
        let mut matched = false;
        for (oco2_idx_set, oco3_idx_set) in match_sets.iter_mut() {
            if oco3_row.iter().any(|i| oco3_idx_set.contains(i)) {
                oco2_idx_set.insert(oco2_idx);
                oco3_idx_set.extend(oco3_row.iter());
                matched = true;
                break;
            }
        }

        if !matched {
            match_sets.push((
                HashSet::from([oco2_idx]), HashSet::from_iter(oco3_row.into_iter())
            ));
        }
    }

    OcoMatchGroups { match_sets }
}