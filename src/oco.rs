use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use indicatif::{ProgressBar, ProgressStyle, ParallelProgressIterator};
use ndarray::{Array1, Ix1, Ix2};
use netcdf::extent::Extents;
use rayon::prelude::*;
use rayon::iter::ParallelIterator;
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
        println!("  -> Adding dimensions");
        grp.add_dimension("sounding", self.num_soundings() as usize)
            .map_err(|e| MatchupError::from_nc_error(e, PathBuf::from("output")))?;
        grp.add_attribute("source_lite_file", self.lite_file.display().to_string().as_str())
            .map_err(|e| MatchupError::from_nc_error(e, PathBuf::from("output")))?;
        println!("  -> Writing longitudes");
        write_nc_var(grp, self.longitude.view(), "longitude", &["sounding"], Some("degrees_east"), None)?;
        println!("  -> Writing latitudes");
        write_nc_var(grp, self.latitude.view(), "latitude", &["sounding"], Some("degrees_north"), None)?;
        println!("  -> Writing quality flags");
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

impl From<Vec<(u64, Vec<(u64, f32)>)>> for OcoMatches {
    fn from(value: Vec<(u64, Vec<(u64, f32)>)>) -> Self {
        let mut indices = HashMap::new();
        let mut distances = HashMap::new();

        for (idx_oco2, oco3_info) in value {
            let oco3_inds = oco3_info.iter().map(|(i, _)| *i).collect();
            let oco3_dists = oco3_info.iter().map(|(_, d)| *d).collect();
            indices.insert(idx_oco2, oco3_inds);
            distances.insert(idx_oco2, oco3_dists);
        }

        Self { indices, distances }
    }
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

    pub fn ordered_oco2_indices(&self) -> Vec<u64> {
        let mut oco2inds: Vec<u64> = self.indices
            .keys()
            .map(|&k| k)
            .collect();
        oco2inds.sort();
        oco2inds
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

        println!("  -> Adding dimensions");
        grp.add_dimension("oco2_match", n_oco2)
            .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
        grp.add_dimension("oco3_match", max_oco3)
            .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;

        println!("  -> Sorting OCO-2 matched indices");
        let ordered_keys = Self::get_sorted_keys(&self.indices);
        if ordered_keys != Self::get_sorted_keys(&self.distances) {
            return Err(MatchupError::InternalError(
                "OcoMatches instance has inconsistent keys for `indices` and `distances`".to_owned()
            ))
        }

        println!("  -> Writing the OCO-2 matched indices");
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

        println!("  -> Writing 2D variable {varname}");
        let mut var = grp.add_variable::<T>(varname, &["oco2_match", "oco3_match"])
            .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
        var.set_fill_value(fill_value)
            .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
        // var.compression(9, true)
        //     .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;

        let pb = setup_progress_bar(keys.len() as u64, "rows written");
        for (i, k) in keys.iter().enumerate() {
            pb.set_position(i as u64 + 1);
            let row = data.get(k).ok_or_else(|| MatchupError::InternalError(
                format!("OcoMatches::write_variable received a key {k} not in the HashMap for variable {varname}")
            ))?;
            let extents: Extents = [i..i+1, 0..row.len()].into();
            var.put_values(&row, extents)
                .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
        }
        pb.finish_with_message("  -> Finished writing 2D variable values");

        println!("  -> Writing attributes");
        if let Some(units) = units {
            var.add_attribute("units", units)
                .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
        }

        if let Some(description) = description {
            var.add_attribute("description", description)
                .map_err(|e| MatchupError::from_nc_error(e, file.clone()))?;
        }
        println!("  -> Finished with variable {varname}");

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
        // Convert the lite files to path strings and get checksums, we'll make these attributes later
        let oco2_file_string = format!("{}", oco2_lite_file.display());
        let oco2_checksum = utils::file_sha256(oco2_lite_file)?;
        let oco3_file_string = format!("{}", oco2_lite_file.display());
        let oco3_checksum = utils::file_sha256(oco3_lite_file)?;
        
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

        grp.add_attribute("oco2_lite_file_path", oco2_file_string)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
        grp.add_attribute("oco2_lite_file_sha256", oco2_checksum)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
        grp.add_attribute("oco3_lite_file_path", oco3_file_string)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
        grp.add_attribute("oco3_lite_file_sha256", oco3_checksum)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;

        Ok(grp)
    }
}

pub fn match_oco3_to_oco2_parallel(oco2: &OcoGeo, oco3: &OcoGeo, max_dist: f32) -> OcoMatches {
    let n_oco2 = oco2.longitude.len();
    let oco2_inds = Array1::from_iter(0..n_oco2);
    
    let mut matchups: Vec<(u64, Vec<(u64, f32)>)> = Vec::new();

    let par_it = ndarray::Zip::from(&oco2_inds)
        .and(&oco2.longitude)
        .and(&oco2.latitude)
        .into_par_iter();

    matchups.par_extend(
        par_it
        .progress_count(n_oco2 as u64)
        .filter_map(|(&i, &x, &y)| { 
            let oco3 = oco3.clone();
            let this_result = make_one_oco_match_vec(i, x, y, &oco3, max_dist);
            if this_result.1.is_empty() {
                None
            }else{
                Some(this_result)
            }
        }
    ));
    
    OcoMatches::from(matchups)
}

fn make_one_oco_match_vec(idx_oco2: usize, lon_oco2: f32, lat_oco2: f32, oco3: &OcoGeo, max_dist: f32) -> (u64, Vec<(u64, f32)>) {
    let mut oco3_matches = Vec::new();

    for (idx_oco3, (&lon_oco3, &lat_oco3)) in oco3.iter_latlon().enumerate() {
        let idx_oco3 = idx_oco3 as u64;
        let this_dist = great_circle_distance(lon_oco2, lat_oco2, lon_oco3, lat_oco3);

        if this_dist <= max_dist {
            oco3_matches.push((idx_oco3, this_dist));
        }
    }

    (idx_oco2 as u64, oco3_matches)
}

fn setup_progress_bar(n_match: u64, action: &str) -> ProgressBar {
    let style = ProgressStyle::with_template(
        &format!("{{bar}} {{human_pos}}/{{human_len}} {action}")
    ).unwrap();

    let pb = ProgressBar::new(n_match);
    pb.set_style(style);
    pb
}

pub fn identify_groups_from_matched_soundings(matched_soundings: OcoMatches) -> OcoMatchGroups {
    let mut match_sets: Vec<(HashSet<u64>, HashSet<u64>)> = Vec::new();
    // It's important to iterate over ordered keys: when I let this be unordered, some groups that
    // should be one got split up, I think because the (non-overlapping) ends got put into separate 
    // groups before the middle soundings were handled.
    let ordered_keys = matched_soundings.ordered_oco2_indices();

    let pb = setup_progress_bar(ordered_keys.len() as u64, "match vectors grouped");
    for oco2_idx in ordered_keys {
        pb.inc(1);
        let oco3_row = matched_soundings.indices.get(&oco2_idx)
            .expect("Tried to get a row of OCO-3 indices for an OCO-2 index that does not exist");
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
                HashSet::from([oco2_idx]), HashSet::from_iter(oco3_row.iter().map(|&i| i))
            ));
        }
    }
    pb.finish_with_message("  -> All matches grouped.");

    OcoMatchGroups { match_sets }
}