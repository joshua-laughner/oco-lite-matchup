use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, Arc};

// use indicatif::{ProgressBar, ProgressStyle, ParallelProgressIterator};
use indicatif::ParallelProgressIterator;
use itertools::{izip, Itertools};
use ndarray::{Array1, Ix1, Ix2, concatenate, Axis, Array2, Array};
use netcdf::extent::Extents;
use rayon::prelude::*;
use rayon::iter::ParallelIterator;
use serde::Serialize;

use crate::error::MatchupError;
use crate::utils::{load_nc_var, write_nc_var, filter_by_quality, great_circle_distance, self, RunningMean, ShowProgress};

const SOUNDING_ID_UNITS: &str = "YYYYMMDDhhmmssmf";
const SOUNDING_ID_DESCR_OCO2: &str = "OCO-2 sounding ID";
const SOUNDING_ID_DESCR_OCO3: &str = "OCO-3 sounding ID";

#[derive(Debug, Serialize, Default)]
pub struct OcoGeo {
    pub lite_files: Vec<PathBuf>,
    pub file_index: Array1<u8>,
    pub sounding_id: Array1<u64>,
    pub timestamp: Array1<f64>,
    pub longitude: Array1<f32>,
    pub latitude: Array1<f32>,
    pub quality: Array1<u8>
}

impl OcoGeo {
    pub fn load_lite_file(lite_file: &Path, flag0_only: bool) -> Result<Self, MatchupError> {
        let ds = netcdf::open(lite_file)
        .map_err(|e| MatchupError::from_nc_error(e, lite_file.to_owned()))?;
            
        let sounding_id = load_nc_var(&ds, "sounding_id")?;
        let timestamp = load_nc_var(&ds, "time")?;
        let longitude = load_nc_var(&ds, "longitude")?;
        let latitude = load_nc_var(&ds, "latitude")?;
        let quality = load_nc_var(&ds, "xco2_quality_flag")?;
        let file_index = Array1::zeros(timestamp.len());

        if flag0_only {
            let longitude = filter_by_quality(longitude.view(), quality.view());
            let latitude = filter_by_quality(latitude.view(), quality.view());
            let quality = filter_by_quality(quality.view(), quality.view());
            Ok(OcoGeo { lite_files: vec![lite_file.to_owned()], file_index, sounding_id, timestamp, longitude, latitude, quality })
        }else{
            Ok(OcoGeo { lite_files: vec![lite_file.to_owned()], file_index, sounding_id, timestamp, longitude, latitude, quality })
        }

    }

    pub fn to_nc_group(&self, grp: &mut netcdf::GroupMut) -> Result<(), MatchupError> {
        let out_file = PathBuf::from("?");
        println!("  -> Adding dimensions");
        grp.add_dimension("lite_file", self.lite_files.len())
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
        grp.add_dimension("sounding", self.num_soundings() as usize)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
        
        println!("  -> Writing lite file list");
        let mut var = grp.add_string_variable("lite_file", &["lite_file"])
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
        for (i, fname) in self.lite_files.iter().enumerate() {
            let ex: Extents = i.into();
            var.put_string(fname.display().to_string().as_str(), ex)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
        }
        var.add_attribute("description", "Source lite files that these soundings came from")
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;

        println!("  -> Writing file index");
        write_nc_var(grp, self.file_index.view(), "file_index", &["sounding"], None, Some("Index of the lite_file variable that defines the path which this point came from"))?;
        println!("  -> Writing timestamps");
        write_nc_var(grp, self.timestamp.view(), "time", &["sounding"], Some("seconds since 1970-01-01 00:00:00"), None)?;
        println!("  -> Writing longitudes");
        write_nc_var(grp, self.longitude.view(), "longitude", &["sounding"], Some("degrees_east"), None)?;
        println!("  -> Writing latitudes");
        write_nc_var(grp, self.latitude.view(), "latitude", &["sounding"], Some("degrees_north"), None)?;
        println!("  -> Writing quality flags");
        write_nc_var(grp, self.quality.view(), "quality_flag", &["sounding"], None, Some("0 = good, 1 = bad"))?;

        Ok(())
    }

    pub fn extend(mut self, other: Self) -> Self {
        let curr_n_files = self.lite_files.len() as u8;
        self.lite_files.extend(other.lite_files);
        self.file_index = concatenate![Axis(0), self.file_index, other.file_index + curr_n_files];
        self.sounding_id = concatenate![Axis(0), self.sounding_id, other.sounding_id];
        self.timestamp = concatenate![Axis(0), self.timestamp, other.timestamp];
        self.longitude = concatenate![Axis(0), self.longitude, other.longitude];
        self.latitude = concatenate![Axis(0), self.latitude, other.latitude];
        self.quality = concatenate![Axis(0), self.quality, other.quality];

        self
    }

    pub fn num_soundings(&self) -> u64 {
        self.longitude.len() as u64
    }
}

#[derive(Debug, Serialize)]
pub struct OcoMatches {
    /// List of OCO-2 files read
    oco2_files: Vec<PathBuf>,
    /// List of OCO-3 files read
    oco3_files: Vec<PathBuf>,
    /// A list of matches each between one OCO-2 sounding and 1 or more OCO-3 soundings
    matches: Vec<Match2to3>
}

impl OcoMatches {
    fn oco2_index_varname() -> &'static str {
        "oco2_index"
    }

    fn oco3_index_varname() -> &'static str {
        "oco3_index"
    }

    fn oco2_fileindex_varname() -> &'static str {
        "oco2_file_index"
    }

    fn oco3_fileindex_varname() -> &'static str {
        "oco3_file_index"
    }

    fn oco2_sounding_id_varname() -> &'static str {
        "oco2_sounding_id"
    }

    fn oco3_sounding_id_varname() -> &'static str {
        "oco3_sounding_id"
    }

    fn dist_varname() -> &'static str {
        "distance"
    }

    fn time_diff_varname() -> &'static str {
        "time_difference"
    }

    fn from_matches(mut sounding_matches: Vec<Match2to3>, oco2_files: Vec<PathBuf>, oco3_files: Vec<PathBuf>) -> Self {
        // Ensure that the matches are ordered by OCO-2 sounding ID, this avoids issues with groups of matches getting
        // split up because we examine them out of order
        sounding_matches.sort_by_key(|m| m.oco2_sounding_id);
        Self { oco2_files, oco3_files, matches: sounding_matches }
    }

    pub fn from_nc_group(grp: &netcdf::Group) -> Result<Self, MatchupError> {
        fn load_var<T: netcdf::NcPutGet, D: ndarray::Dimension>(grp: &netcdf::Group, varname: &str) -> Result<(Array<T, D>, T), MatchupError> {
            let var = grp.variable(varname)
                .ok_or_else(|| MatchupError::NetcdfMissingVar { file: None, varname: varname.to_owned() })?;

            let arr = var.values_arr::<T, _>(Extents::All)?
                .into_dimensionality::<D>()
                .map_err(|e| MatchupError::NetcdfShapeError { file: None, varname: varname.to_owned(), nd_error: e })?;
            let fill: T = var.fill_value()?
                .ok_or_else(|| MatchupError::NetcdfMissingVar { file: None, varname: format!("{varname} fill value") })?;
            
            Ok((arr, fill))
        }

        fn load_1d_var<T: netcdf::NcPutGet + std::cmp::PartialEq + Clone>(grp: &netcdf::Group, varname: &str) -> Result<Vec<T>, MatchupError> {
            let (arr, fill) = load_var::<T, Ix1>(grp, varname)?;
            let v = arr.to_vec();
            if v.iter().any(|el| *el == fill) {
                return Err(MatchupError::InternalError(format!("1D variable {varname} has fill values")));
            }

            Ok(v)
        }

        fn load_2d_var<T: netcdf::NcPutGet + std::cmp::PartialEq + Copy>(grp: &netcdf::Group, varname: &str) -> Result<Vec<Vec<T>>, MatchupError> {
            let (arr, fill) = load_var::<T, Ix2>(grp, varname)?;
            let mut vec_out = Vec::new();
            for row in arr.rows() {
                let row = row.iter().filter_map(|v| if *v == fill { None } else { Some(*v) } ).collect();
                vec_out.push(row);
            }
            Ok(vec_out)
        }

        fn load_string_var(grp: &netcdf::Group, varname: &str) -> Result<Vec<String>, MatchupError> {
            let mut v = Vec::new();
            let var = grp.variable(varname)
                .ok_or_else(|| MatchupError::NetcdfMissingVar { file: None, varname: varname.to_owned() })?;

            let n = if let [dim] = var.dimensions() {
                dim.len()
            } else {
                return Err(MatchupError::InternalError("Expected a full match file to have only string variables with 1 dimension".to_owned()));
            };

            for i in 0..n {
                let s = var.string_value(i)?;
                v.push(s);
            }
            

            Ok(v)
        }

        let oco2_files = load_string_var(grp, "oco2_file")?
            .iter()
            .map(PathBuf::from)
            .collect_vec();
        let oco3_files = load_string_var(grp, "oco3_file")?
            .iter()
            .map(PathBuf::from)
            .collect_vec();
        let oco2_file_indices = load_1d_var::<u8>(grp, Self::oco2_fileindex_varname())?;
        let oco2_sounding_indices = load_1d_var::<u64>(grp, Self::oco2_index_varname())?;
        let oco2_sounding_ids = load_1d_var::<u64>(grp, Self::oco2_sounding_id_varname())?;
        let oco3_file_indices = load_2d_var::<u8>(grp, Self::oco3_fileindex_varname())?;
        let oco3_sounding_indices = load_2d_var::<u64>(grp, Self::oco3_index_varname())?;
        let oco3_sounding_ids = load_2d_var::<u64>(grp, Self::oco3_sounding_id_varname())?;
        let distances = load_2d_var::<f32>(grp, Self::dist_varname())?;
        let time_diffs = load_2d_var::<f32>(grp, Self::time_diff_varname())?;

        let it = izip!(
            oco2_file_indices.into_iter(),
            oco2_sounding_indices.into_iter(),
            oco2_sounding_ids.into_iter(),
            oco3_file_indices.into_iter(),
            oco3_sounding_indices.into_iter(),
            oco3_sounding_ids.into_iter(),
            distances.into_iter(),
            time_diffs.into_iter()
        );

        let oco_matches: Vec<Match2to3> = it
            .map(|(oco2_fi, oco2_i, oco2_sid, oco3_fi, oco3_i, oco3_sid, dist, dt)| {
                Match2to3 { 
                    oco2_file_index: oco2_fi, oco2_sounding_index: oco2_i, oco2_sounding_id: oco2_sid,
                    oco3_file_indices: oco3_fi, oco3_sounding_indices: oco3_i, oco3_sounding_ids: oco3_sid,
                    distance_km: dist, time_diff_s: dt
                }
            }).collect();

        

        Ok(Self { oco2_files, oco3_files, matches: oco_matches })
    }

    pub fn to_nc_group(&self, grp: &mut netcdf::GroupMut) -> Result<(), MatchupError> {
        // Vlen types have weird lifetime issues, so we're doing 2D arrays.
        
        let n_oco2 = self.matches.len();
        let max_oco3 = self.calc_match_dim()?;

        println!("  -> Adding dimensions");
        grp.add_dimension("oco2_file", self.oco2_files.len())?;
        grp.add_dimension("oco3_file", self.oco3_files.len())?;
        grp.add_dimension("oco2_match", n_oco2)?;
        grp.add_dimension("oco3_match", max_oco3)?;

        println!("  -> Writing the OCO -2 and -3 file paths");
        Self::write_paths_variable(grp, &self.oco2_files, "oco2_file", "oco2_file", Some("Paths to the OCO-2 lite files used in this matchup"))?;
        Self::write_paths_variable(grp, &self.oco3_files, "oco3_file", "oco3_file", Some("Paths to the OCO-3 lite files used in this matchup"))?;

        self.write_1d_variable(grp, Self::oco2_fileindex_varname(), None, Some("0-based index of the file from the oco2_file variable that this sounding came from"), |m| m.oco2_file_index, u8::MAX)?;
        self.write_1d_variable(grp, Self::oco2_index_varname(), None, Some("0-based index of the sounding within its lite file"), |m| m.oco2_sounding_index, u64::MAX)?;
        self.write_1d_variable(grp, Self::oco2_sounding_id_varname(), Some(SOUNDING_ID_UNITS), Some(SOUNDING_ID_DESCR_OCO2), |m| m.oco2_sounding_id, u64::MAX)?;

        self.write_2d_variable(grp, Self::oco3_fileindex_varname(), None, Some("0-based index of the file from the oco2_file variable that this sounding came from"), |m| m.oco3_file_indices.as_slice(), u8::MAX)?;
        self.write_2d_variable(grp, Self::oco3_index_varname(), None, Some("0-based index of the sounding within its lite file"), |m| m.oco3_sounding_indices.as_slice(), u64::MAX)?;
        self.write_2d_variable(grp, Self::oco3_sounding_id_varname(), Some(SOUNDING_ID_UNITS), Some(SOUNDING_ID_DESCR_OCO3), |m| m.oco3_sounding_ids.as_slice(), u64::MAX)?;
        self.write_2d_variable(grp, Self::dist_varname(), Some("km"), Some("Distance between the OCO-2 and OCO-3 sounding"), |m| m.distance_km.as_slice(), f32::MAX)?;
        self.write_2d_variable(grp, Self::time_diff_varname(), Some("s"), Some("Time difference between the OCO-2 and OCO-3 sounding in seconds"), |m| m.time_diff_s.as_slice(), f32::MAX)?;
        Ok(())
    }

    fn get_match_1d_array<F, T>(&self, get_item: F, fill_value: T) -> Array1<T>
    where F: Fn(&Match2to3) -> T,
          T: Clone
    {
        let mut arr = Array1::from_elem(self.matches.len(), fill_value);
        for (i, m) in self.matches.iter().enumerate() {
            arr[i] = get_item(m);
        }
        arr
    }

    fn get_match_2d_array<F, T>(&self, get_row: F, row_length: usize, fill_value: T) -> Array2<T>
    where F: Fn(&Match2to3) -> &[T],
          T: Clone + Copy
    {
        let mut arr: Array2<T> = Array2::from_elem((self.matches.len(), row_length), fill_value);
        for (i, m) in self.matches.iter().enumerate() {
            for (j, &v) in get_row(m).iter().enumerate() {
                arr[[i, j]] = v;
            }
        }
        arr
    }

    fn write_paths_variable(grp: &mut netcdf::GroupMut, paths: &[PathBuf], varname: &str, dim: &str, description: Option<&str>) -> Result<(), MatchupError>{
        let data = paths.iter().map(|p| p.display().to_string()).collect_vec();
        utils::write_string_nc_var(grp, &data, varname, dim, None, description)
    }

    fn write_1d_variable<T: netcdf::NcPutGet + Clone + Copy, F: Fn(&Match2to3) -> T>(
        &self,
        grp: &mut netcdf::GroupMut,
        varname: &str,
        units: Option<&str>,
        description: Option<&str>,
        get_item: F,
        fill_value: T
    ) -> Result<(), MatchupError>{
        println!("  -> Writing 1D variable {varname}");
        let mut var = grp.add_variable::<T>(varname, &["oco2_match"])?;
        var.set_fill_value(fill_value)?;
        var.compression(9, true)?;

        let arr = self.get_match_1d_array(get_item, fill_value);
        var.put_values(arr.as_slice().unwrap(), Extents::All)?;

        println!("  -> Writing attributes");
        if let Some(units) = units {
            var.add_attribute("units", units)?;
        }

        if let Some(description) = description {
            var.add_attribute("description", description)?;
        }
        println!("  -> Finished with variable {varname}");

        Ok(())
    }

    fn write_2d_variable<T: netcdf::NcPutGet + Clone + Copy, F: Fn(&Match2to3) -> &[T]>(
        &self,
        grp: &mut netcdf::GroupMut,
        varname: &str,
        units: Option<&str>,
        description: Option<&str>,
        get_row: F,
        fill_value: T
    ) -> Result<(), MatchupError>{
        println!("  -> Writing 2D variable {varname}");
        let row_length = grp.dimension("oco3_match")
            .expect("oco3_match dimension must be created before calling `write_2d_variable`")
            .len();

        let mut var = grp.add_variable::<T>(varname, &["oco2_match", "oco3_match"])?;
        var.set_fill_value(fill_value)?;
        var.compression(9, true)?;


        let arr = self.get_match_2d_array(get_row, row_length, fill_value);
        var.put_values(arr.as_slice().unwrap(), Extents::All)?;

        println!("  -> Writing attributes");
        if let Some(units) = units {
            var.add_attribute("units", units)?;
        }

        if let Some(description) = description {
            var.add_attribute("description", description)?;
        }
        println!("  -> Finished with variable {varname}");

        Ok(())
    }

    fn calc_match_dim(&self) -> Result<usize, MatchupError> {        
        let ninds: Result<Vec<usize>, MatchupError> = self.matches
            .iter()
            .map(|v| {
                let n_fi = v.oco3_file_indices.len();
                let n_i = v.oco3_sounding_indices.len();
                let n_sid = v.oco3_sounding_ids.len();
                let n_dist = v.distance_km.len();

                if n_fi == n_i && n_fi == n_sid && n_fi == n_dist {
                    Ok(n_fi)
                }else{
                    Err(MatchupError::InternalError(format!(
                        "Inconsistent lengths of OCO-3 match values. File indices = {}, sounding indices = {}, sounding IDs = {}, distance = {}",
                        n_fi, n_i, n_sid, n_dist
                    )))
                }
            })
            .collect();

        let max_ninds = ninds?.into_iter().max().unwrap_or(0);
        Ok(max_ninds)

    }
    
}


pub struct OcoMatchGroups {
    oco2_lite_files: Vec<PathBuf>,
    oco3_lite_files: Vec<PathBuf>,
    /// Each element is the set of OCO-2 sounding IDs that match a set of OCO-3 sounding IDs
    match_sets: Vec<(HashSet<u64>, HashSet<u64>)>,
    oco2_sounding_indices: HashMap<u64, (u8, u64)>,
    oco3_sounding_indices: HashMap<u64, (u8, u64)>,
    distances: HashMap<u64, RunningMean<f32>>,
    time_diffs: HashMap<u64, RunningMean<f32>>
}

impl OcoMatchGroups {
    pub fn to_nc_group(&self, ds: &mut netcdf::MutableFile, group_name: Option<&str>) -> Result<(), MatchupError> {
        let out_file = utils::nc_file(ds);
        let mut grp = self.setup_nc_group(ds, group_name)?;

        self.write_file_variables(&mut grp)?;

        for (i, (oco2_inds, oco3_inds)) in self.match_sets.iter().enumerate() {
            // Calculate the first and last sounding ID for each group
            let oco2_sid_min = *oco2_inds.iter().min().expect("Expected at least one OCO-2 index in every hash set");
            let oco2_sid_max = *oco2_inds.iter().max().expect("Expected at least one OCO-2 index in every hash set");
            let oco3_sid_min = *oco3_inds.iter().min().expect("Expected at least one OCO-3 index in every hash set");
            let oco3_sid_max = *oco3_inds.iter().max().expect("Expected at least one OCO-3 index in every hash set");

            // Calculate the mean distance and time difference between OCO-2 and -3
            let group_mean_dist = oco2_inds.iter()
                .try_fold(RunningMean::new(), |mut acc, k| {
                    let dx = self.distances.get(k)
                        .ok_or_else(|| MatchupError::InternalError(format!("OCO-2 sounding ID {k} not stored in the distance hash map")))?;
                    acc += *dx;
                    Ok::<RunningMean<f32>, MatchupError>(acc)
                })?.mean().unwrap_or(f32::NAN);

            let group_mean_dt = oco2_inds.iter()
                .try_fold(RunningMean::new(), |mut acc, k| {
                    let dx = self.time_diffs.get(k)
                        .ok_or_else(|| MatchupError::InternalError(format!("OCO-2 sounding ID {k} not stored in the time difference hash map")))?;
                    acc += *dx;
                    Ok::<RunningMean<f32>, MatchupError>(acc)
                })?.mean().unwrap_or(f32::NAN);

            // Get the corresponding file and sounding indices
            let &(oco2_fid_min, oco2_idx_min) = self.oco2_sounding_indices.get(&oco2_sid_min)
                .ok_or_else(|| MatchupError::InternalError(format!("OCO-2 sounding ID {oco2_sid_min} not stored in the index hashmap")))?;
            let &(oco2_fid_max, oco2_idx_max) = self.oco2_sounding_indices.get(&oco2_sid_max)
                .ok_or_else(|| MatchupError::InternalError(format!("OCO-2 sounding ID {oco2_sid_max} not stored in the index hashmap")))?;
            let &(oco3_fid_min, oco3_idx_min) = self.oco3_sounding_indices.get(&oco3_sid_min)
                .ok_or_else(|| MatchupError::InternalError(format!("OCO-3 sounding ID {oco3_sid_min} not stored in the index hashmap")))?;
            let &(oco3_fid_max, oco3_idx_max) = self.oco3_sounding_indices.get(&oco3_sid_max)
                .ok_or_else(|| MatchupError::InternalError(format!("OCO-3 sounding ID {oco3_sid_max} not stored in the index hashmap")))?;

            let scalar_extents: Extents = [i].into();
            let extents: Extents = [i..i+1, 0..2].into();

            // Writing OCO-2 variables
            {
                grp.variable_mut(&Self::sounding_id_varname(2)).expect("OCO-2 sounding ID variable must be initialized first")
                .put_values(&[oco2_sid_min, oco2_sid_max], &extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }
            
            {
                grp.variable_mut(&Self::sounding_index_varname(2)).expect("OCO-2 sounding index variable must be initialized first")
                .put_values(&[oco2_idx_min, oco2_idx_max], &extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }

            {
                grp.variable_mut(&Self::file_index_varname(2)).expect("OCO-2 file index variable must be initialized first")
                .put_values(&[oco2_fid_min, oco2_fid_max], &extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }

            // Writing OCO-3 variables
            {
                grp.variable_mut(&Self::sounding_id_varname(3)).expect("OCO-3 sounding ID variable must be initialized first")
                .put_values(&[oco3_sid_min, oco3_sid_max], &extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }
            
            {
                grp.variable_mut(&Self::sounding_index_varname(3)).expect("OCO-3 sounding index variable must be initialized first")
                .put_values(&[oco3_idx_min, oco3_idx_max], &extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }

            {
                grp.variable_mut(&Self::file_index_varname(3)).expect("OCO-3 file index variable must be initialized first")
                .put_values(&[oco3_fid_min, oco3_fid_max], &extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }

            // Writing distance and time variables
            {
                grp.variable_mut(Self::distance_varname()).expect("Distance variable must be initialized first")
                .put_values(&[group_mean_dist], &scalar_extents)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
            }

            {
                grp.variable_mut(Self::time_diff_varname()).expect("Time difference variable must be initialized first")
                .put_values(&[group_mean_dt], &scalar_extents)
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

    fn lite_file_varname(instrument: i32) -> String {
        format!("oco{instrument}_lite_file")
    }

    fn lite_file_sha256_varname(instrument: i32) -> String {
        format!("oco{instrument}_lite_file_sha256")
    }

    fn sounding_id_varname(instrument: i32) -> String {
        format!("oco{instrument}_sounding_id")
    }

    fn sounding_index_varname(instrument: i32) -> String {
        format!("oco{instrument}_sounding_index")
    }

    fn file_index_varname(instrument: i32) -> String {
        format!("oco{instrument}_file_index")
    }

    fn distance_varname() -> &'static str {
        "mean_oco2_oco3_distance"
    }

    fn time_diff_varname() -> &'static str {
        "mean_oco2_oco3_time_difference"
    }

    fn setup_nc_group<'f>(&'f self, ds: &'f mut netcdf::MutableFile, group_name: Option<&str>) -> Result<netcdf::GroupMut, MatchupError> {
        
        // Make the group and variables
        let out_file = utils::nc_file(ds);
        let mut grp = if let Some(group_name) = group_name {
            ds.add_group(group_name)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?
        }else{
            ds.root_mut().ok_or_else(|| MatchupError::NetcdfError { nc_error: "Cannot get root group".into(), file: Some(out_file.clone()) })?
        };

        let n_groups = self.match_sets.len();
        grp.add_dimension(Self::match_group_dim(), n_groups)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;
        grp.add_dimension(Self::start_end_dim(), 2)
            .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?;

        let dims1 = vec![Self::match_group_dim()];
        let dims2 = vec![Self::match_group_dim(), Self::start_end_dim()];

        let var_info = [
            (Self::sounding_id_varname(2), &dims2, false, Some(SOUNDING_ID_UNITS), Some(SOUNDING_ID_DESCR_OCO2)),
            (Self::file_index_varname(2), &dims2, false, None, Some("0-based index for the OCO-2 lite file name variable")),
            (Self::sounding_index_varname(2), &dims2, false, None, Some("0-based index for the sounding in the OCO-2 lite file")),
            (Self::sounding_id_varname(3), &dims2, false, Some(SOUNDING_ID_UNITS), Some(SOUNDING_ID_DESCR_OCO3)),
            (Self::file_index_varname(3), &dims2, false, None, Some("0-based index for the OCO-3 lite file name variable")),
            (Self::sounding_index_varname(3), &dims2, false, None, Some("0-based index for the sounding in the OCO-3 lite file")),
            (Self::distance_varname().to_owned(), &dims1, true, Some("km"), Some("Mean distance between the matched OCO-2 and -3 soundings. Note that this is only calculated for soundings meeting the coincidence criteria, which may not be all soundings in the group.")),
            (Self::time_diff_varname().to_owned(), &dims1, true, Some("s"), Some("Mean time difference (in seconds) between the matched OCO-2 and -3 soundings. Note that this is only calculated for soundings meeting the coincidence criteria, which may not be all soundings in the group.")),
        ];

        for (varname, dims, is_float, units, descr) in var_info {
            let mut var = if !is_float {
                grp.add_variable::<u64>(&varname, dims)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?
            } else {
                grp.add_variable::<f32>(&varname, dims)
                .map_err(|e| MatchupError::from_nc_error(e, out_file.clone()))?
            };

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

    fn write_file_variables(&self, grp: &mut netcdf::GroupMut) -> Result<(), MatchupError> { 
        let oco2_lite_files = self.oco2_lite_files.iter().map(|p| p.display().to_string()).collect_vec();
        let oco2_file_sha256 = self.oco2_lite_files.iter().map(|p| utils::file_sha256(p)).collect::<Result<Vec<String>,_>>()?;
        let oco3_lite_files = self.oco3_lite_files.iter().map(|p| p.display().to_string()).collect_vec();
        let oco3_file_sha256 = self.oco3_lite_files.iter().map(|p| utils::file_sha256(p)).collect::<Result<Vec<String>,_>>()?;

        utils::write_string_nc_var(grp, &oco2_lite_files, &Self::lite_file_varname(2), "oco2_lite_file", None, Some("Paths to OCO-2 lite files"))?;
        utils::write_string_nc_var(grp, &oco2_file_sha256, &Self::lite_file_sha256_varname(2), "oco2_lite_file", None, Some("SHA-256 checksums of OCO-2 lite files"))?;
        utils::write_string_nc_var(grp, &oco3_lite_files, &Self::lite_file_varname(3), "oco3_lite_file", None, Some("Paths to OCO-3 lite files"))?;
        utils::write_string_nc_var(grp, &oco3_file_sha256, &Self::lite_file_sha256_varname(3), "oco3_lite_file", None, Some("SHA-256 checksums of OCO-3 lite files"))?;

        Ok(())
    }
}

pub fn match_oco3_to_oco2_parallel(oco2: &OcoGeo, oco3: &OcoGeo, max_dist: f32, max_dt: f64, show_progress: ShowProgress) -> OcoMatches {
    let n_oco2 = oco2.longitude.len();
    let oco2_inds = Array1::from_iter(0..n_oco2);
    
    let mut matchups: Vec<Match2to3> = Vec::new();

    let par_it = ndarray::Zip::from(&oco2_inds)
        .and(&oco2.file_index)
        .and(&oco2.sounding_id)
        .and(&oco2.longitude)
        .and(&oco2.latitude)
        .and(&oco2.timestamp)
        .into_par_iter();


    let pbsty = indicatif::ProgressStyle::with_template(
        "{msg} {bar} {human_pos}/{human_len}"
    ).unwrap();
    let pb = indicatif::ProgressBar::new(n_oco2 as u64);
    pb.set_style(pbsty);
    let sid0 = oco2.sounding_id
        .first()
        .copied()
        .unwrap_or(19930101); // use the TAI93 epoch as the default value
    pb.set_message(format!("Matching {} OCO-2 soundings", utils::sid_to_date(sid0).unwrap_or_default()));
    

    match show_progress {
        ShowProgress::Yes =>  {
            matchups.par_extend(
                par_it
                .progress_with(pb)
                .filter_map(|tup| { 
                    parallel_helper(tup, max_dist, max_dt, oco3)
                }
            ));
        },
        ShowProgress::No => {
            matchups.par_extend(
                par_it
                .filter_map(|tup| { 
                    parallel_helper(tup, max_dist, max_dt, oco3)
                }
            ));
        },
        ShowProgress::Multi(mbar) => {
            let pb = Arc::new(Mutex::from(mbar.add(pb)));
            matchups.par_extend(
                par_it
                .filter_map(|tup| { 
                    let res = parallel_helper(tup, max_dist, max_dt, oco3);
                    if let Ok(pb) = pb.lock() {
                        pb.inc(1);
                    }
                    res
                }
            ));
            
            if let Ok(pb) = pb.lock() {
                pb.finish_and_clear();
            };
        },
    }

    println!("Number of matchups = {}", matchups.len());
    
    OcoMatches::from_matches(matchups, oco2.lite_files.clone(), oco3.lite_files.clone())
}

fn parallel_helper(tup: (&usize, &u8, &u64, &f32, &f32, &f64), max_dist: f32, max_dt: f64, oco3: &OcoGeo) -> Option<Match2to3> {
    let (&i_oco2, &fi_oco2, &sid_oco2, &lon_oco2, &lat_oco2, &ts_oco2) = tup;
    let this_result = make_one_oco_match_vec(fi_oco2, i_oco2, sid_oco2, lon_oco2, lat_oco2, ts_oco2, oco3, max_dist, max_dt);
    if this_result.is_empty() {
        None
    }else{
        Some(this_result)
    }
}

#[derive(Debug, Serialize)]
struct Match2to3 {
    oco2_file_index: u8,
    oco2_sounding_index: u64,
    oco2_sounding_id: u64,
    oco3_file_indices: Vec<u8>,
    oco3_sounding_indices: Vec<u64>,
    oco3_sounding_ids: Vec<u64>,
    distance_km: Vec<f32>,
    time_diff_s: Vec<f32>
}

impl Match2to3 {
    fn new(oco2_file_index: u8, oco2_sounding_index: u64, oco2_sounding_id: u64) -> Self {
        Self { 
            oco2_file_index, 
            oco2_sounding_index, 
            oco2_sounding_id, 
            oco3_file_indices: Vec::new(),
            oco3_sounding_indices: Vec::new(), 
            oco3_sounding_ids: Vec::new(), 
            distance_km: Vec::new(),
            time_diff_s: Vec::new()
        }
    }

    fn add_oco3_match(&mut self, file_idx_oco3: u8, idx_oco3: usize, sid_oco3: u64, dist: f32, dt_sec: f32) {
        self.oco3_file_indices.push(file_idx_oco3);
        self.oco3_sounding_indices.push(idx_oco3 as u64);
        self.oco3_sounding_ids.push(sid_oco3);
        self.distance_km.push(dist);
        self.time_diff_s.push(dt_sec);
    }

    fn is_empty(&self) -> bool {
        self.oco3_sounding_ids.is_empty()
    }
}

fn make_one_oco_match_vec(file_idx_oco2: u8, 
                          idx_oco2: usize, 
                          sid_oco2: u64, 
                          lon_oco2: f32, 
                          lat_oco2: f32, 
                          ts_oco2: f64, 
                          oco3: &OcoGeo, 
                          max_dist: f32, 
                          max_dt: f64) 
    -> Match2to3 {
    let mut oco3_matches = Match2to3::new(file_idx_oco2, idx_oco2 as u64, sid_oco2);

    let it = izip!(oco3.file_index.iter(),
                                                     oco3.sounding_id.iter(),
                                                     oco3.longitude.iter(),
                                                     oco3.latitude.iter(),
                                                     oco3.timestamp.iter()).enumerate();

    for (idx_oco3, (&file_idx_oco3, &sid_oco3, &lon_oco3, &lat_oco3, &ts_oco3)) in it {
        let this_dist = great_circle_distance(lon_oco2, lat_oco2, lon_oco3, lat_oco3);
        let this_delta_time = ts_oco2 - ts_oco3;

        if this_dist <= max_dist && this_delta_time.abs() < max_dt {
            oco3_matches.add_oco3_match(file_idx_oco3, idx_oco3, sid_oco3, this_dist, this_delta_time as f32);
        }
    }

    oco3_matches
}

// fn setup_progress_bar(n_match: u64, action: &str) -> ProgressBar {
//     let style = ProgressStyle::with_template(
//         &format!("{{bar}} {{human_pos}}/{{human_len}} {action}")
//     ).unwrap();

//     let pb = ProgressBar::new(n_match);
//     pb.set_style(style);
//     pb
// }

pub fn identify_groups_from_matched_soundings(matched_soundings: OcoMatches) -> OcoMatchGroups {
    fn update_sounding_inds(
        this_match: &Match2to3, 
        oco2_inds: &mut HashMap<u64, (u8, u64)>, 
        oco3_inds: &mut HashMap<u64, (u8, u64)>, 
        dist_mean: &mut HashMap<u64, RunningMean<f32>>,
        dt_mean: &mut HashMap<u64, RunningMean<f32>>
    ) {
        oco2_inds.insert(this_match.oco2_sounding_id, (this_match.oco2_file_index, this_match.oco2_sounding_index));
        dist_mean.insert(this_match.oco2_sounding_id, RunningMean::from_slice(&this_match.distance_km));
        dt_mean.insert(this_match.oco2_sounding_id, RunningMean::from_slice(&this_match.time_diff_s));
        for (&sid, &fid, &idx) in izip!(this_match.oco3_sounding_ids.iter(), this_match.oco3_file_indices.iter(), this_match.oco3_sounding_indices.iter()) {
            oco3_inds.insert(sid, (fid, idx));
        }
    }

    let mut match_sets: Vec<(HashSet<u64>, HashSet<u64>)> = Vec::new();
    let mut oco2_sounding_indices = HashMap::new();
    let mut oco3_sounding_indices = HashMap::new();
    let mut mean_dists = HashMap::new();
    let mut mean_time_diffs = HashMap::new();

    // It's important to iterate over ordered keys: when I let this be unordered, some groups that
    // should be one got split up, I think because the (non-overlapping) ends got put into separate 
    // groups before the middle soundings were handled. Now I have it set up so that when we create
    // the OcoMatches instance with `from_matches` that enforces ordering by OCO-2 sounding ID.

    // let pb = setup_progress_bar(matched_soundings.matches.len() as u64, "match vectors grouped");
    for m in matched_soundings.matches {
        // pb.inc(1);
        let oco3_row = &m.oco3_sounding_ids;
        let mut matched = false;
        for (oco2_idx_set, oco3_idx_set) in match_sets.iter_mut() {
            if oco3_row.iter().any(|i| oco3_idx_set.contains(i)) {
                oco2_idx_set.insert(m.oco2_sounding_id);
                oco3_idx_set.extend(oco3_row.iter());
                matched = true;
                break;
            }
        }

        if !matched {
            match_sets.push((
                HashSet::from([m.oco2_sounding_id]), HashSet::from_iter(oco3_row.iter().copied())
            ));
        }

        update_sounding_inds(&m, &mut oco2_sounding_indices, &mut oco3_sounding_indices, &mut mean_dists, &mut mean_time_diffs);
    }
    // pb.finish_with_message("  -> All matches grouped.");

    OcoMatchGroups { oco2_lite_files: matched_soundings.oco2_files,
                     oco3_lite_files: matched_soundings.oco3_files,
                     match_sets,
                     oco2_sounding_indices,
                     oco3_sounding_indices,
                     distances: mean_dists,
                     time_diffs: mean_time_diffs }
}