use std::collections::HashMap;
use std::os::linux::raw::ino_t;

pub trait ColMetadata {

}

struct NumericColMetadata {
    name: String,
    col_type: String,
    columns: HashMap<String, Box<dyn ColMetadata>>,
    mean: f64,
    min: String,
    max: String
}

impl ColMetadata for NumericColMetadata {}

struct CategoricalColMetadata {
    name: String,
    col_type: String,
    columns: HashMap<String, Box<dyn ColMetadata>>,
}

impl ColMetadata for CategoricalColMetadata {}