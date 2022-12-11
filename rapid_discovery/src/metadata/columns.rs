use std::collections::HashMap;
use std::os::linux::raw::ino_t;
use polars::prelude::{LazyFrame, max, min};

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

pub(crate) fn construct_column_list_from_lazy_df(df: LazyFrame) -> HashMap<String ,Box<dyn ColMetadata>> {
    let mut df_loc = df.clone();
    let schema = df.schema().unwrap();
    let mut cols = HashMap::new();
    let col_arr: Vec<&String>= schema.iter().map(|res| res.0).collect();

/*    let col_vals = df.clone().mean().min().max().collect();
*/
/*    for col in schema.iter() {
        let col_name = col.0;
        df_loc = df_loc.select([
/*            min("president").alias(&*format!("{}-min", col.0)),
            max(&*col_name).alias(&*format!("{}-min", col.0))*/
            min("president").alias("president")
        ])
    }*/

    df_loc = df_loc.select([
/*            min("president").alias(&*format!("{}-min", col.0)),
            max(&*col_name).alias(&*format!("{}-min", col.0))*/
            min("president").alias("president")
        ]);
    let collected_df = df_loc.collect();

    let res = match collected_df {
        Ok(v) => v,
        Err(e) => panic!("{:#?}", e),
    };
    println!("{}", res);

    cols
}