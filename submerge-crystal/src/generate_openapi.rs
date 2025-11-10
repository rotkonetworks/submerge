use utoipa::OpenApi as _;

use submerge_crystal::api::APIDoc;

fn main() {
    let openapi = APIDoc::openapi();
    let json = openapi.to_pretty_json().unwrap();
    println!("{json}");
}