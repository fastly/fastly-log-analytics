use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    env,
    fmt::Write as _,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    thread,
};

#[path = "src/csp.rs"]
#[allow(dead_code)]
mod csp;

static NEXT_TMP_ID: AtomicUsize = AtomicUsize::new(0);

/// Maps static asset file extensions (including MapLibre `.mjs` worker modules and
/// `.geojson` / `.topojson` vector geography files from `frontend/out`) to MIME types.
fn mime_for(ext: Option<&str>) -> &'static str {
    match ext {
        Some("html") => "text/html",
        Some("js" | "mjs") => "application/javascript",
        Some("css") => "text/css",
        Some("json" | "map" | "geojson" | "topojson") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("txt") => "text/plain",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

fn compressible(mime: &str) -> bool {
    mime.starts_with("text/")
        || matches!(
            mime,
            "application/javascript"
                | "application/json"
                | "application/geo+json"
                | "image/svg+xml"
                | "image/x-icon"
                | "application/wasm"
        )
}

#[derive(Clone, Copy)]
enum Codec {
    Gzip,
    Brotli,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn write_if_missing(target: &Path, bytes: &[u8]) {
    if target.is_file() {
        return;
    }
    let tmp_id = NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed);
    let tmp = target.with_extension(format!("tmp.{}.{tmp_id}", std::process::id()));
    fs::write(&tmp, bytes).expect("write cached static asset");
    fs::rename(&tmp, target).expect("commit cached static asset");
}

/// Writes `raw` compressed with `codec` under `dir`, keying by content SHA-256 so fresh
/// `next build` exports reuse previously compressed assets. Returns `None` when compression
/// would not shrink the asset.
fn precompress(raw: &[u8], dir: &Path, digest: &str, codec: Codec) -> Option<PathBuf> {
    let suffix = match codec {
        Codec::Gzip => "gz",
        Codec::Brotli => "br",
    };
    let target = dir.join(format!("{digest}.{suffix}"));
    if !target.is_file() {
        let encoded = match codec {
            Codec::Gzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
                encoder.write_all(raw).expect("gzip static asset");
                encoder.finish().expect("finish gzip static asset")
            }
            Codec::Brotli => {
                let params = brotli::enc::BrotliEncoderParams {
                    quality: 11,
                    lgwin: 22,
                    size_hint: raw.len(),
                    ..Default::default()
                };
                let mut encoded = Vec::new();
                brotli::BrotliCompress(&mut &raw[..], &mut encoded, &params)
                    .expect("brotli static asset");
                encoded
            }
        };
        write_if_missing(&target, &encoded);
    }
    let size = fs::metadata(&target)
        .expect("precompressed asset size")
        .len();
    (size < raw.len() as u64).then_some(target)
}

fn optional_bytes(path: Option<PathBuf>) -> String {
    match path {
        Some(path) => format!("Some(include_bytes!({path:?}))"),
        None => "None".to_owned(),
    }
}

type AssetEntry = (String, Vec<String>);

fn asset_entry(root: &Path, path: &Path, cache_dir: &Path) -> AssetEntry {
    let relative = path.strip_prefix(root).expect("static export path");
    let mime = mime_for(path.extension().and_then(|value| value.to_str()));
    let raw = fs::read(path).expect("read static asset");
    let digest = sha256_hex(&raw);
    let script_hashes = if mime == "text/html" {
        csp::inline_scripts(&raw)
            .into_iter()
            .map(csp::script_hash)
            .collect()
    } else {
        Vec::new()
    };
    let (mut keep_identity, mut gzip, mut br) = (true, None, None);
    if compressible(mime) {
        gzip = precompress(&raw, cache_dir, &digest, Codec::Gzip);
        if mime == "application/wasm" {
            // Brotli 11 of DuckDB's 36 MB module takes minutes in an unoptimized build script.
            // Its identity copy would dominate the package, so it ships only gzipped.
            if gzip.is_some() {
                keep_identity = false;
            }
        } else {
            br = precompress(&raw, cache_dir, &digest, Codec::Brotli);
        }
    }
    let identity = keep_identity.then(|| {
        let target = cache_dir.join(format!("{digest}.raw"));
        write_if_missing(&target, &raw);
        target
    });
    let entry = format!(
        "crate::site::Asset {{ path: {:?}, content_type: {:?}, identity: {}, gzip: {}, br: {} }}",
        format!("/{}", relative.display()),
        mime,
        optional_bytes(identity),
        optional_bytes(gzip),
        optional_bytes(br),
    );
    (entry, script_hashes)
}

fn collect_paths(directory: &Path, out: &mut Vec<PathBuf>) {
    let mut files: Vec<_> = fs::read_dir(directory)
        .expect("read static export")
        .map(|entry| entry.expect("read static export entry").path())
        .collect();
    files.sort();
    for path in files {
        if path.is_dir() {
            collect_paths(&path, out);
        } else {
            out.push(path);
        }
    }
}

fn collect(
    root: &Path,
    cache_dir: &Path,
    entries: &mut Vec<String>,
    script_hashes: &mut BTreeSet<String>,
) {
    fs::create_dir_all(cache_dir).expect("create precompressed asset dir");
    let cache_dir = cache_dir
        .canonicalize()
        .expect("precompressed asset dir absolute path");
    let mut paths = Vec::new();
    collect_paths(root, &mut paths);
    if paths.is_empty() {
        return;
    }
    // Schedule largest assets first so multi-MB Wasm/JS bundles never become a tail straggler.
    let mut order: Vec<usize> = (0..paths.len()).collect();
    order.sort_by_key(|&idx| {
        std::cmp::Reverse(fs::metadata(&paths[idx]).map(|m| m.len()).unwrap_or(0))
    });
    let workers = thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4)
        .min(paths.len());
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<AssetEntry>>> =
        Mutex::new((0..paths.len()).map(|_| None).collect());
    thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let pos = next.fetch_add(1, Ordering::Relaxed);
                if pos >= order.len() {
                    break;
                }
                let idx = order[pos];
                let item = asset_entry(root, &paths[idx], &cache_dir);
                results.lock().expect("asset results lock")[idx] = Some(item);
            });
        }
    });
    for item in results
        .into_inner()
        .expect("asset results")
        .into_iter()
        .flatten()
    {
        entries.push(item.0);
        script_hashes.extend(item.1);
    }
}

fn main() {
    println!("cargo:rerun-if-changed=../../frontend/out");
    println!("cargo:rerun-if-changed=web/out");
    println!("cargo:rerun-if-changed=src/csp.rs");
    println!("cargo:rerun-if-env-changed=FLP_BUILD_SHA");
    println!("cargo:rerun-if-env-changed=FLP_ASSET_CACHE_DIR");
    println!("cargo:rerun-if-env-changed=FLP_WEB_OUT");
    let out_dir = Path::new(&env::var("OUT_DIR").expect("build output")).to_path_buf();
    let cache_dir = env::var_os("FLP_ASSET_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| out_dir.join("precompressed"));
    let root = env::var_os("FLP_WEB_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let frontend_out = PathBuf::from("../../frontend/out");
            if frontend_out.is_dir() {
                frontend_out
            } else {
                PathBuf::from("web/out")
            }
        });
    let mut entries = Vec::new();
    let mut script_hashes = BTreeSet::new();
    if root.is_dir() {
        collect(&root, &cache_dir, &mut entries, &mut script_hashes);
    } else if env::var("TARGET").expect("build target") == "wasm32-wasip1"
        && env::var("PROFILE").expect("build profile") == "release"
    {
        panic!("Build the frontend with STATIC_EXPORT=1 before building the Compute package");
    }
    let destination = out_dir.join("assets.rs");
    let rendered = format!(
        "pub static ASSETS: &[crate::site::Asset] = &[{}];\n\
         pub static INLINE_SCRIPT_HASHES: &[&str] = &{:?};",
        entries.join(",\n"),
        script_hashes.into_iter().collect::<Vec<_>>()
    );
    if fs::read_to_string(&destination).ok().as_deref() != Some(rendered.as_str()) {
        fs::write(destination, rendered).expect("write static asset index");
    }
    let sha = env::var("FLP_BUILD_SHA").unwrap_or_else(|_| "uncommitted".to_owned());
    println!("cargo:rustc-env=FLP_BUILD_SHA={sha}");
}
