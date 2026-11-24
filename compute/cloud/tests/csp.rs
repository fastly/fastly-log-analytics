use fastly_log_analytics::{csp, ASSETS, INLINE_SCRIPT_HASHES};
use std::{collections::BTreeSet, fs, path::Path};

fn html_hashes<'a>(pages: impl Iterator<Item = &'a [u8]>) -> BTreeSet<String> {
    pages
        .flat_map(|html| csp::inline_scripts(html).into_iter().map(csp::script_hash))
        .collect()
}

#[test]
fn hash_list_is_exactly_the_inline_scripts_of_every_embedded_page() {
    let pages = ASSETS
        .iter()
        .filter(|asset| asset.content_type == "text/html")
        .map(|asset| asset.identity.expect("pages ship an identity body"));
    let expected = html_hashes(pages);
    let generated: BTreeSet<String> = INLINE_SCRIPT_HASHES.iter().map(|h| h.to_string()).collect();
    assert_eq!(generated, expected);
    assert_eq!(generated.len(), INLINE_SCRIPT_HASHES.len());
}

#[test]
fn hash_list_matches_the_static_export_on_disk() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let frontend_out = manifest_dir.join("../../frontend/out");
    let root = if frontend_out.is_dir() {
        frontend_out
    } else {
        manifest_dir.join("web/out")
    };
    if !root.is_dir() {
        assert!(INLINE_SCRIPT_HASHES.is_empty());
        return;
    }
    let mut pages = Vec::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "html") {
                pages.push(fs::read(path).unwrap());
            }
        }
    }
    if pages.is_empty() {
        assert!(INLINE_SCRIPT_HASHES.is_empty());
        return;
    }
    let on_disk = html_hashes(pages.iter().map(Vec::as_slice));
    assert!(
        !on_disk.is_empty(),
        "Next always inlines its bootstrap scripts"
    );
    let generated: BTreeSet<String> = INLINE_SCRIPT_HASHES.iter().map(|h| h.to_string()).collect();
    assert_eq!(generated, on_disk);
}

#[test]
fn csp_policy_shares_base_directives_with_frontend_proxy_ts() {
    let proxy_ts =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../frontend/proxy.ts"))
            .expect("read frontend/proxy.ts");
    let rendered = csp::policy(&[]);
    for shared_directive in [
        "default-src 'self'",
        "worker-src 'self' blob:",
        "style-src 'self' 'unsafe-inline'",
        "img-src 'self' data: blob:",
        "font-src 'self' data:",
        "connect-src 'self'",
        "frame-ancestors 'none'",
        "base-uri 'self'",
        "form-action 'self'",
    ] {
        assert!(
            rendered.contains(shared_directive),
            "csp.rs policy missing {shared_directive}: {rendered}"
        );
        assert!(
            proxy_ts.contains(shared_directive),
            "frontend/proxy.ts missing {shared_directive}"
        );
    }
}
