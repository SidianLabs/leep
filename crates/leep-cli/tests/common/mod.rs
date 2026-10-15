//! In-memory mock of the Leep HTTP API: just the endpoints the CLI flows
//! need, enough to exercise commit, checkout and land without a real server.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use leep_core::api::*;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};

#[derive(Default)]
pub struct Data {
    pub blobs: HashMap<Hash, Vec<u8>>,
    pub changesets: HashMap<ChangesetId, Changeset>,
    pub files: HashMap<ChangesetId, BTreeMap<String, FileInfo>>,
    pub refs: BTreeMap<String, ChangesetId>,
    /// Blobs the server answers with garbage for.
    pub corrupt: HashSet<Hash>,
    /// Every hash received by `PUT /v1/blobs/{hash}`, in order.
    pub puts: Vec<Hash>,
    /// `(path, raw query string)` of every log, blame and provenance request.
    pub queries: Vec<(String, String)>,
    /// Every signature request received, with the changeset it was posted to.
    pub signatures: Vec<(ChangesetId, AddSignatureRequest)>,
    /// Refs that refuse to land a changeset without a recorded signature,
    /// like a server started with `--require-signatures`.
    pub require_signed: HashSet<String>,
}

pub type Shared = Arc<Mutex<Data>>;

pub struct Mock {
    pub url: String,
    pub data: Shared,
}

impl Data {
    pub fn insert(&mut self, cs: Changeset) -> ChangesetId {
        let mut files = cs
            .parents
            .first()
            .map(|p| self.files[p].clone())
            .unwrap_or_default();
        for (path, ch) in &cs.changes {
            match ch {
                FileChange::Modify {
                    content,
                    kind,
                    size,
                } => {
                    files.insert(
                        path.to_string(),
                        FileInfo {
                            path: path.clone(),
                            content: *content,
                            kind: *kind,
                            size: *size,
                        },
                    );
                }
                FileChange::Delete => {
                    files.remove(path.as_str());
                }
            }
        }
        let id = cs.id();
        self.files.insert(id, files);
        self.changesets.insert(id, cs);
        id
    }

    pub fn text(&self, f: &Option<FileInfo>) -> String {
        f.as_ref()
            .map(|f| String::from_utf8_lossy(&self.blobs[&f.content]).into_owned())
            .unwrap_or_default()
    }
}

/// Bearer tokens the mock accepts; `None` leaves it open.
#[derive(Clone)]
pub struct Tokens {
    pub write: &'static str,
    pub read: &'static str,
}

/// Mirrors the real server: 401 without a valid token, 403 for a read token
/// on anything but GET and `POST /v1/blobs/missing`.
async fn gate(
    State(t): State<Tokens>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let got = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let mutating =
        req.method() != axum::http::Method::GET && req.uri().path() != "/v1/blobs/missing";
    match got {
        Some(g) if g == t.write => next.run(req).await,
        Some(g) if g == t.read && !mutating => next.run(req).await,
        Some(g) if g == t.read => {
            err(StatusCode::FORBIDDEN, "token lacks write access").into_response()
        }
        _ => err(StatusCode::UNAUTHORIZED, "missing or invalid bearer token").into_response(),
    }
}

impl Mock {
    pub fn start() -> Mock {
        Self::start_with(None)
    }

    pub fn start_with_auth(tokens: Tokens) -> Mock {
        Self::start_with(Some(tokens))
    }

    fn start_with(tokens: Option<Tokens>) -> Mock {
        let data: Shared = Arc::default();
        let router = Router::new()
            .route("/v1/blobs/missing", post(missing))
            .route("/v1/blobs/{hash}", put(put_blob).get(get_blob))
            .route("/v1/changesets", post(create))
            .route(
                "/v1/changesets/{id}/signatures",
                post(add_signature).get(list_signatures),
            )
            .route("/v1/refs", get(list_refs))
            .route("/v1/refs/{*name}", get(get_ref).post(cas_ref))
            .route("/v1/resolve/{*rev}", get(resolve))
            .route("/v1/merge-base", get(merge_base))
            .route("/v1/files/{id}", get(files))
            .route("/v1/log/{id}", get(log))
            .route("/v1/diff", get(diff))
            .route("/v1/last-change/{id}", get(last_change))
            .route("/v1/blame/{id}", get(blame))
            .route("/v1/provenance/{id}", get(provenance))
            .route("/v1/search/{id}", get(search))
            .route("/v1/land", post(land));
        let app = match tokens {
            Some(t) => router.layer(axum::middleware::from_fn_with_state(t, gate)),
            None => router,
        }
        .with_state(data.clone());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let l = tokio::net::TcpListener::from_std(listener).unwrap();
                axum::serve(l, app).await.unwrap();
            });
        });
        Mock {
            url: format!("http://{addr}"),
            data,
        }
    }

    /// Create a changeset on top of `parent` from `(path, content, kind)`
    /// triples and point `main` at it.
    pub fn seed(
        &self,
        parent: Option<ChangesetId>,
        files: &[(&str, &str, FileKind)],
        deletes: &[&str],
    ) -> ChangesetId {
        let mut d = self.data.lock().unwrap();
        let mut changes = BTreeMap::new();
        for (p, body, kind) in files {
            let h = Hash::of_blob(body.as_bytes());
            d.blobs.insert(h, body.as_bytes().to_vec());
            changes.insert(
                RepoPath::new(*p).unwrap(),
                FileChange::Modify {
                    content: h,
                    kind: *kind,
                    size: body.len() as u64,
                },
            );
        }
        for p in deletes {
            changes.insert(RepoPath::new(*p).unwrap(), FileChange::Delete);
        }
        let id = d.insert(Changeset {
            parents: parent.into_iter().collect(),
            author: "seed".into(),
            timestamp: 1,
            message: "seed".into(),
            changes,
            extra: BTreeMap::new(),
        });
        d.refs.insert("main".into(), id);
        id
    }
}

type Resp<T> = Result<Json<T>, (StatusCode, Json<ErrorResponse>)>;

fn err(code: StatusCode, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (code, Json(ErrorResponse { error: msg.into() }))
}

async fn missing(
    State(s): State<Shared>,
    Json(r): Json<MissingBlobsRequest>,
) -> Json<MissingBlobsResponse> {
    let d = s.lock().unwrap();
    Json(MissingBlobsResponse {
        missing: r
            .hashes
            .into_iter()
            .filter(|h| !d.blobs.contains_key(h))
            .collect(),
    })
}

async fn put_blob(
    State(s): State<Shared>,
    Path(hash): Path<String>,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let h: Hash = hash
        .parse()
        .map_err(|_| err(StatusCode::BAD_REQUEST, "bad hash"))?;
    if Hash::of_blob(&body) != h {
        return Err(err(StatusCode::BAD_REQUEST, "hash mismatch"));
    }
    let mut d = s.lock().unwrap();
    d.puts.push(h);
    d.blobs.insert(h, body.to_vec());
    Ok(StatusCode::NO_CONTENT)
}

async fn get_blob(
    State(s): State<Shared>,
    Path(hash): Path<String>,
) -> Result<Vec<u8>, (StatusCode, Json<ErrorResponse>)> {
    let h: Hash = hash
        .parse()
        .map_err(|_| err(StatusCode::BAD_REQUEST, "bad hash"))?;
    let d = s.lock().unwrap();
    if d.corrupt.contains(&h) {
        return Ok(b"definitely not the right bytes".to_vec());
    }
    d.blobs
        .get(&h)
        .cloned()
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "no such blob"))
}

async fn create(
    State(s): State<Shared>,
    Json(cs): Json<Changeset>,
) -> Resp<CreateChangesetResponse> {
    let mut d = s.lock().unwrap();
    for p in &cs.parents {
        if !d.changesets.contains_key(p) {
            return Err(err(StatusCode::BAD_REQUEST, "unknown parent"));
        }
    }
    for ch in cs.changes.values() {
        if let FileChange::Modify { content, .. } = ch {
            if !d.blobs.contains_key(content) {
                return Err(err(StatusCode::BAD_REQUEST, "missing blob"));
            }
        }
    }
    let id = d.insert(cs);
    Ok(Json(CreateChangesetResponse { id }))
}

/// Records the request; the tests verify the signature themselves.
async fn add_signature(
    State(s): State<Shared>,
    Path(id): Path<String>,
    Json(req): Json<AddSignatureRequest>,
) -> Resp<AddSignatureResponse> {
    let id: ChangesetId = id
        .parse()
        .map_err(|_| err(StatusCode::BAD_REQUEST, "bad id"))?;
    let mut d = s.lock().unwrap();
    if !d.changesets.contains_key(&id) {
        return Err(err(StatusCode::NOT_FOUND, "no such changeset"));
    }
    d.signatures.push((id, req));
    Ok(Json(AddSignatureResponse {
        key_id: "mockkey".into(),
        name: "mock".into(),
    }))
}

async fn list_signatures(
    State(s): State<Shared>,
    Path(id): Path<String>,
) -> Resp<SignaturesResponse> {
    let id: ChangesetId = id
        .parse()
        .map_err(|_| err(StatusCode::BAD_REQUEST, "bad id"))?;
    let d = s.lock().unwrap();
    Ok(Json(SignaturesResponse {
        signatures: d
            .signatures
            .iter()
            .filter(|(c, _)| *c == id)
            .map(|(_, r)| SignatureInfo {
                key_id: "mockkey".into(),
                name: "mock".into(),
                public_key: r.public_key.clone(),
            })
            .collect(),
        verified: true,
        via: None,
    }))
}

async fn get_ref(State(s): State<Shared>, Path(name): Path<String>) -> Json<RefResponse> {
    let target = s.lock().unwrap().refs.get(&name).copied();
    Json(RefResponse { name, target })
}

async fn list_refs(State(s): State<Shared>) -> Json<RefsResponse> {
    Json(RefsResponse {
        refs: s.lock().unwrap().refs.clone(),
    })
}

/// Mirrors the server's ref rules: valid names, and tags never move.
async fn cas_ref(
    State(s): State<Shared>,
    Path(name): Path<String>,
    Json(r): Json<CasRefRequest>,
) -> Result<(StatusCode, Json<CasRefResponse>), (StatusCode, Json<ErrorResponse>)> {
    if r.new.is_some() {
        leep_core::refname::validate(&name)
            .map_err(|e| err(StatusCode::BAD_REQUEST, &e.to_string()))?;
    }
    if leep_core::refname::is_tag(&name) && r.expected.is_some() && r.new.is_some() {
        return Err(err(StatusCode::BAD_REQUEST, "tag is immutable"));
    }
    let mut d = s.lock().unwrap();
    let current = d.refs.get(&name).copied();
    if current != r.expected {
        return Ok((
            StatusCode::CONFLICT,
            Json(CasRefResponse { ok: false, current }),
        ));
    }
    match r.new {
        Some(id) => d.refs.insert(name, id),
        None => d.refs.remove(&name),
    };
    Ok((
        StatusCode::OK,
        Json(CasRefResponse {
            ok: true,
            current: r.new,
        }),
    ))
}

async fn resolve(State(s): State<Shared>, Path(rev): Path<String>) -> Resp<ResolveResponse> {
    let d = s.lock().unwrap();
    match d.refs.get(&rev) {
        Some(id) => Ok(Json(ResolveResponse { id: *id })),
        None => Err(err(StatusCode::NOT_FOUND, "unknown revision")),
    }
}

fn ancestors(d: &Data, from: ChangesetId) -> Vec<ChangesetId> {
    let mut seen = vec![from];
    let mut i = 0;
    while i < seen.len() {
        for p in &d.changesets[&seen[i]].parents {
            if !seen.contains(p) {
                seen.push(*p);
            }
        }
        i += 1;
    }
    seen
}

#[derive(serde::Deserialize)]
struct BaseQuery {
    a: ChangesetId,
    b: ChangesetId,
}

/// The first of `a`'s ancestors (nearest first) that `b` also has.
async fn merge_base(
    State(s): State<Shared>,
    Query(q): Query<BaseQuery>,
) -> Resp<MergeBaseResponse> {
    let d = s.lock().unwrap();
    for id in [q.a, q.b] {
        if !d.changesets.contains_key(&id) {
            return Err(err(StatusCode::NOT_FOUND, "no such changeset"));
        }
    }
    let theirs = ancestors(&d, q.b);
    let base = ancestors(&d, q.a).into_iter().find(|c| theirs.contains(c));
    Ok(Json(MergeBaseResponse { base }))
}

async fn files(
    State(s): State<Shared>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Resp<FilesResponse> {
    let id: ChangesetId = id
        .parse()
        .map_err(|_| err(StatusCode::BAD_REQUEST, "bad id"))?;
    let d = s.lock().unwrap();
    let all = d
        .files
        .get(&id)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "no such changeset"))?;
    let prefix = q.get("prefix").map(String::as_str).unwrap_or("");
    Ok(Json(FilesResponse {
        files: all
            .values()
            .filter(|f| f.path.is_within(prefix))
            .cloned()
            .collect(),
    }))
}

/// Fast-forward when possible; otherwise replay the changes on the head,
/// reporting a conflict when both sides touched the same path differently.
async fn land(State(s): State<Shared>, Json(r): Json<LandRequest>) -> Resp<LandResponse> {
    let mut d = s.lock().unwrap();
    let cs = d
        .changesets
        .get(&r.changeset)
        .cloned()
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "no such changeset"))?;
    if d.require_signed.contains(&r.target_ref)
        && !d.signatures.iter().any(|(c, _)| *c == r.changeset)
    {
        return Err(err(
            StatusCode::BAD_REQUEST,
            &format!("changeset {} is not signed by a trusted key", r.changeset),
        ));
    }
    let head = d.refs.get(&r.target_ref).copied();
    let parent = cs.parents.first().copied();
    let Some(head) = head.filter(|h| Some(*h) != parent) else {
        d.refs.insert(r.target_ref, r.changeset);
        return Ok(Json(LandResponse::Landed {
            landed: r.changeset,
            rebased: false,
            attempts: 1,
        }));
    };
    let base_files = parent.map(|p| d.files[&p].clone()).unwrap_or_default();
    let head_files = d.files[&head].clone();
    let mut conflicts = vec![];
    let mut clean = std::collections::BTreeMap::new();
    for (path, ch) in &cs.changes {
        let key = path.to_string();
        let (b, h) = (base_files.get(&key), head_files.get(&key));
        if b.map(|f| f.content) == h.map(|f| f.content) {
            clean.insert(path.clone(), ch.clone());
            continue;
        }
        let theirs = match ch {
            FileChange::Modify {
                content,
                kind,
                size,
            } => Some(FileInfo {
                path: path.clone(),
                content: *content,
                kind: *kind,
                size: *size,
            }),
            FileChange::Delete => None,
        };
        if theirs.as_ref().map(|f| f.content) == h.map(|f| f.content) {
            continue;
        }
        let ours = h.cloned();
        let merged = format!(
            "<<<<<<< ours\n{}=======\n{}>>>>>>> theirs\n",
            d.text(&ours),
            d.text(&theirs)
        );
        conflicts.push(Conflict {
            path: path.clone(),
            base: b.cloned(),
            ours,
            theirs,
            merged_with_markers: Some(merged),
        });
    }
    if !conflicts.is_empty() {
        return Ok(Json(LandResponse::Conflicts {
            onto: head,
            conflicts,
            clean,
        }));
    }
    let mut rebased = cs;
    rebased.parents = vec![head];
    let id = d.insert(rebased);
    d.refs.insert(r.target_ref, id);
    Ok(Json(LandResponse::Landed {
        landed: id,
        rebased: true,
        attempts: 1,
    }))
}

fn pairs(raw: &Option<String>) -> Vec<(String, String)> {
    let raw = raw.as_deref().unwrap_or("");
    let mut out = Vec::new();
    for kv in raw.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        // Enough decoding for the `:` the client escapes.
        out.push((k.to_string(), v.replace("%3A", ":").replace("%3a", ":")));
    }
    out
}

fn record(d: &mut Data, path: &str, raw: &Option<String>) {
    d.queries
        .push((path.to_string(), raw.clone().unwrap_or_default()));
}

/// Newest-first first-parent log honoring repeated `extra` (`k:v` or `k`)
/// and `author`, like the real server.
async fn log(
    State(s): State<Shared>,
    Path(id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Resp<LogResponse> {
    let mut d = s.lock().unwrap();
    record(&mut d, "log", &raw);
    let mut cur: Option<ChangesetId> = Some(
        id.parse()
            .map_err(|_| err(StatusCode::BAD_REQUEST, "bad id"))?,
    );
    let q = pairs(&raw);
    let mut entries = Vec::new();
    while let Some(c) = cur {
        let cs = d
            .changesets
            .get(&c)
            .ok_or_else(|| err(StatusCode::NOT_FOUND, "no such changeset"))?
            .clone();
        let ok = q.iter().all(|(k, v)| match k.as_str() {
            "author" => cs.author.to_lowercase().contains(&v.to_lowercase()),
            "extra" => match v.split_once(':') {
                Some((k, want)) => cs.extra.get(k).is_some_and(|h| h == want),
                None => cs.extra.contains_key(v),
            },
            _ => true,
        });
        cur = cs.parents.first().copied();
        if ok {
            let follow = q.iter().any(|(k, v)| k == "follow" && v == "true");
            entries.push(ChangesetInfo {
                id: c,
                generation: 0,
                root_tree: Hash::of_blob(b""),
                changeset: cs,
                path_at: follow
                    .then(|| q.iter().find(|(k, _)| k == "path"))
                    .flatten()
                    .and_then(|(_, p)| RepoPath::new(p.as_str()).ok()),
                renamed: None,
            });
        }
    }
    Ok(Json(LogResponse { entries }))
}

/// File-level diff between two changesets, with rename detection when asked,
/// like the real server (`renames=true`, `rename_threshold`).
async fn diff(State(s): State<Shared>, RawQuery(raw): RawQuery) -> Resp<DiffResponse> {
    let mut d = s.lock().unwrap();
    record(&mut d, "diff", &raw);
    let q: HashMap<String, String> = pairs(&raw).into_iter().collect();
    let id = |k: &str| -> Result<Option<ChangesetId>, (StatusCode, Json<ErrorResponse>)> {
        q.get(k)
            .map(|v| {
                v.parse()
                    .map_err(|_| err(StatusCode::BAD_REQUEST, "bad id"))
            })
            .transpose()
    };
    let from = id("from")?;
    let to = id("to")?.ok_or_else(|| err(StatusCode::BAD_REQUEST, "missing to"))?;
    let none = BTreeMap::new();
    let old = from.map_or(&none, |f| &d.files[&f]);
    let new = d
        .files
        .get(&to)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "no such changeset"))?;
    let mut entries = Vec::new();
    for path in old
        .keys()
        .chain(new.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        let (o, n) = (old.get(path), new.get(path));
        if o.map(|f| (f.content, f.kind)) != n.map(|f| (f.content, f.kind)) {
            entries.push(DiffEntry {
                path: RepoPath::new(path.as_str()).unwrap(),
                old: o.cloned(),
                new: n.cloned(),
            });
        }
    }
    let mut resp = DiffResponse {
        entries,
        ..Default::default()
    };
    if q.get("renames").is_some_and(|v| v == "true") {
        let mut opts = leep_core::rename::RenameOptions::default();
        if let Some(t) = q.get("rename_threshold") {
            opts.threshold = t
                .parse()
                .map_err(|_| err(StatusCode::BAD_REQUEST, "bad rename_threshold"))?;
        }
        let (deleted, added) = leep_core::rename::split_entries(&resp.entries);
        let found =
            leep_core::rename::detect_renames(deleted, added, opts, |h| d.blobs.get(h).cloned());
        resp.renames = found.renames;
        resp.renames_truncated = found.truncated;
    }
    Ok(Json(resp))
}

/// Answers with the requested changeset itself, which is all the CLI tests
/// need to see the client build the request and decode the response.
async fn last_change(
    State(s): State<Shared>,
    Path(id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Resp<LastChangeResponse> {
    let mut d = s.lock().unwrap();
    record(&mut d, "last-change", &raw);
    let id: ChangesetId = id
        .parse()
        .map_err(|_| err(StatusCode::BAD_REQUEST, "bad id"))?;
    let cs = d
        .changesets
        .get(&id)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "no such changeset"))?
        .clone();
    let path = pairs(&raw)
        .into_iter()
        .find(|(k, _)| k == "path")
        .map(|(_, v)| v.replace("%2F", "/"))
        .ok_or_else(|| err(StatusCode::BAD_REQUEST, "missing path"))?;
    Ok(Json(LastChangeResponse {
        path,
        changeset: ChangesetInfo {
            id,
            generation: 0,
            root_tree: Hash::of_blob(b""),
            changeset: cs,
            path_at: None,
            renamed: None,
        },
        concurrent: vec![],
    }))
}

/// One summary per line owner; every line is attributed to the requested
/// changeset, which is all the CLI tests need.
async fn blame(
    State(s): State<Shared>,
    Path(id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Resp<BlameResponse> {
    let mut d = s.lock().unwrap();
    record(&mut d, "blame", &raw);
    let id: ChangesetId = id
        .parse()
        .map_err(|_| err(StatusCode::BAD_REQUEST, "bad id"))?;
    let cs = d
        .changesets
        .get(&id)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "no such changeset"))?;
    let summary = ChangesetSummary {
        author: cs.author.clone(),
        timestamp: cs.timestamp,
        message_first_line: cs.message.lines().next().unwrap_or("").into(),
        agent: cs
            .extra
            .iter()
            .filter(|(k, _)| k.starts_with("agent."))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    };
    Ok(Json(BlameResponse {
        path: RepoPath::new("a.txt").unwrap(),
        lines: vec![BlameLine {
            changeset: id,
            origin_line: 1,
            text: "hello".into(),
        }],
        changesets: BTreeMap::from([(id, summary)]),
    }))
}

/// Counts first-parent changesets by author; just enough to see the client
/// decode the response.
/// Literal substring search over a revision's text files: just enough to
/// check what the client asks for and how it decodes the answer.
async fn search(
    State(s): State<Shared>,
    Path(id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Resp<SearchResponse> {
    let mut d = s.lock().unwrap();
    record(&mut d, "search", &raw);
    let id: ChangesetId = id
        .parse()
        .map_err(|_| err(StatusCode::BAD_REQUEST, "bad id"))?;
    let q: HashMap<String, String> = pairs(&raw).into_iter().collect();
    let needle = q.get("q").cloned().unwrap_or_default();
    let ci = q.get("i").is_some_and(|v| v == "true");
    let limit: usize = q.get("limit").map_or(100, |l| l.parse().unwrap());
    let prefix = q.get("path").cloned().unwrap_or_default();
    let files = d
        .files
        .get(&id)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, "no such changeset"))?;
    let mut matches = Vec::new();
    for (path, f) in files {
        if !f.path.is_within(&prefix) {
            continue;
        }
        let text = String::from_utf8_lossy(&d.blobs[&f.content]).into_owned();
        for (i, line) in text.lines().enumerate() {
            let hit = if ci {
                line.to_lowercase().find(&needle.to_lowercase())
            } else {
                line.find(&needle)
            };
            if let Some(start) = hit {
                matches.push(SearchMatch {
                    path: RepoPath::new(path.as_str()).unwrap(),
                    line_number: i as u64 + 1,
                    line: line.to_string(),
                    ranges: vec![SearchRange {
                        start: start as u32,
                        end: (start + needle.len()) as u32,
                    }],
                });
            }
        }
    }
    let truncated = matches.len() > limit;
    matches.truncate(limit);
    Ok(Json(SearchResponse {
        matches,
        truncated,
        stats: SearchStats::default(),
    }))
}

async fn provenance(
    State(s): State<Shared>,
    Path(id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Resp<ProvenanceResponse> {
    let mut d = s.lock().unwrap();
    record(&mut d, "provenance", &raw);
    let mut cur: Option<ChangesetId> = Some(
        id.parse()
            .map_err(|_| err(StatusCode::BAD_REQUEST, "bad id"))?,
    );
    let mut by_author: BTreeMap<String, u64> = BTreeMap::new();
    let mut scanned = 0;
    while let Some(c) = cur {
        let cs = &d.changesets[&c];
        *by_author.entry(cs.author.clone()).or_default() += 1;
        scanned += 1;
        cur = cs.parents.first().copied();
    }
    Ok(Json(ProvenanceResponse {
        agents: vec![],
        humans: by_author
            .into_iter()
            .map(|(author, changesets)| HumanStat {
                author,
                changesets,
                files_touched: 0,
                first: 0,
                last: 0,
            })
            .collect(),
        scanned,
        truncated: false,
    }))
}
