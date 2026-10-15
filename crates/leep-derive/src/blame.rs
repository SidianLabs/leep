//! Line-level blame over first-parent history, cached as derived data.

use bytes::Bytes;
use leep_core::api::{BlameLine, BlameResponse, FileInfo};
use leep_core::{ChangesetId, Error, Hash, RepoPath, Result};
use similar::{ChangeTag, TextDiff};

use crate::Repo;

fn cache_key(id: ChangesetId, path: &RepoPath) -> Hash {
    let mut b = id.0.as_bytes().to_vec();
    b.extend_from_slice(path.as_str().as_bytes());
    Hash::of_object("blame", &b)
}

fn strip_eol(line: &str) -> String {
    let l = line.strip_suffix('\n').unwrap_or(line);
    l.strip_suffix('\r').unwrap_or(l).to_string()
}

impl Repo {
    async fn utf8_content(&self, info: &FileInfo) -> Result<String> {
        let data = self
            .blobs()
            .get(&info.content)
            .await?
            .ok_or_else(|| Error::Storage(format!("blob {} missing", info.content)))?;
        String::from_utf8(data.to_vec())
            .map_err(|_| Error::Invalid(format!("{} is not valid UTF-8", info.path)))
    }

    /// Attribute each line of `path` at `id` to the changeset that introduced
    /// it, following first parents.
    pub async fn blame(&self, id: ChangesetId, path: &RepoPath) -> Result<BlameResponse> {
        let tip = self
            .lookup_file(id, path)
            .await?
            .ok_or_else(|| Error::NotFound(format!("file {path} at {id}")))?;
        // Walk first parents collecting the changesets that changed the file,
        // stopping at its creation or at a cached blame.
        let mut chain: Vec<(ChangesetId, FileInfo)> = Vec::new();
        let mut cached: Option<(BlameResponse, FileInfo)> = None;
        let (mut cur, mut cur_info) = (id, tip);
        loop {
            let cs = self.get_changeset(cur).await?;
            let parent = cs.parents.first().copied();
            let parent_info = match parent {
                Some(p) => self.lookup_file(p, path).await?,
                None => None,
            };
            let changed = parent_info
                .as_ref()
                .is_none_or(|p| p.content != cur_info.content);
            if changed {
                if let Some(v) = self
                    .meta()
                    .get_derived("blame", &cache_key(cur, path))
                    .await?
                {
                    cached = Some((postcard::from_bytes(&v)?, cur_info));
                    break;
                }
                chain.push((cur, cur_info.clone()));
            }
            match (parent, parent_info) {
                (Some(p), Some(pi)) => (cur, cur_info) = (p, pi),
                _ => break,
            }
        }
        let latest = chain.first().map(|(c, _)| *c);
        let (mut lines, mut old_text) = match cached {
            Some((resp, info)) => {
                if chain.is_empty() {
                    return Ok(resp);
                }
                (resp.lines, self.utf8_content(&info).await?)
            }
            None => (Vec::new(), String::new()),
        };
        for (cid, info) in chain.into_iter().rev() {
            let new_text = self.utf8_content(&info).await?;
            let diff = TextDiff::from_lines(&old_text, &new_text);
            let mut next = Vec::new();
            for ch in diff.iter_all_changes() {
                match ch.tag() {
                    ChangeTag::Delete => {}
                    ChangeTag::Equal => {
                        next.push(lines[ch.old_index().expect("old index")].clone());
                    }
                    ChangeTag::Insert => next.push(BlameLine {
                        changeset: cid,
                        origin_line: ch.new_index().expect("new index") as u32 + 1,
                        text: strip_eol(ch.value()),
                    }),
                }
            }
            lines = next;
            old_text = new_text;
        }
        let resp = BlameResponse {
            path: path.clone(),
            lines,
        };
        let value = Bytes::from(postcard::to_stdvec(&resp)?);
        for key_id in latest.into_iter().chain([id]) {
            self.meta()
                .put_derived("blame", &cache_key(key_id, path), value.clone())
                .await?;
        }
        Ok(resp)
    }
}
