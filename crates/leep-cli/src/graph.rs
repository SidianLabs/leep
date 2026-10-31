//! `leep log --graph`: the ASCII commit graph drawn left of each entry, in
//! the style of `git log --graph` (`*`, `|`, `/`, `\`, `_`, and `-.` for an
//! octopus merge).
//!
//! The log arrives newest first by generation, so every changeset comes
//! after all of its children that are shown. [`Graph`] keeps one lane per
//! changeset still expected below; feeding it the entries in order, across
//! every page of one listing, draws the whole graph.

/// The rows drawn for one entry. Trailing spaces are trimmed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Rows {
    /// Printed alone before the entry: shifting lanes right to make room for
    /// an octopus merge's parents.
    pub before: Vec<String>,
    /// Beside the entry's first line: `*` in the entry's lane.
    pub commit: String,
    /// Beside the following lines, or alone once they run out: parents
    /// fanning out, lanes ending, and lanes that meet merging.
    pub after: Vec<String>,
    /// Beside any further lines: the lanes as they now stand.
    pub padding: String,
}

/// Lane state carried from one entry to the next.
#[derive(Debug)]
pub struct Graph<T> {
    /// The changeset each lane expects next, left to right; never repeats.
    cols: Vec<T>,
}

impl<T: PartialEq + Clone> Default for Graph<T> {
    fn default() -> Self {
        Graph { cols: Vec::new() }
    }
}

/// One row being drawn, two characters per lane.
struct Canvas(Vec<char>);

impl Canvas {
    fn new() -> Self {
        Canvas(Vec::new())
    }

    fn put(&mut self, at: usize, c: char) {
        if self.0.len() <= at {
            self.0.resize(at + 1, ' ');
        }
        self.0[at] = c;
    }

    fn free(&self, at: usize) -> bool {
        self.0.get(at).is_none_or(|c| *c == ' ')
    }

    /// Something other than lanes going straight down.
    fn moves(&self) -> bool {
        self.0.iter().any(|c| !matches!(c, ' ' | '|'))
    }

    fn done(self) -> String {
        let s: String = self.0.into_iter().collect();
        s.trim_end().to_string()
    }
}

fn straight(n: usize) -> String {
    let mut c = Canvas::new();
    for i in 0..n {
        c.put(2 * i, '|');
    }
    c.done()
}

impl<T: PartialEq + Clone> Graph<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Draw the next entry of the log and move the lanes past it.
    pub fn next(&mut self, id: &T, parents: &[T]) -> Rows {
        let idx = match self.cols.iter().position(|c| c == id) {
            Some(i) => i,
            None => {
                self.cols.push(id.clone());
                self.cols.len() - 1
            }
        };
        let mut ps: Vec<&T> = Vec::new();
        for p in parents {
            if p != id && !ps.contains(&p) {
                ps.push(p);
            }
        }
        // Parents already in a lane to the left are joined there; the rest
        // take lanes in place of this one, in parent order.
        let left: Vec<usize> = ps
            .iter()
            .filter_map(|p| self.cols[..idx].iter().position(|c| c == *p))
            .collect();
        let new: Vec<T> = ps
            .iter()
            .filter(|p| !self.cols[..idx].contains(p))
            .map(|p| (*p).clone())
            .collect();
        let m = new.len();
        let right = self.cols.len() - idx - 1;
        // An octopus merge draws `*-.`: make room by shifting the lanes to
        // its right first, one column per row.
        let shift = if right > 0 { m.saturating_sub(2) } else { 0 };

        let mut rows = Rows::default();
        for r in 0..shift {
            let mut c = Canvas::new();
            for i in 0..=idx {
                c.put(2 * i, '|');
            }
            for i in idx + 1..self.cols.len() {
                c.put(2 * (i + r) + 1, '\\');
            }
            rows.before.push(c.done());
        }

        let mut c = Canvas::new();
        for i in 0..idx {
            c.put(2 * i, '|');
        }
        c.put(2 * idx, '*');
        if m >= 3 {
            for at in 2 * idx + 1..2 * (idx + m - 2) {
                c.put(at, '-');
            }
            c.put(2 * (idx + m - 2), '.');
        }
        for i in idx + 1..self.cols.len() {
            c.put(2 * (i + shift), '|');
        }
        rows.commit = c.done();

        // Parents fan out, the lane ends, or it joins lanes to the left.
        let mut c = Canvas::new();
        for i in 0..idx {
            c.put(2 * i, '|');
        }
        if m >= 1 {
            c.put(2 * idx, '|');
        }
        for k in 1..m {
            c.put(2 * (idx + k) - 1, '\\');
        }
        for &j in &left {
            c.put(2 * idx - 1, '/');
            for at in (2 * j + 1..2 * idx - 1).step_by(2) {
                c.put(at, '_');
            }
        }
        for i in idx + 1..self.cols.len() {
            let cur = i + shift;
            match m {
                0 => c.put(2 * cur - 1, '/'),
                1 => c.put(2 * cur, '|'),
                _ => c.put(2 * cur + 1, '\\'),
            }
        }
        if c.moves() {
            rows.after.push(c.done());
        }

        // Lanes now expecting the same changeset merge into the leftmost.
        let mut lanes: Vec<(usize, T)> = self.cols[..idx]
            .iter()
            .cloned()
            .chain(new)
            .chain(self.cols[idx + 1..].iter().cloned())
            .enumerate()
            .collect();
        let mut fin: Vec<T> = Vec::new();
        for (_, t) in &lanes {
            if !fin.contains(t) {
                fin.push(t.clone());
            }
        }
        let target = |t: &T| fin.iter().position(|f| f == t).unwrap_or(0);
        let settled = |lanes: &[(usize, T)]| {
            lanes.len() == fin.len() && lanes.iter().all(|(c, t)| *c == target(t))
        };
        // Each row settles at least the leftmost unsettled lane; the bound
        // is only a guard.
        let mut guard = 2 * lanes.len() + 2;
        while !settled(&lanes) && guard > 0 {
            guard -= 1;
            let mut c = Canvas::new();
            let mut next: Vec<(usize, T)> = Vec::new();
            for (col, t) in lanes {
                if let Some(&(dest, _)) = next.iter().find(|(_, n)| *n == t) {
                    // A twin to the left: cross to it with `_` over the
                    // lanes between, as git draws `|_|/`. Lanes drawn so far
                    // are all left of this one, so a free even column in
                    // the way holds no lane.
                    let path = 2 * dest + 1..2 * col - 1;
                    let odd = path.clone().step_by(2);
                    if dest < col && c.free(2 * col - 1) && odd.clone().all(|a| c.free(a)) {
                        c.put(2 * col - 1, '/');
                        for a in path {
                            if c.free(a) {
                                c.put(a, '_');
                            }
                        }
                        continue;
                    }
                } else if col > target(&t)
                    && next.last().is_none_or(|(p, _)| *p + 1 < col)
                    && c.free(2 * col - 1)
                {
                    c.put(2 * col - 1, '/');
                    next.push((col - 1, t));
                    continue;
                }
                c.put(2 * col, '|');
                next.push((col, t));
            }
            rows.after.push(c.done());
            lanes = next;
        }
        debug_assert!(settled(&lanes), "lanes left unsettled");
        self.cols = fin;
        rows.padding = straight(self.cols.len());
        rows
    }
}

/// An entry's text lines with the graph to their left: the commit row on the
/// first line, then the rows after it, then padding; rows left over when the
/// text runs out are printed alone.
pub fn render(rows: &Rows, lines: &[&str]) -> String {
    let mut out = String::new();
    for b in &rows.before {
        out.push_str(b);
        out.push('\n');
    }
    let width = std::iter::once(&rows.commit)
        .chain(&rows.after)
        .chain(std::iter::once(&rows.padding))
        .map(|r| r.chars().count())
        .max()
        .unwrap_or(0);
    let mut after = rows.after.iter();
    for (k, line) in lines.iter().enumerate() {
        let g = if k == 0 {
            &rows.commit
        } else {
            after.next().unwrap_or(&rows.padding)
        };
        if line.is_empty() {
            out.push_str(g);
        } else {
            out.push_str(&format!("{g:<width$} {line}"));
        }
        out.push('\n');
    }
    for g in after {
        out.push_str(g);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One-line entries `(id, parents)`, newest first, drawn as
    /// `--oneline --graph` would draw them with the id as the text.
    fn draw(entries: &[(&str, &[&str])]) -> String {
        let mut g = Graph::new();
        let mut out = String::new();
        for (id, parents) in entries {
            let parents: Vec<&str> = parents.to_vec();
            out.push_str(&render(&g.next(id, &parents), &[id]));
        }
        out
    }

    #[test]
    fn linear() {
        let got = draw(&[("C", &["B"]), ("B", &["A"]), ("A", &[])]);
        assert_eq!(got, "* C\n* B\n* A\n");
    }

    #[test]
    fn branch_and_merge() {
        let got = draw(&[("M", &["B", "C"]), ("C", &["A"]), ("B", &["A"]), ("A", &[])]);
        assert_eq!(got, "*   M\n|\\\n| * C\n* | B\n|/\n* A\n");
    }

    #[test]
    fn two_branches_merged_in_sequence() {
        // main: A - B - M1 - M2; side x off A into M1, side y off B into M2.
        let got = draw(&[
            ("M2", &["M1", "Y"]),
            ("Y", &["B"]),
            ("M1", &["B", "X"]),
            ("X", &["A"]),
            ("B", &["A"]),
            ("A", &[]),
        ]);
        let want = "\
*   M2
|\\
| * Y
* |  M1
|\\ \\
|_|/
| * X
* | B
|/
* A
";
        assert_eq!(got, want, "\n{got}");
    }

    #[test]
    fn octopus() {
        let got = draw(&[
            ("O", &["A", "B", "C", "D"]),
            ("D", &["R"]),
            ("C", &["R"]),
            ("B", &["R"]),
            ("A", &["R"]),
            ("R", &[]),
        ]);
        let want = "\
*---.   O
|\\ \\ \\
| | | * D
| | * | C
| | |/
| * | B
| |/
* | A
|/
* R
";
        assert_eq!(got, want, "\n{got}");
    }

    #[test]
    fn octopus_shifts_lanes_to_its_right() {
        let got = draw(&[("T", &["O", "Z"]), ("O", &["A", "B", "C"]), ("Z", &["A"])]);
        let want = "\
*   T
|\\
|  \\
*-. |   O
|\\ \\ \\
| | | * Z
|_|_|/
";
        assert_eq!(got, want, "\n{got}");
    }

    #[test]
    fn lane_ends_and_lanes_right_of_it_move_left() {
        // Two roots: the side lane ends and the lane right of it moves left.
        let got = draw(&[
            ("M", &["A", "S", "B"]),
            ("S", &[]),
            ("A", &["B"]),
            ("B", &[]),
        ]);
        let want = "\
*-.   M
|\\ \\
| * | S
|  /
* | A
|/
* B
";
        assert_eq!(got, want, "\n{got}");
    }

    #[test]
    fn parent_in_a_lane_to_the_left_is_joined_with_underscores() {
        let mut g = Graph::new();
        g.next(&"t", &["a", "b", "c"]);
        // c's lane (2) has parent a (lane 0): a join over lane 1.
        let rows = g.next(&"c", &["a"]);
        assert_eq!(rows.after, vec!["|_|/".to_string()]);
    }

    #[test]
    fn random_histories_settle_every_lane() {
        // Random DAGs of up to 4 parents, drawn newest first by generation:
        // every entry gets one `*`, and lanes never repeat a changeset.
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut rand = |n: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n as u64) as usize
        };
        for _ in 0..300 {
            let n = 2 + rand(40);
            // Changeset i's parents are older (smaller) changesets.
            let parents: Vec<Vec<usize>> = (0..n)
                .map(|i| {
                    if i == 0 {
                        return vec![];
                    }
                    let k = 1 + usize::from(rand(4) == 0) * (1 + rand(3));
                    (0..k).map(|_| rand(i)).collect()
                })
                .collect();
            let mut g = Graph::new();
            for i in (0..n).rev() {
                let rows = g.next(&i, &parents[i]);
                assert_eq!(rows.commit.matches('*').count(), 1, "{rows:?}");
                for (k, c) in g.cols.iter().enumerate() {
                    assert!(!g.cols[..k].contains(c), "{:?}", g.cols);
                }
                assert_eq!(rows.padding, straight(g.cols.len()));
            }
        }
    }

    #[test]
    fn multi_line_entries_carry_the_rows_after_the_commit() {
        let mut g = Graph::new();
        let rows = g.next(&"M", &["B", "C"]);
        let got = render(&rows, &["changeset M", "Author: a", "", "    msg"]);
        assert_eq!(got, "*   changeset M\n|\\  Author: a\n| |\n| |     msg\n");
    }
}
