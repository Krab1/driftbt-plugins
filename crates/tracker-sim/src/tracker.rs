//! HTTP tracker client: builds announce/scrape URLs and reads the bencoded answer. Phase 1 speaks HTTP(S) only.
//! Query parameters follow libtorrent's names (`compact=1`, not `compact_peers`). The URL is built by hand because
//! `info_hash` and `peer_id` are raw bytes that must be percent-encoded exactly once.

use crate::settings::HashEncoding;

pub struct Params<'a> {
    pub info_hash: &'a [u8; 20],
    pub peer_id: &'a str,
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub num_want: u32,
    pub event: Option<&'a str>,
    pub key: &'a str,
    /// Parameter order and set of a Transmission client instead of a libtorrent one.
    pub transmission_style: bool,
}

pub fn hash_bytes(hex: &str) -> Option<[u8; 20]> {
    if hex.len() != 40 || !hex.is_ascii() {
        return None;
    }
    let mut out = [0u8; 20];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

pub fn pct(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            s.push(b as char);
        } else {
            s.push_str(&format!("%{b:02X}"));
        }
    }
    s
}

fn encode_hash(h: &[u8; 20], enc: HashEncoding) -> String {
    match enc {
        HashEncoding::Raw => pct(h),
        HashEncoding::Hex => h.iter().map(|b| format!("{b:02x}")).collect(),
    }
}

fn join(base: &str, query: &str) -> String {
    format!("{base}{}{query}", if base.contains('?') { '&' } else { '?' })
}

/// `extra` is already-encoded `name=value` pairs (tracker key, passkey) appended unchanged.
pub fn announce_url(base: &str, p: &Params, enc: HashEncoding, extra: &str) -> String {
    let head = format!(
        "info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}",
        encode_hash(p.info_hash, enc),
        pct(p.peer_id.as_bytes()),
        p.port,
        p.uploaded,
        p.downloaded,
        p.left
    );
    let event = p.event.map(|e| format!("&event={e}")).unwrap_or_default();
    let mut q = if p.transmission_style {
        format!("{head}&numwant={}&key={}&compact=1&supportcrypto=1{event}", p.num_want, p.key)
    } else {
        format!("{head}&key={}{event}&numwant={}&compact=1&no_peer_id=1&supportcrypto=1", p.key, p.num_want)
    };
    if !extra.is_empty() {
        q.push('&');
        q.push_str(extra);
    }
    join(base, &q)
}

/// `…/announce` → `…/scrape` (BEP 48); `None` when the path doesn't end in an announce segment.
pub fn scrape_url(base: &str, info_hash: &[u8; 20], enc: HashEncoding, extra: &str) -> Option<String> {
    let (path, query) = base.split_once('?').unwrap_or((base, ""));
    let slash = path.rfind('/')?;
    let tail = path[slash + 1..].strip_prefix("announce")?;
    let mut q = String::from(query);
    for part in [format!("info_hash={}", encode_hash(info_hash, enc)), extra.to_string()] {
        if !part.is_empty() {
            if !q.is_empty() {
                q.push('&');
            }
            q.push_str(&part);
        }
    }
    Some(format!("{}scrape{tail}?{q}", &path[..=slash]))
}

#[derive(Debug, PartialEq)]
pub struct Answer {
    pub interval: Option<u64>,
}

/// Reads just the top-level `failure reason` and `interval` of a bencoded answer.
pub fn parse_answer(body: &[u8]) -> Result<Answer, String> {
    let mut p = Bencode { b: body, i: 0 };
    p.expect(b'd')?;
    let (mut interval, mut failure) = (None, None);
    while p.peek()? != b'e' {
        match p.bytes()? {
            b"failure reason" => failure = Some(String::from_utf8_lossy(p.bytes()?).into_owned()),
            b"interval" => interval = Some(p.int()?.max(0) as u64),
            _ => p.skip(0)?,
        }
    }
    match failure {
        Some(f) => Err(format!("tracker refused: {}", f.chars().take(200).collect::<String>())),
        None => Ok(Answer { interval }),
    }
}

struct Bencode<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Bencode<'a> {
    fn peek(&self) -> Result<u8, String> {
        self.b.get(self.i).copied().ok_or_else(|| "answer is cut short".to_string())
    }

    fn expect(&mut self, c: u8) -> Result<(), String> {
        if self.peek()? != c {
            return Err("answer is not bencoded".into());
        }
        self.i += 1;
        Ok(())
    }

    fn int(&mut self) -> Result<i64, String> {
        self.expect(b'i')?;
        let end = self.b[self.i..].iter().position(|&c| c == b'e').ok_or("bad integer")?;
        let n = std::str::from_utf8(&self.b[self.i..self.i + end]).ok().and_then(|s| s.parse().ok()).ok_or("bad integer")?;
        self.i += end + 1;
        Ok(n)
    }

    fn bytes(&mut self) -> Result<&'a [u8], String> {
        let colon = self.b[self.i..].iter().position(|&c| c == b':').ok_or("bad string")?;
        let len: usize = std::str::from_utf8(&self.b[self.i..self.i + colon]).ok().and_then(|s| s.parse().ok()).ok_or("bad string")?;
        let start = self.i + colon + 1;
        let s = self.b.get(start..start.checked_add(len).ok_or("bad string")?).ok_or("answer is cut short")?;
        self.i = start + len;
        Ok(s)
    }

    fn skip(&mut self, depth: u32) -> Result<(), String> {
        if depth > 32 {
            return Err("answer is nested too deeply".into());
        }
        match self.peek()? {
            b'i' => self.int().map(drop),
            b'l' | b'd' => {
                self.i += 1;
                while self.peek()? != b'e' {
                    self.skip(depth + 1)?;
                }
                self.i += 1;
                Ok(())
            }
            _ => self.bytes().map(drop),
        }
    }
}

/// One GET. The error text never contains the URL (it may hold a passkey).
pub async fn get(client: &reqwest::Client, url: &str, user_agent: &str) -> Result<Vec<u8>, String> {
    let r = client.get(url).header("User-Agent", user_agent).send().await.map_err(|e| e.without_url().to_string())?;
    if !r.status().is_success() {
        return Err(format!("status {}", r.status().as_u16()));
    }
    r.bytes().await.map(|b| b.to_vec()).map_err(|e| e.without_url().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn announce_url_encodes_raw_bytes_once() {
        let h = hash_bytes(&format!("ab01{}", "ff".repeat(18))).unwrap();
        let p = Params { info_hash: &h, peer_id: "-qB4650-abcdefghijkl", port: 51413, uploaded: 5, downloaded: 6, left: 7, num_want: 200, event: Some("started"), key: "AB12CD34", transmission_style: false };
        let u = announce_url("http://t.example/announce?passkey=k", &p, HashEncoding::Raw, "apikey=z");
        assert!(u.starts_with("http://t.example/announce?passkey=k&info_hash=%AB%01%FF"), "{u}");
        assert!(u.contains("peer_id=-qB4650-abcdefghijkl&port=51413&uploaded=5&downloaded=6&left=7"));
        assert!(u.contains("&event=started&numwant=200&compact=1") && u.ends_with("&apikey=z"));
        assert!(announce_url("http://t/announce", &p, HashEncoding::Hex, "").contains("info_hash=ab01ffff"));
        let tr = announce_url("http://t/announce", &Params { transmission_style: true, ..p }, HashEncoding::Raw, "");
        assert!(tr.contains("&left=7&numwant=200&key=AB12CD34&compact=1&supportcrypto=1&event=started") && !tr.contains("no_peer_id"), "{tr}");
    }

    #[test]
    fn scrape_url_rewrites_the_announce_segment() {
        let h = [0u8; 20];
        let u = scrape_url("http://t/x/announce.php?passkey=k", &h, HashEncoding::Hex, "").unwrap();
        assert_eq!(u, format!("http://t/x/scrape.php?passkey=k&info_hash={}", "00".repeat(20)));
        assert!(scrape_url("http://t/other", &h, HashEncoding::Raw, "").is_none());
    }

    #[test]
    fn answers() {
        assert_eq!(parse_answer(b"d8:intervali1800e5:peers0:e"), Ok(Answer { interval: Some(1800) }));
        assert_eq!(parse_answer(b"d5:peersl3:abce8:intervali60ee"), Ok(Answer { interval: Some(60) }));
        assert!(parse_answer(b"d14:failure reason7:bad bote").unwrap_err().contains("bad bot"));
        assert!(parse_answer(b"<html>").is_err());
        assert!(parse_answer(b"d8:interval").is_err());
    }
}
