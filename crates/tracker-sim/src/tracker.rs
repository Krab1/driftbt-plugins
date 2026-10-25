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
    /// The tracker's own id for us (percent-encoded), echoed back as `trackerid`.
    pub tracker_id: Option<&'a str>,
    /// Parameter order and set of a Transmission client instead of a libtorrent one.
    pub transmission_style: bool,
    /// uTorrent's parameter set and lower-case percent codes.
    pub utorrent_style: bool,
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

/// Like `pct`, with lower-case hex digits (uTorrent writes `%e7`, the others `%E7`).
pub fn pct_lower(bytes: &[u8]) -> String {
    pct(bytes)
        .as_bytes()
        .iter()
        .scan(0u8, |left, &b| {
            // the two characters after a % are the hex digits
            let c = if *left > 0 { *left -= 1; (b as char).to_ascii_lowercase() } else { if b == b'%' { *left = 2; } b as char };
            Some(c)
        })
        .collect()
}

fn encode_hash(h: &[u8; 20], enc: HashEncoding, lower: bool) -> String {
    match enc {
        HashEncoding::Raw if lower => pct_lower(h),
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
        encode_hash(p.info_hash, enc, p.utorrent_style),
        if p.utorrent_style { pct_lower(&crate::identity::peer_id_bytes(p.peer_id)) } else { pct(&crate::identity::peer_id_bytes(p.peer_id)) },
        p.port,
        p.uploaded,
        p.downloaded,
        p.left
    );
    let mut event = p.event.map(|e| format!("&event={e}")).unwrap_or_default();
    if let Some(t) = p.tracker_id {
        event.push_str(&format!("&trackerid={t}"));
    }
    let mut q = if p.utorrent_style {
        format!("{head}&corrupt=0&key={}{event}&numwant={}&compact=1&no_peer_id=1", p.key, p.num_want)
    } else if p.transmission_style {
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
    for part in [format!("info_hash={}", encode_hash(info_hash, enc, false)), extra.to_string()] {
        if !part.is_empty() {
            if !q.is_empty() {
                q.push('&');
            }
            q.push_str(&part);
        }
    }
    Some(format!("{}scrape{tail}?{q}", &path[..=slash]))
}

/// What a tracker told us in an announce reply. Everything but a refusal is optional.
#[derive(Debug, PartialEq, Default)]
pub struct Answer {
    pub interval: Option<u64>,
    pub min_interval: Option<u64>,
    /// Seeders and leechers on the torrent, our own peers included.
    pub complete: Option<u64>,
    pub incomplete: Option<u64>,
    pub warning: Option<String>,
    /// Percent-encoded, ready to send back.
    pub tracker_id: Option<String>,
}

impl Answer {
    /// Combines the answers of several trackers: the first value of each field, the largest minimum interval.
    pub fn merge(&mut self, o: Answer) {
        self.interval = self.interval.or(o.interval);
        self.min_interval = self.min_interval.max(o.min_interval);
        self.complete = self.complete.or(o.complete);
        self.incomplete = self.incomplete.or(o.incomplete);
        self.warning = self.warning.take().or(o.warning);
        self.tracker_id = self.tracker_id.take().or(o.tracker_id);
    }
}

/// The start of the error for a reply with a `failure reason`.
const REFUSED: &str = "tracker refused";

pub fn is_refusal(error: &str) -> bool {
    error.starts_with(REFUSED)
}

/// Reads just the top-level `failure reason` and `interval` of a bencoded answer.
pub fn parse_answer(body: &[u8]) -> Result<Answer, String> {
    let mut p = Bencode { b: body, i: 0 };
    p.expect(b'd')?;
    let (mut a, mut failure) = (Answer::default(), None);
    while p.peek()? != b'e' {
        match p.bytes()? {
            b"failure reason" => failure = Some(String::from_utf8_lossy(p.bytes()?).into_owned()),
            b"interval" => a.interval = p.opt_int()?,
            b"min interval" => a.min_interval = p.opt_int()?,
            b"complete" => a.complete = p.opt_int()?,
            b"incomplete" => a.incomplete = p.opt_int()?,
            b"warning message" => a.warning = Some(String::from_utf8_lossy(p.bytes()?).chars().take(200).collect()),
            b"tracker id" => a.tracker_id = Some(pct(p.bytes()?)),
            _ => p.skip(0)?,
        }
    }
    match failure {
        Some(f) => Err(format!("{REFUSED}: {}", f.chars().take(200).collect::<String>())),
        None => Ok(a),
    }
}

/// Seeders and leechers of one torrent from a scrape reply (`files` → info hash → `complete`, `incomplete`).
pub fn parse_scrape(body: &[u8], info_hash: &[u8; 20]) -> Result<(Option<u64>, Option<u64>), String> {
    let mut p = Bencode { b: body, i: 0 };
    p.expect(b'd')?;
    while p.peek()? != b'e' {
        if p.bytes()? != b"files" {
            p.skip(0)?;
            continue;
        }
        p.expect(b'd')?;
        while p.peek()? != b'e' {
            if p.bytes()? != info_hash {
                p.skip(0)?;
                continue;
            }
            p.expect(b'd')?;
            let (mut complete, mut incomplete) = (None, None);
            while p.peek()? != b'e' {
                match p.bytes()? {
                    b"complete" => complete = p.opt_int()?,
                    b"incomplete" => incomplete = p.opt_int()?,
                    _ => p.skip(0)?,
                }
            }
            return Ok((complete, incomplete));
        }
    }
    Err("the scrape reply doesn't list this torrent".into())
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

    /// An integer, or `None` (the value is skipped) if the tracker sent something else there.
    fn opt_int(&mut self) -> Result<Option<u64>, String> {
        if self.peek()? == b'i' {
            Ok(Some(self.int()?.max(0) as u64))
        } else {
            self.skip(0).map(|()| None)
        }
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
pub async fn get(client: &reqwest::Client, url: &str, user_agent: &str, close: bool) -> Result<Vec<u8>, String> {
    let mut req = client.get(url).header("User-Agent", user_agent);
    if close {
        req = req.header("Connection", "close");
    }
    let r = req.send().await.map_err(|e| e.without_url().to_string())?;
    if !r.status().is_success() {
        return Err(format!("status {}", r.status().as_u16()));
    }
    const MAX_BODY: usize = 1 << 20;
    let mut r = r;
    let mut body = Vec::new();
    while let Some(chunk) = r.chunk().await.map_err(|e| e.without_url().to_string())? {
        if body.len() + chunk.len() > MAX_BODY {
            return Err("answer too large".into());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn announce_url_encodes_raw_bytes_once() {
        let h = hash_bytes(&format!("ab01{}", "ff".repeat(18))).unwrap();
        let p = Params { info_hash: &h, peer_id: "-qB4650-abcdefghijkl", port: 51413, uploaded: 5, downloaded: 6, left: 7, num_want: 200, event: Some("started"), key: "AB12CD34", tracker_id: Some("t%201"), transmission_style: false, utorrent_style: false };
        let u = announce_url("http://t.example/announce?passkey=k", &p, HashEncoding::Raw, "apikey=z");
        assert!(u.starts_with("http://t.example/announce?passkey=k&info_hash=%AB%01%FF"), "{u}");
        assert!(u.contains("peer_id=-qB4650-abcdefghijkl&port=51413&uploaded=5&downloaded=6&left=7"));
        assert!(u.contains("&event=started&trackerid=t%201&numwant=200") && u.ends_with("&apikey=z"));
        assert!(announce_url("http://t/announce", &p, HashEncoding::Hex, "").contains("info_hash=ab01ffff"));
        let tr = announce_url("http://t/announce", &Params { transmission_style: true, ..p }, HashEncoding::Raw, "");
        assert!(tr.contains("&left=7&numwant=200&key=AB12CD34&compact=1&supportcrypto=1&event=started") && !tr.contains("no_peer_id"), "{tr}");
    }

    #[test]
    fn scrape_replies() {
        let h = [7u8; 20];
        let mut body = b"d5:filesd20:".to_vec();
        body.extend_from_slice(&[1u8; 20]);
        body.extend_from_slice(b"d8:completei9ee20:");
        body.extend_from_slice(&h);
        body.extend_from_slice(b"d8:completei4e10:downloadedi8e10:incompletei2eeee");
        assert_eq!(parse_scrape(&body, &h), Ok((Some(4), Some(2))));
        assert!(parse_scrape(&body, &[9u8; 20]).is_err());
        assert!(parse_scrape(b"d5:flagsdee", &h).is_err());
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
        assert_eq!(parse_answer(b"d8:intervali1800e5:peers0:e"), Ok(Answer { interval: Some(1800), ..Default::default() }));
        assert_eq!(parse_answer(b"d5:peersl3:abce8:intervali60ee"), Ok(Answer { interval: Some(60), ..Default::default() }));
        let full = parse_answer(b"d8:completei3e10:incompletei7e8:intervali1800e12:min intervali900e15:warning message4:slow10:tracker id3:a b5:peers0:e").unwrap();
        assert_eq!((full.complete, full.incomplete, full.interval, full.min_interval), (Some(3), Some(7), Some(1800), Some(900)));
        assert_eq!((full.warning.as_deref(), full.tracker_id.as_deref()), (Some("slow"), Some("a%20b")));
        assert_eq!(parse_answer(b"d8:complete3:abc8:intervali60ee").unwrap().complete, None, "a value of the wrong kind is ignored");
        let mut m = Answer { interval: Some(60), min_interval: Some(30), ..Default::default() };
        m.merge(Answer { interval: Some(99), min_interval: Some(45), complete: Some(2), ..Default::default() });
        assert_eq!((m.interval, m.min_interval, m.complete), (Some(60), Some(45), Some(2)));
        assert!(parse_answer(b"d14:failure reason7:bad bote").unwrap_err().contains("bad bot"));
        assert!(parse_answer(b"<html>").is_err());
        assert!(parse_answer(b"d8:interval").is_err());
    }
}

/// What each client profile puts on the wire, written out in full. A change to a profile or to the URL builder that
/// alters a request fails here, so it can't happen by accident; change the expected strings on purpose.
#[cfg(test)]
mod golden {
    use super::*;
    use crate::profiles;

    fn request(profile: &str, event: Option<&str>) -> (String, String) {
        let p = profiles::resolve(profile, "", "");
        let h = hash_bytes(&format!("{}{}", "0123456789abcdef".repeat(2), "f0".repeat(4))).unwrap();
        let params = Params { info_hash: &h, peer_id: &format!("{}abcdefghijkl", p.prefix), port: 51413, uploaded: 1000, downloaded: 0, left: 0, num_want: p.num_want, event, key: "AB12CD34", tracker_id: None, transmission_style: p.transmission_query, utorrent_style: p.utorrent_query };
        (announce_url("http://t.example/announce", &params, HashEncoding::Raw, ""), p.user_agent)
    }

    #[test]
    fn qbittorrent_and_deluge_announce_like_libtorrent() {
        let (u, ua) = request("qbittorrent", Some("started"));
        assert_eq!(ua, "qBittorrent/4.6.5");
        assert_eq!(u, "http://t.example/announce?info_hash=%01%23Eg%89%AB%CD%EF%01%23Eg%89%AB%CD%EF%F0%F0%F0%F0&peer_id=-qB4650-abcdefghijkl&port=51413&uploaded=1000&downloaded=0&left=0&key=AB12CD34&event=started&numwant=200&compact=1&no_peer_id=1&supportcrypto=1");
        let (u, ua) = request("deluge", None);
        assert_eq!(ua, "Deluge/2.1.1 libtorrent/2.0.9.0");
        assert_eq!(u, "http://t.example/announce?info_hash=%01%23Eg%89%AB%CD%EF%01%23Eg%89%AB%CD%EF%F0%F0%F0%F0&peer_id=-DE211s-abcdefghijkl&port=51413&uploaded=1000&downloaded=0&left=0&key=AB12CD34&numwant=200&compact=1&no_peer_id=1&supportcrypto=1");
    }

    #[test]
    fn utorrent_announces_exactly_like_the_capture_of_a_real_one() {
        // GET /announce?info_hash=B%e7%8a0%13m%84%2c%d4%2a%04%0e%df%92u%09%ee%e6%af%ff&peer_id=-UT360S-%1e%b9_%e7%92Kc%e5%eak%06%de&port=38934&uploaded=0&downloaded=0&left=11534966&corrupt=0&key=092B59D3&event=started&numwant=200&compact=1&no_peer_id=1
        // from uTorrent 3.6.0 (User-Agent uTorrent/360(113359134)(47390)), recorded by a tracker test dashboard.
        let h = hash_bytes("42e78a30136d842cd42a040edf927509eee6afff").unwrap();
        let id: String = [0x2du8, 0x55, 0x54, 0x33, 0x36, 0x30, 0x53, 0x2d, 0x1e, 0xb9, 0x5f, 0xe7, 0x92, 0x4b, 0x63, 0xe5, 0xea, 0x6b, 0x06, 0xde].iter().map(|&b| b as char).collect();
        let p = profiles::resolve("utorrent", "", "");
        assert_eq!(p.user_agent, "uTorrent/360(113359134)(47390)");
        let params = Params { info_hash: &h, peer_id: &id, port: 38934, uploaded: 0, downloaded: 0, left: 11_534_966, num_want: p.num_want, event: Some("started"), key: "092B59D3", tracker_id: None, transmission_style: false, utorrent_style: p.utorrent_query };
        assert_eq!(
            announce_url("http://127.0.0.1:8000/announce", &params, HashEncoding::Raw, ""),
            "http://127.0.0.1:8000/announce?info_hash=B%e7%8a0%13m%84%2c%d4%2a%04%0e%df%92u%09%ee%e6%af%ff&peer_id=-UT360S-%1e%b9_%e7%92Kc%e5%eak%06%de&port=38934&uploaded=0&downloaded=0&left=11534966&corrupt=0&key=092B59D3&event=started&numwant=200&compact=1&no_peer_id=1"
        );
        let periodic = announce_url("http://t/announce", &Params { event: None, ..params }, HashEncoding::Raw, "");
        assert!(periodic.ends_with("&key=092B59D3&numwant=200&compact=1&no_peer_id=1"), "{periodic}");
    }

    #[test]
    fn transmission_orders_its_parameters_its_own_way() {
        let (u, ua) = request("transmission", Some("stopped"));
        assert_eq!(ua, "Transmission/4.0.4");
        assert_eq!(u, "http://t.example/announce?info_hash=%01%23Eg%89%AB%CD%EF%01%23Eg%89%AB%CD%EF%F0%F0%F0%F0&peer_id=-TR4040-abcdefghijkl&port=51413&uploaded=1000&downloaded=0&left=0&numwant=80&key=AB12CD34&compact=1&supportcrypto=1&event=stopped");
    }
}
