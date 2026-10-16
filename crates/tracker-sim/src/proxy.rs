//! Network identity (Phase 3): which proxy, if any, each simulated peer's tracker requests go through, and the HTTP
//! client built for it. Many peers behind one address is the strongest bot fingerprint, so with `per_peer` mode a
//! peer without a proxy of its own does not run at all rather than leak the real address.

use std::net::IpAddr;
use std::time::Duration;

use crate::settings::{ProxyDef, ProxyMode};

/// Which proxy the next peer uses: `Ok(None)` for a direct connection, `Ok(Some(i))` for `defs[i]`, or `Err`
/// when the mode needs a proxy and none is free. `current` is the proxy id saved for this identity: it is kept while
/// it is still in the list and no other peer of this job took it, so an identity does not change address between
/// runs and two identities never share one. `taken` are the proxies already given to earlier peers of the job.
pub fn assign(defs: &[ProxyDef], mode: ProxyMode, current: Option<&str>, taken: &[usize]) -> Result<Option<usize>, String> {
    match mode {
        ProxyMode::None => Ok(None),
        ProxyMode::Shared => if defs.is_empty() { Err("proxy_mode is shared but the proxy list is empty".into()) } else { Ok(Some(0)) },
        ProxyMode::PerPeer => current
            .and_then(|c| defs.iter().position(|d| d.id == c))
            .filter(|i| !taken.contains(i))
            .or_else(|| (0..defs.len()).find(|i| !taken.contains(i)))
            .map(Some)
            .ok_or_else(|| format!("proxy_mode is per_peer but no proxy is left for peer {} ({} in the list)", taken.len() + 1, defs.len())),
    }
}

/// An HTTP client for one peer: through its proxy, or from a chosen local address, or plain.
pub fn client(proxy: Option<&ProxyDef>, local_address: Option<IpAddr>) -> Result<reqwest::Client, String> {
    let mut b = reqwest::Client::builder().timeout(Duration::from_secs(15));
    if let Some(p) = proxy {
        let mut px = reqwest::Proxy::all(&p.url).map_err(|e| format!("proxy {}: {}", p.id, e.without_url()))?;
        if !p.username.is_empty() {
            px = px.basic_auth(&p.username, &p.password);
        }
        b = b.proxy(px);
    } else if let Some(ip) = local_address {
        b = b.local_address(ip);
    }
    b.build().map_err(|e| e.without_url().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defs(n: usize) -> Vec<ProxyDef> {
        (0..n).map(|i| ProxyDef { id: format!("p{i}"), url: format!("socks5h://127.0.0.1:{}", 1080 + i), username: String::new(), password: String::new() }).collect()
    }

    #[test]
    fn modes() {
        let d = defs(2);
        assert_eq!(assign(&d, ProxyMode::None, None, &[]), Ok(None));
        assert_eq!(assign(&d, ProxyMode::Shared, None, &[0]), Ok(Some(0)));
        assert!(assign(&[], ProxyMode::Shared, None, &[]).is_err());
        assert_eq!(assign(&d, ProxyMode::PerPeer, None, &[0]), Ok(Some(1)));
        assert!(assign(&d, ProxyMode::PerPeer, None, &[0, 1]).is_err(), "no free proxy: the peer must not run direct");
    }

    #[test]
    fn saved_assignment_sticks_when_the_list_is_reordered() {
        let mut d = defs(3);
        d.reverse();
        assert_eq!(assign(&d, ProxyMode::PerPeer, Some("p0"), &[]), Ok(Some(2)));
        assert_eq!(assign(&d, ProxyMode::PerPeer, Some("gone"), &[]), Ok(Some(0)), "a removed proxy falls back to a free one");
        assert_eq!(assign(&d, ProxyMode::PerPeer, Some("p0"), &[2]), Ok(Some(0)), "a proxy another peer holds is not shared");
    }

    #[test]
    fn clients_build() {
        let d = defs(1);
        assert!(client(Some(&d[0]), None).is_ok());
        assert!(client(Some(&ProxyDef { url: "not a url".into(), ..d[0].clone() }), None).is_err());
        assert!(client(None, Some("127.0.0.1".parse().unwrap())).is_ok());
    }
}
