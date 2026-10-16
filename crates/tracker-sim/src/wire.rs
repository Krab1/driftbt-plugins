//! BitTorrent peer wire protocol (BEP 3): the handshake and the length-prefixed messages. Only what the loopback
//! swarm uses; no extension protocol yet.

use tokio::io::{AsyncRead, AsyncReadExt};

/// Block size used for requests (16 KiB, what every client sends).
pub const BLOCK: u32 = 16384;
/// Largest message accepted: a bitfield for a very large torrent, or a block with some headroom.
const MAX_MSG: usize = 4 << 20;
const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request { piece: u32, begin: u32, len: u32 },
    Piece { piece: u32, begin: u32, data: Vec<u8> },
    Cancel { piece: u32, begin: u32, len: u32 },
}

impl Msg {
    pub fn encode(&self) -> Vec<u8> {
        fn frame(id: u8, body: &[&[u8]]) -> Vec<u8> {
            let len = 1 + body.iter().map(|b| b.len()).sum::<usize>();
            let mut v = Vec::with_capacity(4 + len);
            v.extend_from_slice(&(len as u32).to_be_bytes());
            v.push(id);
            body.iter().for_each(|b| v.extend_from_slice(b));
            v
        }
        let n = |x: u32| x.to_be_bytes();
        match self {
            Self::KeepAlive => vec![0; 4],
            Self::Choke => frame(0, &[]),
            Self::Unchoke => frame(1, &[]),
            Self::Interested => frame(2, &[]),
            Self::NotInterested => frame(3, &[]),
            Self::Have(p) => frame(4, &[&n(*p)]),
            Self::Bitfield(b) => frame(5, &[b]),
            Self::Request { piece, begin, len } => frame(6, &[&n(*piece), &n(*begin), &n(*len)]),
            Self::Piece { piece, begin, data } => frame(7, &[&n(*piece), &n(*begin), data]),
            Self::Cancel { piece, begin, len } => frame(8, &[&n(*piece), &n(*begin), &n(*len)]),
        }
    }

    /// `payload` is everything after the 4-byte length; empty is a keep-alive.
    pub fn decode(payload: &[u8]) -> Result<Self, String> {
        let Some((&id, body)) = payload.split_first() else { return Ok(Self::KeepAlive) };
        let word = |i: usize| body.get(i * 4..i * 4 + 4).map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]])).ok_or_else(|| format!("message {id} is too short"));
        let exact = |n: usize| if body.len() == n { Ok(()) } else { Err(format!("message {id} has the wrong length")) };
        Ok(match id {
            0 => exact(0).map(|()| Self::Choke)?,
            1 => exact(0).map(|()| Self::Unchoke)?,
            2 => exact(0).map(|()| Self::Interested)?,
            3 => exact(0).map(|()| Self::NotInterested)?,
            4 => {
                exact(4)?;
                Self::Have(word(0)?)
            }
            5 => Self::Bitfield(body.to_vec()),
            6 | 8 => {
                exact(12)?;
                let (piece, begin, len) = (word(0)?, word(1)?, word(2)?);
                if id == 6 { Self::Request { piece, begin, len } } else { Self::Cancel { piece, begin, len } }
            }
            7 => {
                if body.len() < 8 {
                    return Err("piece message is too short".into());
                }
                Self::Piece { piece: word(0)?, begin: word(1)?, data: body[8..].to_vec() }
            }
            other => return Err(format!("unknown message id {other}")),
        })
    }
}

pub fn handshake(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> [u8; 68] {
    let mut b = [0u8; 68];
    b[0] = 19;
    b[1..20].copy_from_slice(PROTOCOL);
    b[28..48].copy_from_slice(info_hash);
    b[48..68].copy_from_slice(peer_id);
    b
}

/// The info hash and peer id of a received handshake.
pub fn parse_handshake(b: &[u8; 68]) -> Result<([u8; 20], [u8; 20]), String> {
    if b[0] != 19 || &b[1..20] != PROTOCOL {
        return Err("not a BitTorrent handshake".into());
    }
    Ok((b[28..48].try_into().expect("20 bytes"), b[48..68].try_into().expect("20 bytes")))
}

pub async fn read_msg<R: AsyncRead + Unpin>(r: &mut R) -> Result<Msg, String> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await.map_err(|e| e.to_string())?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_MSG {
        return Err(format!("message of {len} bytes is too large"));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await.map_err(|e| e.to_string())?;
    Msg::decode(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_message_roundtrips() {
        let msgs = [
            Msg::KeepAlive,
            Msg::Choke,
            Msg::Unchoke,
            Msg::Interested,
            Msg::NotInterested,
            Msg::Have(7),
            Msg::Bitfield(vec![0xff, 0x80]),
            Msg::Request { piece: 1, begin: 16384, len: 16384 },
            Msg::Piece { piece: 2, begin: 0, data: vec![1, 2, 3] },
            Msg::Cancel { piece: 3, begin: 4, len: 5 },
        ];
        for m in msgs {
            let e = m.encode();
            assert_eq!(u32::from_be_bytes(e[..4].try_into().unwrap()) as usize, e.len() - 4);
            assert_eq!(Msg::decode(&e[4..]).unwrap(), m);
        }
    }

    #[test]
    fn bad_messages_are_errors() {
        assert!(Msg::decode(&[4, 0, 0]).is_err());
        assert!(Msg::decode(&[0, 1]).is_err());
        assert!(Msg::decode(&[99]).is_err());
        assert!(Msg::decode(&[7, 0, 0, 0, 1]).is_err());
    }

    #[test]
    fn handshake_roundtrips() {
        let h = handshake(&[1; 20], &[2; 20]);
        assert_eq!(parse_handshake(&h).unwrap(), ([1; 20], [2; 20]));
        let mut bad = h;
        bad[1] = b'X';
        assert!(parse_handshake(&bad).is_err());
    }

    #[tokio::test]
    async fn oversized_frames_are_refused() {
        let mut data: &[u8] = &[0x7f, 0xff, 0xff, 0xff];
        assert!(read_msg(&mut data).await.is_err());
    }
}
