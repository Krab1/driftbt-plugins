//! Line-delimited JSON-RPC 2.0 on stdin/stdout. Only the main thread writes, one flushed line at a time.

use std::collections::VecDeque;
use std::io::Write;

use serde_json::{json, Value};

pub struct Msg {
    /// `None` for a notification, which gets no reply.
    pub id: Option<Value>,
    pub method: String,
    pub params: Value,
}

/// Responses (no `method`) and anything that isn't a JSON object come back as errors for the caller to skip.
pub fn parse_line(line: &str) -> Result<Msg, String> {
    let v: Value = serde_json::from_str(line).map_err(|e| e.to_string())?;
    let method = v.get("method").and_then(Value::as_str).ok_or("no method")?.to_string();
    Ok(Msg { id: v.get("id").filter(|i| !i.is_null()).cloned(), method, params: v.get("params").cloned().unwrap_or(Value::Null) })
}

pub fn send(v: &Value) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{v}");
    let _ = out.flush();
}

pub fn ok(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

pub fn err(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

/// Asks Drift for one of this plugin's secrets and waits for the answer. Requests that arrive meanwhile are kept
/// in `later` for the main loop. Only call it while answering a request (not from a notification).
pub fn ask_secret(lines: &mut impl Iterator<Item = std::io::Result<String>>, later: &mut VecDeque<String>, key: &str) -> String {
    send(&json!({"jsonrpc": "2.0", "id": 900, "method": "get_secret", "params": {"key": key}}));
    while let Some(line) = next_line(lines) {
        match serde_json::from_str::<Value>(&line) {
            Ok(v) if v["id"] == 900 && v.get("method").is_none() => return v["result"]["value"].as_str().unwrap_or("").to_string(),
            _ => later.push_back(line),
        }
    }
    String::new()
}

/// The next line, skipping any that aren't valid UTF-8; `None` at the end of input or a real read error.
pub fn next_line(lines: &mut impl Iterator<Item = std::io::Result<String>>) -> Option<String> {
    loop {
        match lines.next()? {
            Ok(l) => return Some(l),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => continue,
            Err(_) => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_notifications_and_junk() {
        let r = parse_line(r#"{"jsonrpc":"2.0","id":10,"method":"get_selection","params":{}}"#).unwrap();
        assert_eq!((r.method.as_str(), r.id), ("get_selection", Some(json!(10))));
        let n = parse_line(r#"{"jsonrpc":"2.0","method":"torrents_updated","params":{"torrents":[]}}"#).unwrap();
        assert!(n.id.is_none());
        assert!(parse_line(r#"{"jsonrpc":"2.0","id":900,"result":{"value":"k"}}"#).is_err(), "a response is not a call");
        assert!(parse_line("not json").is_err());
        assert!(parse_line("[1,2]").is_err());
    }
}
