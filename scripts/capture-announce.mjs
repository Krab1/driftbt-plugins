// Records what a BitTorrent client sends to a tracker, byte for byte, so a client profile can be copied from the real thing.
//
//   node scripts/capture-announce.mjs [port]      (default 8899)
//
// It prints a magnet link whose only tracker is this program (127.0.0.1 only, nothing leaves the computer). Add that
// magnet to the client; it announces here every few seconds at first. Each request is shown and appended to
// capture-<port>.log with its headers in the order and spelling the client used. Stop with Ctrl+C.
import { createServer } from "node:net";
import { appendFileSync } from "node:fs";
import { randomBytes } from "node:crypto";

const port = Number(process.argv[2] ?? 8899);
if (!Number.isInteger(port) || port < 1024 || port > 65535) throw new Error("port must be 1024-65535");
const log = `capture-${port}.log`;
const hash = randomBytes(20).toString("hex");

// A valid answer, so the client carries on as it would with a real tracker: no peers, ask again in 30 minutes.
const body = (path) => (path.startsWith("/scrape") ? "d5:filesdee" : "d8:completei0e10:incompletei0e8:intervali1800e12:min intervali900e5:peers0:e");

let n = 0;
createServer((sock) => {
  let data = Buffer.alloc(0);
  sock.on("error", () => {});
  sock.on("data", (chunk) => {
    data = Buffer.concat([data, chunk]);
    if (data.length > 16384) return sock.destroy();
    const end = data.indexOf("\r\n\r\n");
    if (end < 0) return;
    const raw = data.subarray(0, end).toString("latin1");
    const path = raw.split(" ")[1] ?? "/";
    const answer = body(path);
    sock.end(`HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: ${answer.length}\r\nConnection: close\r\n\r\n${answer}`);
    const entry = `--- request ${++n} from ${sock.remoteAddress}:${sock.remotePort} at ${new Date().toISOString()}\n${raw}\n\n`;
    appendFileSync(log, entry);
    console.log(entry);
  });
}).listen(port, "127.0.0.1", () => {
  console.log(`Listening on http://127.0.0.1:${port}/announce (this computer only). Logging to ${log}\n`);
  console.log(`Add this magnet link to the client:\n\nmagnet:?xt=urn:btih:${hash}&dn=capture-test&tr=${encodeURIComponent(`http://127.0.0.1:${port}/announce`)}\n`);
});
