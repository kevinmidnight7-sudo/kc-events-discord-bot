/*
index.js
Discord.js v14 + Firebase Admin (Realtime Database & Firestore)

Commands:
- /link (DMs auth link)
- /badges (public profile embed)
- /whoami (debug)
- /dumpme (debug)
*/

// Make dotenv optional (used locally, ignored on Render if not installed)
try {
  require('dotenv').config();
} catch (_) {}


// Healthcheck server for Render const express = require(‘express’);
const app = express(); app.get(‘/’, (_req, res) =>
res.status(200).send(‘OK’)); app.get(‘/health’, (_req, res) => { const
status = { up: true, uptime: process.uptime(), memory:
process.memoryUsage().rss, ts: Date.now(), };
res.status(200).json(status); }); const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(Healthcheck on :${PORT}));

// — PATCH (Step 5): Add global error hooks —
process.on(“unhandledRejection”, (err) => {
console.error(“[unhandledRejection]”, err); });
process.on(“uncaughtException”, (err) => {
console.error(“[uncaughtException]”, err); }); // — END PATCH —

console.log(‘ENV sanity’, { hasToken: !!process.env.DISCORD_BOT_TOKEN,
hasClient: !!process.env.DISCORD_CLIENT_ID, hasGuild:
!!process.env.DISCORD_GUILD_ID, // optional hasDbUrl:
!!process.env.FB_DATABASE_URL, hasSAJson:
!!process.env.FB_SERVICE_ACCOUNT_JSON, hasSAPath:
!!process.env.FB_SERVICE_ACCOUNT_PATH, });

const { Client, GatewayIntentBits, Partials, SlashCommandBuilder,
Routes, REST, ChannelType, PermissionFlagsBits, GuildMember,
ActionRowBuilder, ButtonBuilder, ButtonStyle, ModalBuilder,
TextInputBuilder, TextInputStyle, EmbedBuilder, StringSelectMenuBuilder
} = require(‘discord.js’);

const admin = require(‘firebase-admin’); const fs = require(‘fs’); const
path = require(‘path’);

// Node HTTPS module used to fetch images for role icons const https =
require(‘https’);

const EMOJI = { offence: ‘<:red:1407696672229818579>’, defence:
‘<:blue:1407696743474139260>’, overall: ‘<:gold:1407696766719234119>’,
verified: ‘<:Verified:1407697810866045008>’, diamond:
‘<:diamondi:1407696929059635212>’, emerald:
‘<:emeraldi:1407696908033593365>’, };

const LB = { CATS: [ { key: ‘overall’, label: ‘Overall Winner’ }, { key:
‘offence’, label: ‘Best Offence’ }, { key: ‘defence’, label: ‘Best
Defence’ }, ], PAGE_SIZE: 10, };

const CLIPS = { PAGE_SIZE: 5, REACTS: [‘🔥’,‘😂’,‘😮’,‘👍’,‘💯’],
MAX_LIST: 50 };

const POST_EMOJIS = [‘🔥’,‘❤️’,‘😂’,‘👍’,‘👎’]; // same list you show on
the site

const DEFAULT_EMBED_COLOR = 0xF1C40F;

// Battledome UI design system. Provides status, icons and colours for
// consistent embeds across all Battledome commands and features. //
These emojis and colours are used throughout the status, recent activity
// and leaderboard embeds so the bot always has a cohesive look and
feel. const BD_UI = { STATUS: { online: ‘🟢’, idle: ‘🟡’, offline: ‘🔴’
}, ICONS: { players: ‘👥’, active: ‘🔥’, time: ‘🕒’, // Region flag
icons; fallback to empty string if missing. // West region removed per
2026 guidelines region: { East: ‘🇺🇸’, EU: ‘🇪🇺’ } }, COLORS: { online:
0x2ECC71, idle: 0xF1C40F, offline: 0xE74C3C } };

// —————————————————————————– // Slither server leaderboard constants
and cache // // NTL (https://ntl-slither.com/ss/rs.php) hosts a realtime
status page for // Slither.io private servers. We add a new /server
command which fetches // this page, parses all server blocks, caches the
result for a short period // and returns a leaderboard embed for a
specified server id. See // slither_server_command_instructions.txt for
full details. // // SLITHER defines the endpoint and caching parameters.
slitherCache stores // the last fetch timestamp, parsed server map, raw
HTML length and error. const SLITHER = { URL:
‘https://ntl-slither.com/ss/rs.php’, TIMEOUT_MS: 8000, CACHE_MS: 15000,
};

const slitherCache = { fetchedAt: 0, servers: new Map(), rawLen: 0,
lastError: null, };

// Fetch plain text from a URL with a timeout and custom user-agent.
Uses // AbortController to cancel the request if it hangs. Throws on
non‑2xx // responses. Returns the response body as a string. async
function fetchText(url, timeoutMs) { const controller = new
AbortController(); const t = setTimeout(() => controller.abort(),
timeoutMs).unref?.(); try { const res = await fetch(url, { signal:
controller.signal, headers: { ‘user-agent’: ‘KC-Discord-Bot/1.0’ }, });
if (!res.ok) throw new Error(HTTP ${res.status} for ${url}); return
await res.text(); } finally { clearTimeout(t); } }

// Parse the NTL real‑time server page into a Map of server objects.
Each // server block begins with a numeric id followed by ip:port and
region. // Leaderboard lines are parsed into rank, name and score.
Totals and // update info are extracted. See the instructions for format
details. function parseNtlServers(html) { // Replace with newlines and
strip other tags. Normalize whitespace. const text = html
.replace(/<br/?>/gi, ‘’) .replace(/<[^>]+>/g, ’‘) .replace(/ /g,’ ‘)
.replace(/0a0/g,’ ‘) .replace(/g,’‘); const lines = text .split(’‘)
.map((l) => l.trim()) .filter(Boolean); const servers = new Map(); let
cur = null; const headerRe =
/^()+([0-9.]+:)+-+(.+)$/;  const lbRe = /^(\d{1,2})#\s*(.+)$/; for
(const line of lines) { const h = line.match(headerRe); if (h) { const
id = Number(h[1]); cur = { id, ipPort: h[2], region: h[3].trim(),
serverTime: null, totalScore: null, totalPlayers: null, updated: null,
leaderboard: [], }; servers.set(id, cur); continue; } if (!cur)
continue; // Server time line if (/^server time:/i.test(line)) {
cur.serverTime = line.replace(/^server time:/i,’’).trim(); continue; }
// Leaderboard lines: e.g. “1# name 12505” const lbm = line.match(lbRe);
if (lbm) { const rank = Number(lbm[1]); const rest = lbm[2].trim();
const m2 = rest.match(/^(.*?)()$/); if (m2) { const name = (m2[1] ||
’‘).trim(); const score = Number(m2[2]); cur.leaderboard.push({ rank,
name: name ||’(no name)‘, score }); } continue; } // Total score line if
(/^total score:/i.test(line)) { cur.totalScore =
Number(line.replace(/[^0-9]/g,’‘)) || null; continue; } // Total players
line if (/^total players:/i.test(line)) { cur.totalPlayers =
Number(line.replace(/[^0-9]/g,’‘)) || null; continue; } // Updated line
if (/^updated:/i.test(line)) { cur.updated =
line.replace(/^updated:/i,’’).trim(); continue; } } // Trim leaderboard
to top 10 and sort by rank for (const s of servers.values()) {
s.leaderboard = s.leaderboard .filter((e) => e.rank >= 1 && e.rank <=
10) .sort((a, b) => a.rank - b.rank); } return servers; }

// Cache wrapper for the Slither server map. Returns cached data if the
// last fetch occurred within SLITHER.CACHE_MS milliseconds. Otherwise
// fetches fresh HTML and reparses. Returns an object with servers map,
// fromCache boolean and fetchedAt timestamp. async function
getSlitherServersCached() { const now = Date.now(); const age = now -
slitherCache.fetchedAt; if (slitherCache.servers.size && age <
SLITHER.CACHE_MS) { return { servers: slitherCache.servers, fromCache:
true, fetchedAt: slitherCache.fetchedAt, }; } try { const html = await
fetchText(SLITHER.URL, SLITHER.TIMEOUT_MS); const servers =
parseNtlServers(html); slitherCache.fetchedAt = Date.now();
slitherCache.servers = servers; slitherCache.rawLen = html.length;
slitherCache.lastError = null; return { servers, fromCache: false,
fetchedAt: slitherCache.fetchedAt }; } catch (e) {
slitherCache.lastError = e.message; if (slitherCache.servers.size) {
return { servers: slitherCache.servers, fromCache: true, fetchedAt:
slitherCache.fetchedAt, error: e.message, }; } throw e; } }

// Simple in-memory cache for frequently accessed, non-critical data
const globalCache = { userNames: new Map(), // uid -> displayName
userNamesFetchedAt: 0, clipDestinations: new Map(), // guildId ->
channelId // Destination channels for clan battle announcements (guildId
-> channelId) battleDestinations: new Map(), // Destination channels for
Battledome updates (guildId -> channelId) bdDestinations: new Map(), //
Destination channels for Battledome join logs (guildId -> channelId).
When set, // join alerts ping subscribers in this channel instead of
sending DMs. bdJoinLogs: new Map(), // Persistent Battledome status
messages (guildId -> { channelId, messageId }) bdStatusMessages: new
Map(), // Mapping of Discord users to their KC accounts for BD highscore
opt‑in bdToKC: new Map(), // discordId -> { kcUid, kcName, enabled } //
Reverse lookup from Battledome displayName to KC uid for fast highscore
matching kcNameToUid: new Map(), };

// only these will be private const isEphemeralCommand = (name) => new
Set([‘whoami’, ‘dumpme’, ‘help’, ‘vote’, ‘syncavatar’, ‘post’,
‘postmessage’, ‘link’, ‘setclipschannel’, ‘latestfive’, ‘notifybd’,
‘setbattledomechannel’, ‘setjoinlogschannel’, ‘bdtokc’]).has(name);

// Parse #RRGGBB -> int for embed.setColor function hexToInt(hex) { if
(!hex || typeof hex !== ‘string’) return null; const m =
hex.match(/#([0-9a-f]{6})/i); return m ? parseInt(m[1], 16) : null; }

// Extract first hex colour from a CSS gradient string function
firstHexFromGradient(grad) { if (!grad || typeof grad !== ‘string’)
return null; const m = grad.match(/#([0-9a-f]{6})/i); return m ?
#${m[1]} : null; }

// ———- Firebase Admin init ———- function loadServiceAccount() { if
(process.env.FB_SERVICE_ACCOUNT_JSON) { try { return
JSON.parse(process.env.FB_SERVICE_ACCOUNT_JSON); } catch (e) {
console.error(‘Failed to parse FB_SERVICE_ACCOUNT_JSON:’, e);
process.exit(1); } } if (process.env.FB_SERVICE_ACCOUNT_PATH) { const p
= process.env.FB_SERVICE_ACCOUNT_PATH; try { return
JSON.parse(fs.readFileSync(path.resolve(p), ‘utf8’)); } catch (e) {
console.error(‘Failed to read FB_SERVICE_ACCOUNT_PATH:’, p, e);
process.exit(1); } } console.error(‘No service account provided. Set
FB_SERVICE_ACCOUNT_JSON or FB_SERVICE_ACCOUNT_PATH.’); process.exit(1);
}

const serviceAccount = loadServiceAccount();

admin.initializeApp({ credential: admin.credential.cert(serviceAccount),
databaseURL: process.env.FB_DATABASE_URL, // REQUIRED for RTDB
storageBucket: process.env.FB_STORAGE_BUCKET || undefined, });

const rtdb = admin.database();

// ===== Bot lock ===== const LOCK_KEY = ’_runtime/botLock’; const
LOCK_TTL_MS = 90_000; // 90s const OWNER_ID =
process.env.RENDER_INSTANCE_ID || pid:${process.pid};

async function claimBotLock() { const now = Date.now(); // IMPORTANT:
correct transaction signature (no options object) const result = await
rtdb.ref(LOCK_KEY).transaction(cur => { if (!cur) { return { owner:
OWNER_ID, expiresAt: now + LOCK_TTL_MS }; } if (cur.expiresAt &&
cur.expiresAt < now) { // stale -> take over return { owner: OWNER_ID,
expiresAt: now + LOCK_TTL_MS }; } // someone else owns it return; //
abort }, undefined /* onComplete /, false / applyLocally */);

// result has .committed (compat) or .committed-like semantics; if using
admin, // just treat “snapshot.val()?.owner === OWNER_ID” as success:
const snap = await rtdb.ref(LOCK_KEY).get(); const val = snap.val() ||
{}; return val.owner === OWNER_ID; }

async function renewBotLock() { const now = Date.now(); await
rtdb.ref(LOCK_KEY).transaction(cur => { if (!cur) return; // nothing to
renew if (cur.owner !== OWNER_ID) return; // not ours anymore
cur.expiresAt = now + LOCK_TTL_MS; return cur; }, undefined, false); }

// ———- Helpers ———- function encPath(p){ return String(p).replace(///g,
‘|’); } function decPath(s){ return String(s).replace(/|/g, ‘/’); }

async function withTimeout(promise, ms, label=‘op’){ return
Promise.race([ promise, new Promise((_,rej)=>setTimeout(()=>rej(new
Error(Timeout ${ms}ms: ${label})), ms)) ]); }

// — Interaction Ack Helpers — async function safeDefer(interaction,
opts = {}) { try { if (interaction.deferred || interaction.replied)
return; if (interaction.isChatInputCommand()) { return await
interaction.deferReply({ ephemeral: !!opts.ephemeral }); } if
(interaction.isMessageComponent()) { if (opts.intent === “update”) {
return await interaction.deferUpdate(); } return; } if
(interaction.isModalSubmit()) { return await interaction.deferReply({
ephemeral: !!opts.ephemeral }); } } catch (err) {
console.error(“safeDefer error:”, err); } }

async function safeReply(interaction, options) { try { if
(interaction.isMessageComponent()) { if (interaction.deferred &&
!interaction.replied) { return await interaction.update(options); } if
(interaction.replied) { return await interaction.followUp(options); }
return await interaction.reply(options); } if (interaction.deferred) {
if (interaction.replied) { return await interaction.followUp(options); }
return await interaction.editReply(options); } if (interaction.replied)
{ return await interaction.followUp(options); } return await
interaction.reply(options); } catch (err) { console.error(“safeReply
error”, { deferred: interaction.deferred, replied: interaction.replied,
isChatInput: interaction.isChatInputCommand(), isComponent:
interaction.isMessageComponent(), isModal: interaction.isModalSubmit(),
}, err); if (err.code !== 10062) { try { if (!interaction.replied) {
await interaction.followUp(options); } } catch (e) {
console.error(“safeReply followup failed”, e); } } } }

// — Battledome Helpers (NEW) — const BD = { TIMEOUT_MS: 10000,
MIN_FETCH_INTERVAL_MS: 3500, // hard cooldown INFO_FRESH_MS: 6000, //
consider fresh INFO_STALE_MS: 60000, // keep stale until LB_FRESH_MS:
15000, LB_STALE_MS: 5 * 60 * 1000, STATS_FRESH_MS: 5 * 60 * 1000,
STATS_STALE_MS: 60 * 60 * 1000, WEST_STATS_URL:
“http://snakey.monster/bdstats.htm”, };

// Authoritative Server List (HTTP only) // Authoritative Server List
(HTTP only). West region removed per 2026 migration. const BD_SERVERS =
{ East: { name: “East Coast Battledome”, url:
“http://206.221.176.241:444/bdinfo.json”, region: “East” }, EU: { name:
“EU Battledome”, url: “http://51.91.19.175:444/bdinfo.json”, region:
“EU” } };

// Dynamically derive region keys. Used throughout to avoid hard‑coding
regions. const BD_REGIONS = Object.keys(BD_SERVERS);

// Map of Server Name -> Region Key (Fix for Battledome Selection) const
BD_NAME_OVERRIDES = { // West mappings removed – West server retired
“East Coast Battledome”: “East”, “New York Battledome”: “East”, “EU
Battledome”: “EU”, };

// Map region -> state for polling // Per‑region state used by the
poller to compute diffs and activity thresholds. const bdState = {}; for
(const region of BD_REGIONS) { bdState[region] = { lastNames: new Set(),
lastOnline: 0, lastIndome: 0, lastCheck: 0 }; }

// — Battledome Live Status & Recent Activity Storage (NEW) — // Persist
the most recently fetched info for each region so we can build a //
consolidated status embed across all battledomes. Each entry holds the
// parsed info returned from the BD API (see parseBdInfo) and the time
at // which it was fetched. These values are updated on each poll. const
bdLastInfo = {}; const bdLastFetch = {}; for (const region of
BD_REGIONS) { bdLastInfo[region] = null; bdLastFetch[region] = 0; }

// Map of guildId -> messageId for the live status messages posted by //
updateBdStatusMessages(). When broadcasting updates we edit the existing
// message rather than posting a new one. This keeps the channel tidy
and // ensures a single live snapshot. (Deprecated: use
globalCache.bdStatusMessages instead) const bdSummaryMessages = new
Map();

// Mutex to ensure only one Battledome status update runs at a time.
Without // this, multiple concurrent calls (from pollers, manual refresh
or channel // changes) can race and cause duplicate messages. When
non-null, a promise // representing the in-flight update is stored here.
All callers will await // the same promise and exit early. The mutex is
cleared when the update // completes. let bdStatusUpdateInFlight = null;

// Track recent join and leave events per region. Each entry is an
object // { name, time, type } where type is ‘join’ or ‘leave’. A
separate list is // maintained for each region. Events older than
BD_RECENT_WINDOW_MS are // automatically pruned. const bdRecent = {};
for (const region of BD_REGIONS) { bdRecent[region] = []; }

// How long to keep join/leave events in memory (e.g. 15 minutes). This
// determines the window for the /recentlyjoined command. Adjust as
needed. const BD_RECENT_WINDOW_MS = 15 * 60 * 1000;

// Helper to remove stale entries from bdRecent. Called in checkRegion.
function pruneBdRecent() { const cutoff = Date.now() -
BD_RECENT_WINDOW_MS; for (const region of Object.keys(bdRecent)) { const
list = bdRecent[region]; let idx = 0; while (idx < list.length &&
list[idx].time < cutoff) { idx++; } if (idx > 0) { bdRecent[region] =
list.slice(idx); } } }

// Build an array of embeds summarising the current status of all
battledome // servers. Each embed covers one region and includes online
counts, dome // counts, players this hour and a short leaderboard (top
10 players). If // no info has been fetched yet for a region, a warming
message is shown. function buildBdStatusEmbeds() { const embeds = [];
for (const region of BD_REGIONS) { const info = bdLastInfo[region];
const fetchedAt = bdLastFetch[region]; if (!info) { const title =
BD_SERVERS[region]?.name || ${region} Battledome; embeds.push(new
EmbedBuilder() .setTitle(title) .setDescription(‘Cache warming up.
Please wait…’) .setColor(DEFAULT_EMBED_COLOR) .setFooter({ text: ‘Cache
warming up’ })); continue; } const players = Array.isArray(info.players)
? info.players : []; // Build a simple leaderboard of the top 10 players
by score. Show their // rank, name and score. Indicate if they are
currently in the dome with an // emoji. Idle players (inactive > 60s)
are annotated. const lines = players.slice(0, 10).map((p, idx) => {
const inDome = p.indomenow === ‘Yes’ ? ’ 🏟️’ : ’‘; const rank = p.rank
!= null ? #${p.rank} : #${idx + 1}; const score = p.score != null ?
(${p.score}) :’‘; const name = p.name ||’(unknown)‘; const idle =
p.inactive && p.inactive > 60 ?’ (idle)’ : ’‘; return
${rank} **${name}**${score}${inDome}${idle}; }); const ageSec =
fetchedAt ? Math.floor((Date.now() - fetchedAt) / 1000) : 0; const
cacheNote = ’Updated’ + (ageSec < 2 ? ‘just now’ : ${ageSec}s ago);
const embed = new EmbedBuilder()
.setTitle(Battledome — ${BD_SERVERS[region]?.name || region})
.addFields( { name: ‘Online now’, value: String(info.onlinenow ?? ‘—’),
inline: true }, { name: ‘In dome now’, value: String(info.indomenow ??
‘—’), inline: true }, { name: ‘Players this hour’, value:
String(info.thishour ?? ‘—’), inline: true }, { name:
Players (${players.length}), value: lines.length ? lines.join(‘’) : ‘No
players listed.’ } ) .setColor(DEFAULT_EMBED_COLOR) .setFooter({ text:
cacheNote }); embeds.push(embed); } return embeds; }

// Build embeds summarising recent join/leave events. Each embed covers
one // region and lists players who have joined or left within the
recent // window defined by BD_RECENT_WINDOW_MS. If no events are
recorded, a // friendly message is displayed instead. A small status
header is also // included (online/in‑dome counts) to provide context.
function buildBdRecentEmbeds() { const embeds = []; pruneBdRecent(); for
(const region of BD_REGIONS) { const info = bdLastInfo[region]; const
events = bdRecent[region] || []; const joined = events.filter(e =>
e.type === ‘join’); const left = events.filter(e => e.type === ‘leave’);
const linesJoin = joined.map(e => { const ago = Math.floor((Date.now() -
e.time) / 1000); const secs = ago; return
• **${e.name}** \- <t:${Math.floor(e.time/1000)}:R>; }); const
linesLeave = left.map(e => { return
• **${e.name}** \- <t:${Math.floor(e.time/1000)}:R>; }); const embed =
new EmbedBuilder()
.setTitle(Recent Activity — ${BD_SERVERS[region]?.name || region})
.setColor(DEFAULT_EMBED_COLOR); if (info) { embed.addFields( { name:
‘Online now’, value: String(info.onlinenow ?? ‘—’), inline: true }, {
name: ‘In dome now’, value: String(info.indomenow ?? ‘—’), inline: true
}, { name: ‘Players this hour’, value: String(info.thishour ?? ‘—’),
inline: true } ); } if (linesJoin.length) { embed.addFields({ name:
Joined (${linesJoin.length}), value: linesJoin.join(‘’) }); } else {
embed.addFields({ name: ‘Joined’, value: ‘No recent joins.’ }); } if
(linesLeave.length) { embed.addFields({ name:
Left (${linesLeave.length}), value: linesLeave.join(‘’) }); } else {
embed.addFields({ name: ‘Left’, value: ‘No recent leaves.’ }); }
embeds.push(embed); } return embeds; }

// Build a single embed summarising the current status of all Battledome
// servers. Each region becomes a field on the embed. This unified //
representation makes it easier to scan without multiple embeds and //
keeps channels cleaner. The embed includes online counts, in‑dome //
counts, players this hour and a short leaderboard (top 10 players).
function buildBdStatusUnifiedEmbed(options = {}) { const showAdvanced =
options.showAdvanced || false; const embed = new EmbedBuilder();
embed.setTitle(‘🏟️ Battledome Status’); let maxAgeSec = 0; // Track
worst server status across all regions. offline > idle > online let
worstStatus = ‘online’; for (const region of BD_REGIONS) { const info =
bdLastInfo[region]; const fetchedAt = bdLastFetch[region]; const
serverName = BD_SERVERS[region]?.name || region; const lines = []; if
(info) { // Determine status based on player counts const onNow =
info.onlinenow ?? 0; const inDome = info.indomenow ?? 0; let status =
‘offline’; if (onNow > 0) status = (inDome > 0 ? ‘online’ : ‘idle’); //
Track worst status for embed colour if (status === ‘offline’)
worstStatus = ‘offline’; else if (status === ‘idle’ && worstStatus !==
‘offline’) worstStatus = ‘idle’; // Compose lines with icons const pct =
onNow > 0 ? Math.floor((inDome / onNow) * 100) : 0;
lines.push(${BD_UI.STATUS[status]} ${status.charAt(0).toUpperCase() + status.slice(1)});
lines.push(${BD_UI.ICONS.players} Players: ${onNow});
lines.push(${BD_UI.ICONS.active} Active: ${inDome} (${pct}%)); // Append
leaderboard: limit is 10 when advanced view is enabled, otherwise 5.
const limit = showAdvanced ? 10 : 5; const leaderboard =
formatLeaderboard(bdTop[region] || new Map(), limit); // Separate stats
and leaderboard with a blank line for readability lines.push(’‘);
lines.push(leaderboard); // Include advanced details if requested.
Derive the in‑game IP:PORT from // the Battledome server URL rather than
showing the full API path or // falling back to info.ip (which is often
undefined). Use URL().host to // extract only host:port from
BD_SERVERS[region].url (e.g. http://172.99.249.149:444/bdinfo.json ->
172.99.249.149:444). if (showAdvanced) { let serverIp = ’Unknown’; try {
const u = new URL(BD_SERVERS[region]?.url || ’‘); serverIp = u.host ||
’Unknown’; } catch (_) {} lines.push(🖥️ Server: ${serverIp}); }

      if (fetchedAt) {
        const age = Math.floor((Date.now() - fetchedAt) / 1000);
        if (age > maxAgeSec) maxAgeSec = age;
      }
    } else {
      // If cache is warming, show offline status and note
      lines.push(`${BD_UI.STATUS.offline} Offline`);
      lines.push(`${BD_UI.ICONS.players} Players: 0`);
      lines.push(`${BD_UI.ICONS.active} Active: 0 (0%)`);
      lines.push('_Cache warming up. Please wait…_');
    }
    const regionIcon = BD_UI.ICONS.region[region] || '';
    embed.addFields({ name: `${regionIcon} ${serverName}`, value: lines.join('\n'), inline: false });

} // Set description and colour based on worst status and last update
const updatedStr = maxAgeSec > 0 ? (maxAgeSec < 2 ? ‘just now’ :
${maxAgeSec}s ago) : ‘warming up’;
embed.setDescription(Monitoring ${Object.keys(BD_SERVERS).length} server(s) • Updated ${updatedStr});
embed.setColor(BD_UI.COLORS[worstStatus]); return embed; }

// Build a single embed summarising recent join and leave activity
across // all Battledome regions. Each region becomes a field. The embed
lists // basic counts (online, in‑dome and players this hour) alongside
recent // joins and leaves. Limiting to a single embed avoids clutter
and // matches the unified dashboard style. function
buildBdRecentUnifiedEmbed() { pruneBdRecent(); const embed = new
EmbedBuilder() .setTitle(‘🧾 Recent Battledome Activity’)
.setColor(DEFAULT_EMBED_COLOR); for (const region of BD_REGIONS) { const
info = bdLastInfo[region]; const events = bdRecent[region] || []; const
joined = events.filter(e => e.type === ‘join’); const left =
events.filter(e => e.type === ‘leave’); const lines = []; if (info) {
lines.push(${BD_UI.ICONS.players} Online: ${info.onlinenow ?? '—'});
lines.push(${BD_UI.ICONS.active} In Dome: ${info.indomenow ?? '—'});
lines.push(${BD_UI.ICONS.time} Players This Hour: ${info.thishour ?? '—'});
} if (joined.length) { const joinLines = joined.map(e =>
• **${e.name}** — <t:${Math.floor(e.time/1000)}:R>);
lines.push(**Joined (${joined.length})**);
lines.push(joinLines.join(‘’)); } else { lines.push(‘Joined’);
lines.push(‘No recent joins.’); } if (left.length) { const leaveLines =
left.map(e => • **${e.name}** — <t:${Math.floor(e.time/1000)}:R>);
lines.push(**Left (${left.length})**); lines.push(leaveLines.join(‘’));
} else { lines.push(‘Left’); lines.push(‘No recent leaves.’); } const
serverName = BD_SERVERS[region]?.name || region; const regionIcon =
BD_UI.ICONS.region[region] || ’‘; embed.addFields({ name:
${regionIcon} ${serverName}, value: lines.join(’’), inline: false }); }
return embed; }

// Build a single embed comparing the top Battledome scores across //
regions and globally. Each leaderboard becomes a field. The embed //
displays the top 10 players sorted by score for Global, West, // East
and EU. A footer indicates when the data was last updated. function
buildBdScoreCompareEmbed() { // Helper to extract top entries from a Map
of name -> score objects function topEntries(map) { const arr = [];
map.forEach((val, key) => { const score = (val && typeof val ===
‘object’) ? (val.score ?? 0) : (Number(val) || 0); arr.push({ name: key,
score }); }); arr.sort((a, b) => (b.score || 0) - (a.score || 0));
return arr.slice(0, 10); } const eastTop = topEntries(bdTop.East); const
euTop = topEntries(bdTop.EU); // Format lines for a leaderboard const
fmt = list => list.map((p, i) => { const rank = String(i +
1).padStart(2, ’ ‘); const name = clampName(p.name, 25).padEnd(27);
const score = String(p.score).padStart(5); return
\${rank}.\` **${name}** `—
${score}\``;  }).join('\n') || '_No scores recorded._';  const embed = new EmbedBuilder()  .setTitle('🏆 Top Battledome Scores Comparison')  .setColor(DEFAULT_EMBED_COLOR);  embed.addFields(  { name: `${BD_UI.ICONS.region.East
||’’}
East, value: fmt(eastTop),   inline: false },     { name:${BD_UI.ICONS.region.EU || ''} EU`, value: fmt(euTop), inline: false }  );  const lastUpdate = bdTopMeta.lastUpdatedAt || Date.now();  embed.setFooter({ text: `Updated <t:${Math.floor(lastUpdate/1000)}:R>`
}); return embed; }

// —————————————————————————– // Battledome helpers for join logs and
server host extraction // // Extract the in‑game host:port from the
configured BD_SERVERS URL for a // region. Falls back to ‘Unknown’ if
parsing fails. This allows details // views and join logs to show the
actual connection address instead of the // API endpoint. function
bdServerHost(region) { try { return new URL(BD_SERVERS[region]?.url ||
’‘).host || ’Unknown’; } catch { return ‘Unknown’; } }

/** * Format a leaderboard for display in the Battledome status embed. *
Accepts a Map of player name -> score objects or numbers. Sorts by *
descending score, clamps names to 18 characters and pads values so *
everything aligns. Returns a string that includes a heading with * the
limit and a code block. If no players are present, returns * ‘No scores
recorded.’. @param {Map|string[]} source The data source (usually
bdTop[region]) * @param {number} limit Maximum number of players to
display * @returns {string} */ function formatLeaderboard(source, limit
= 5) { const entries = []; if (source instanceof Map) { for (const
[name, entry] of source.entries()) { const score = (entry && typeof
entry === ‘object’) ? (entry.score ?? 0) : (Number(entry) || 0);
entries.push({ name: name || ‘(unknown)’, score }); } } else if
(Array.isArray(source)) { for (const p of source) { if (p && typeof p
=== ‘object’) { entries.push({ name: p.name || ‘(unknown)’, score:
p.score || 0 }); } } } entries.sort((a, b) => (b.score || 0) - (a.score
|| 0)); const top = entries.slice(0, limit); if (top.length === 0) {
return ‘No scores recorded.’; } // Build aligned lines: rank padded to 2
chars, name clamped/padded to 18, score padded to 5 const lines =
top.map((p, idx) => { const rank = String(idx + 1).padStart(2, ’ ‘);
const name = clampStr(p.name, 18,’‘).padEnd(18,’ ‘); const score =
String(p.score).padStart(5,’ ‘); return ${rank}. ${name} ${score}; });
return **Top ${limit}**\n\``${lines.join(’’)}````; }

// Post join and leave events to the configured Battledome join logs
channel. // When a guild has /setjoinlogschannel configured, this
function constructs // green (joins) and red (leaves) embeds summarising
the names and counts // of players who joined or left the specified
region during a poll. Up to // 10 names are shown inline; additional
players are summarised. A timestamp // is included in the footer. If no
names are provided, no embed is sent. async function
postJoinLogsBlock(guildId, region, { joins = [], leaves = [] }) { const
joinLogsId = globalCache.bdJoinLogs?.get(guildId); if (!joinLogsId)
return; const chan = await client.channels.fetch(joinLogsId).catch(() =>
null); if (!chan || !chan.isTextBased?.()) return; const serverName =
BD_SERVERS[region]?.name || region; const regionIcon =
BD_UI.ICONS.region?.[region] || ’‘; const host = bdServerHost(region);
const embeds = []; if (joins.length > 0) { const list = joins.slice(0,
10).map(n => • **${n}**).join(’‘); const more = joins.length > 10 ?
\n… +${joins.length - 10} more :’‘; embeds.push( new EmbedBuilder()
.setTitle(’✅ Battledome Join Log’) .setColor(0x2ECC71) .setDescription(
${regionIcon} **${serverName}**\n + 🖥️ Server: **${host}**\n +
${BD_UI.ICONS.players} Joined: **${joins.length}**\n\n + (list || ‘No
named joins’) + more ) .setFooter({ text:
Updated ${new Date().toLocaleString()} }) ); } if (leaves.length > 0) {
const list2 = leaves.slice(0, 10).map(n => • **${n}**).join(‘’); const
more2 = leaves.length > 10 ? \n… +${leaves.length - 10} more : ’‘;
embeds.push( new EmbedBuilder() .setTitle(’❌ Battledome Leave Log’)
.setColor(0xE74C3C) .setDescription( ${regionIcon} **${serverName}**\n +
🖥️ Server: **${host}**\n +
${BD_UI.ICONS.players} Left: **${leaves.length}**\n\n + (list2 || ‘No
named leaves’) + more2 ) .setFooter({ text:
Updated ${new Date().toLocaleString()} }) ); } if (embeds.length) {
await chan.send({ embeds }); } }

// Build the persistent dashboard action rows. The public Battledome
status // message includes three buttons: Controls, Refresh and Details.
The // Details button toggles advanced information. Pass the guildId and
// showAdvanced state so the button labels can reflect the current //
setting (e.g. View Details vs Hide Details). function
buildBdDashboardActionRows(guildId, showAdvanced = false) { const
controlsBtn = new ButtonBuilder() .setCustomId(bd:controls:${guildId})
.setLabel(‘⚙️ Controls’) .setStyle(ButtonStyle.Secondary); const
refreshBtn = new ButtonBuilder() .setCustomId(bd:refresh:${guildId})
.setLabel(‘🔄 Refresh’) .setStyle(ButtonStyle.Primary); // The details
button should always display the same label. Instead of // switching
between “Hide” and “View” (which can be cognitively // distracting),
keep a single “📊 Details” label. The button still // toggles the
advanced/compact views internally. const detailsBtn = new
ButtonBuilder() .setCustomId(bd:details:${guildId}) .setLabel(‘📊
Details’) .setStyle(ButtonStyle.Secondary); const row = new
ActionRowBuilder().addComponents(controlsBtn, refreshBtn, detailsBtn);
return [row]; }

// Send or update the live Battledome status message in each configured
// guild/channel. For each guild configured via setbattledomechannel, //
check if we already sent a status message. If so, edit it with the //
latest embeds; otherwise send a new message and remember its ID. This //
function should be called after each poll to keep the status fresh and
// when a destination channel is added. async function
updateBdStatusMessages() { // For each guild configured to receive
Battledome updates, build // a unified embed and attach the dashboard
controls. The embed // may include advanced details based on per‑guild
settings. for (const [guildId, channelId] of
globalCache.bdDestinations.entries()) { try { const channel = await
client.channels.fetch(channelId).catch(() => null); if (!channel ||
!channel.isTextBased?.()) continue; // Determine if advanced details are
enabled for this guild let showAdvanced = false; try { const snap =
await rtdb.ref(config/bdShowAdvanced/${guildId}).get(); if
(snap.exists() && snap.val() === true) showAdvanced = true; } catch {}
const embed = buildBdStatusUnifiedEmbed({ showAdvanced }); const
components = buildBdDashboardActionRows(guildId, showAdvanced); const
msgId = bdSummaryMessages.get(guildId); if (msgId) { // Try to edit
existing message. If it fails (e.g. message deleted), fall back to send.
try { const msg = await channel.messages.fetch(msgId); await msg.edit({
content: ’‘, embeds: [embed], components }); continue; } catch {} } //
No existing message or failed to edit; send a new one const msg = await
channel.send({ content:’’, embeds: [embed], components });
bdSummaryMessages.set(guildId, msg.id); } catch (e) {
console.error([BD Status] Failed to update status message for guild ${guildId}:,
e.message); } } }

// — Cache Globals — const bdFetchCache = new Map(); const bdTop = {
global: new Map(), }; // Initialise per‑region top maps based on
BD_REGIONS for (const region of BD_REGIONS) { bdTop[region] = new Map();
} const bdTopMeta = { lastUpdatedAt: 0, seededHardcoded: false }; let
bdTopDirty = false; const lastCacheAnnounce = {}; // regionKey ->
timestamp

async function swrFetch(key, { fetcher, freshMs, staleMs, minIntervalMs
}) { const now = Date.now(); let entry = bdFetchCache.get(key); if
(!entry) { entry = { data: null, fetchedAt: 0, lastAttemptAt: 0,
inFlight: null, lastError: null }; bdFetchCache.set(key, entry); }

const age = now - entry.fetchedAt; const isFresh = entry.data && age <=
freshMs; const isStale = entry.data && age <= staleMs;

const doFetch = () => { if (entry.inFlight) return entry.inFlight;
entry.lastAttemptAt = Date.now(); entry.inFlight = (async () => { try {
console.log([SWR] Fetching real data for ${key}); const res = await
fetcher(); entry.data = res; entry.fetchedAt = Date.now();
entry.lastError = null; return res; } catch (e) { entry.lastError =
e.message; throw e; } finally { entry.inFlight = null; } })(); return
entry.inFlight; };

// A) Fresh: Return cached if (isFresh) { return { data: entry.data,
fromCache: true, stale: false, fetchedAt: entry.fetchedAt }; }

// B) Stale: Return cached, maybe background refresh if (isStale) {
const timeSinceAttempt = now - entry.lastAttemptAt; if (!entry.inFlight
&& timeSinceAttempt >= minIntervalMs) { // Trigger background refresh
(do not await) doFetch().catch(e =>
console.warn([SWR] BG fail ${key}: ${e.message})); } return { data:
entry.data, fromCache: true, stale: true, fetchedAt: entry.fetchedAt };
}

// C) No data or expired: Must fetch if (entry.inFlight) { //
Deduplicate: wait for existing in-flight try { const data = await
entry.inFlight; return { data, fromCache: false, stale: false,
fetchedAt: entry.fetchedAt }; } catch (e) { // If in-flight fails but we
have stale data (even if very old), return that if (entry.data) return {
data: entry.data, fromCache: true, stale: true, fetchedAt:
entry.fetchedAt, error: e.message }; throw e; } }

// Rate limiting for the sync fetch const timeSinceAttempt = now -
entry.lastAttemptAt; if (timeSinceAttempt < minIntervalMs) { if
(entry.data) return { data: entry.data, fromCache: true, stale: true,
fetchedAt: entry.fetchedAt }; // Hard throttle wait await new Promise(r
=> setTimeout(r, minIntervalMs - timeSinceAttempt)); }

try { const data = await doFetch(); return { data, fromCache: false,
stale: false, fetchedAt: entry.fetchedAt }; } catch (e) { // Fallback to
stale if available if (entry.data) return { data: entry.data, fromCache:
true, stale: true, fetchedAt: entry.fetchedAt, error: e.message }; throw
e; } }

async function fetchJson(url, timeoutMs = BD.TIMEOUT_MS) { const
controller = new AbortController(); const t = setTimeout(() =>
controller.abort(), timeoutMs).unref?.();

try { const res = await fetch(url, { signal: controller.signal }); if
(!res.ok) throw new Error(HTTP ${res.status} for ${url}); return await
res.json(); } finally { clearTimeout(t); } }

// Tolerant parser for BD info function parseBdInfo(json) { if (!json ||
typeof json !== ‘object’) return null;

// Normalise list let players = Array.isArray(json.players) ?
json.players : [];

// Clean players players = players.map(p => ({ name: p.name ||
‘(unknown)’, score: parseInt(p.score, 10) || 0, rank: p.rank, indomenow:
p.indomenow, // might be undefined inactive: p.inactive // might be
undefined (seconds) })).filter(p => p.name !== ‘(unknown)’);

// Sort by score desc players.sort((a,b) => b.score - a.score);

return { name: json.name || ‘Unknown Server’, onlinenow:
parseInt(json.onlinenow, 10) || 0, indomenow: parseInt(json.indomenow,
10) || 0, thishour: parseInt(json.thishour, 10) || 0, players }; }

async function fetchBdInfo(url) { const json = await fetchJson(url);
return parseBdInfo(json); }

// Top Scores helpers function updateBdTopScore(regionKey, name, score,
serverName = ““) { const clean = String(name ||”“).trim(); if (!clean)
return;

const rec = { score: Number(score) || 0, seenAt: Date.now(), serverName
}; let changed = false;

// Update region map if (bdTop[regionKey] instanceof Map) { const cur =
bdTop[regionKey].get(clean); const curScore = (cur && typeof cur ===
“object”) ? (cur.score ?? 0) : (Number(cur) || 0); if (rec.score >
curScore) { bdTop[regionKey].set(clean, rec); changed = true; } }

// Update global map only if not West. West scores should not influence
all‑time/global if (regionKey !== ‘West’) { const cur =
bdTop.global.get(clean); const curScore = (cur && typeof cur ===
‘object’) ? (cur.score ?? 0) : (Number(cur) || 0); if (rec.score >
curScore) { bdTop.global.set(clean, rec); changed = true; } }

if (changed) { bdTopMeta.lastUpdatedAt = Date.now(); bdTopDirty = true;
// If this player’s name is linked to a KC account via BD‑to‑KC opt‑in,
// update their highscore in the users collection. Only update if the //
new score beats the existing highscore. The mapping cache is kept //
current by RTDB listeners on config/bdToKC. try { const uid =
globalCache.kcNameToUid?.get(clean); if (uid) {
updatePlayerHighscore(uid, rec.score).catch(() => {}); } } catch {} } }

// Refactored to use shared updater logic function
updateBdTop(regionKey, players) { if (!Array.isArray(players)) return;
for (const p of players) { if (!p.name || p.score == null) continue;
updateBdTopScore(regionKey, p.name, p.score, regionKey); } }

// Update a user’s Battledome highscore in Firebase RTDB. Writes to //
users//bdHighscore with { score, updatedAt } only if the new // score
exceeds the stored value. If no prior score exists, it will be //
created. Errors are logged but do not throw. async function
updatePlayerHighscore(kcUid, newScore) { if (!kcUid ||
!Number.isFinite(newScore)) return; try { const snap = await
rtdb.ref(users/${kcUid}/bdHighscore).get(); let current = 0; if
(snap.exists()) { const val = snap.val(); if (typeof val === ‘object’ &&
val !== null) { current = Number(val.score) || 0; } else { current =
Number(val) || 0; } } if (newScore > current) { await
rtdb.ref(users/${kcUid}/bdHighscore).set({ score: newScore, updatedAt:
admin.database.ServerValue.TIMESTAMP, }); } } catch (e) {
console.error(‘[BD Highscore] Failed to update highscore for’, kcUid,
e?.message || e); } }

// —————————————————————————– // Command Helpers: Set Clips Channel &
Set Events Channel // // The bot previously relied on external functions
defined in another file // (index9.js) to handle the /setclipschannel
and /seteventschannel commands. // These functions perform permission
checks, validate the selected channel, // persist the destination in
RTDB and update the in‑memory cache. The // definitions have been
restored here to eliminate the regression.

/** * Handle the /setclipschannel command. Allows a guild administrator
to choose * the channel where new KC clip posts will be broadcast.
Requires * Manage Guild permission. Writes to config/clipDestinations in
RTDB and * updates globalCache.clipDestinations. @param
{import(‘discord.js’).ChatInputCommandInteraction} interaction */ async
function handleSetClipsChannel(interaction) { if
(!interaction.inGuild()) { return safeReply(interaction, { content: ‘Run
in a server.’, ephemeral: true }); } const picked =
interaction.options.getChannel(‘channel’, true); if (picked.guildId !==
interaction.guildId) { return safeReply(interaction, { content: ‘Channel
not in this server.’, ephemeral: true }); } if (typeof picked.isThread
=== ‘function’ && picked.isThread()) { return safeReply(interaction, {
content: ‘No threads.’, ephemeral: true }); } const chan = await
interaction.client.channels.fetch(picked.id).catch(() => null); if
(!chan || !chan.isTextBased?.()) { return safeReply(interaction, {
content: ‘Invalid text channel.’, ephemeral: true }); } const invokerOk
= interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
if (!invokerOk) { return safeReply(interaction, { content: ‘Manage
Server permission required.’, ephemeral: true }); } const me =
interaction.guild.members.me ?? await
interaction.guild.members.fetchMe().catch(() => null); if (!me ||
!chan.permissionsFor(me).has([PermissionFlagsBits.ViewChannel,
PermissionFlagsBits.SendMessages, PermissionFlagsBits.EmbedLinks])) {
return safeReply(interaction, { content: ‘I need View, Send and Embed
perms there.’, ephemeral: true }); } // Persist to RTDB await
rtdb.ref(config/clipDestinations/${interaction.guildId}).set({
channelId: chan.id, updatedBy: interaction.user.id, updatedAt:
admin.database.ServerValue.TIMESTAMP, }); // Update cache
globalCache.clipDestinations.set(interaction.guildId, chan.id); //
Confirm to user return safeReply(interaction, { content:
✅ Clips will be posted to <#${chan.id}>., ephemeral: true }); }

/** * Handle the /seteventschannel command. Allows a guild administrator
to choose * the channel where new clan battle events (e.g. accepted
challenges) * will be announced. Requires Manage Guild permission.
Writes to * config/battleDestinations in RTDB and updates
globalCache.battleDestinations. @param
{import(‘discord.js’).ChatInputCommandInteraction} interaction */ async
function handleSetEventsChannel(interaction) { if
(!interaction.inGuild()) { return safeReply(interaction, { content: ‘Run
in a server.’, ephemeral: true }); } const picked =
interaction.options.getChannel(‘channel’, true); if (picked.guildId !==
interaction.guildId) { return safeReply(interaction, { content: ‘Channel
not in this server.’, ephemeral: true }); } if (typeof picked.isThread
=== ‘function’ && picked.isThread()) { return safeReply(interaction, {
content: ‘No threads.’, ephemeral: true }); } const chan = await
interaction.client.channels.fetch(picked.id).catch(() => null); if
(!chan || !chan.isTextBased?.()) { return safeReply(interaction, {
content: ‘Invalid text channel.’, ephemeral: true }); } const invokerOk
= interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
if (!invokerOk) { return safeReply(interaction, { content: ‘Manage
Server permission required.’, ephemeral: true }); } const me =
interaction.guild.members.me ?? await
interaction.guild.members.fetchMe().catch(() => null); if (!me ||
!chan.permissionsFor(me).has([PermissionFlagsBits.ViewChannel,
PermissionFlagsBits.SendMessages, PermissionFlagsBits.EmbedLinks])) {
return safeReply(interaction, { content: ‘I need View, Send and Embed
perms there.’, ephemeral: true }); } // Persist to RTDB await
rtdb.ref(config/battleDestinations/${interaction.guildId}).set({
channelId: chan.id, updatedBy: interaction.user.id, updatedAt:
admin.database.ServerValue.TIMESTAMP, }); // Update cache
globalCache.battleDestinations.set(interaction.guildId, chan.id); //
Confirm to user return safeReply(interaction, { content:
✅ Clan battle announcements will post to <#${chan.id}>., ephemeral:
true }); }

// Cached wrapper for BD info async function getBdInfoCached(url) { //
Default fallback region – pick the first available region (West removed)
let region = BD_REGIONS[0] || ‘East’; for(const [k,v] of
Object.entries(BD_SERVERS)) { if (v.url === url) region = k; }

    return swrFetch(url, {
        fetcher: async () => {
            const data = await fetchBdInfo(url);
            // Side effect: update top scores
            if (data && data.players) {
                updateBdTop(region, data.players);
            }
            return data;
        },
        freshMs: BD.INFO_FRESH_MS,
        staleMs: BD.INFO_STALE_MS,
        minIntervalMs: BD.MIN_FETCH_INTERVAL_MS
    });

}

// Maintain the last manual refresh timestamp per guild to throttle user
// initiated refreshes. This map tracks the epoch ms of the most recent
// refresh triggered via the dashboard refresh button. const
bdManualRefreshAt = new Map();

async function announceBdCacheUpdated(regionKey, fetchedAt) { // This
function previously broadcasted cache update messages to the //
configured Battledome channels. To keep channels clean, we now //
suppress these announcements entirely. The cache metadata is still //
maintained by the surrounding code, but no message will be sent. return;
}

// Periodically fetch BD info for all regions in the background to keep
caches warm. // Uses the same SWR wrapper to avoid spamming endpoints;
respects MIN_FETCH_INTERVAL_MS. async function warmBdCachesForever() {
// Define an order for polling. Rotating the order periodically can
balance load. // Polling order. West removed. const order =
[…BD_REGIONS]; while (true) { for (const regionKey of order) { const url
= BD_SERVERS[regionKey]?.url; if (!url) continue; try { const r = await
getBdInfoCached(url); // If the fetch was a real network call (not
served from cache), announce the update if (r && r.data && !r.fromCache)
{ announceBdCacheUpdated(regionKey, r.fetchedAt).catch(() => {}); } }
catch (e) { // Swallow errors; stale data is kept by the SWR wrapper
console.warn([BD Warm] ${regionKey} failed: ${e.message}); } // Ensure
at least MIN_FETCH_INTERVAL_MS spacing between region fetches await new
Promise(res => setTimeout(res, Math.max(3500,
BD.MIN_FETCH_INTERVAL_MS))); } } }

// West Stats Page Helpers async function fetchWestStatsHtml() { const
controller = new AbortController(); const t = setTimeout(() =>
controller.abort(), BD.TIMEOUT_MS).unref?.(); try { const res = await
fetch(BD.WEST_STATS_URL, { signal: controller.signal, headers: {
“user-agent”: “KC-BD-Bot/1.0” } }); if (!res.ok) throw new
Error(HTTP ${res.status} for ${BD.WEST_STATS_URL}); return await
res.text(); } finally { clearTimeout(t); } }

function parseWestStats(html) { try { const marker = “Highest Scorer”;
const idx = html.indexOf(marker); if (idx < 0) return []; const tail =
html.slice(idx);

    const lines = tail.split(/\r?\n/).map(l => l.trim()).filter(Boolean);

    // Expected row format:
    // 20260210 0600 172.99.249.149:444 1 33 8 th ' fercho
    const rowRe = /^(\d{8})\s+(\d{4})\s+([^\s]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(.+)$/;

    const out = [];
    for (const l of lines) {
      const m = l.match(rowRe);
      if (!m) continue;
      out.push({
        date: m[1],
        time: m[2],
        server: m[3],
        rank: Number(m[4]) || 0,
        score: Number(m[5]) || 0,
        players: Number(m[6]) || 0,
        name: (m[7] || "").trim()
      });
    }
    return out;

} catch { return []; } }

async function getWestStatsCached() { return await
swrFetch(BD.WEST_STATS_URL, { fetcher: fetchWestStatsHtml, freshMs:
BD.STATS_FRESH_MS, staleMs: BD.STATS_STALE_MS, minIntervalMs:
BD.MIN_FETCH_INTERVAL_MS }); }

async function seedTopFromWestStats() { const r = await
getWestStatsCached().catch(() => null); if (!r?.data) return;

const rows = parseWestStats(r.data); if (!rows || rows.length === 0) {
console.warn(‘[WestStats] Could not parse rows’); return; } for (const
row of rows) { if (!row.name) continue; updateBdTopScore(“West”,
row.name, row.score, “West Stats Page”); } }

// Hardcoded scores to seed (All-Time Top LB)
const HARDCODED_TOP_SCORES = [
  { name: "BL/\\CKST/\\R2", score: 115 },
  { name: "BL/\\CKST/\\R", score: 109 },
  { name: "[tgrs] WIVATE", score: 108 },          // from screenshot
  { name: "BL/\\CKST/\\R5", score: 105 },         // NEW (EU)
  { name: "BL/\\CKST/\\R4", score: 104 },
  { name: "2", score: 102 },
  { name: "@Flying Tacos rusty af", score: 101 },
  { name: "Lx* <3 INS 2", score: 100 },           // from screenshot
  { name: "mia xo", score: 100 },                 // from screenshot
  { name: "SCAVENGER", score: 98 },
  { name: "BL/\\CKST/\\R3", score: 94 },
  { name: "banana llama", score: 94 },
  { name: "[RR] prime", score: 94 },
  { name: "mino", score: 93 },
  { name: "Lx* <3 INS", score: 89 },
  { name: "Skilla 00.1", score: 89 },

  // keep these to maintain 17+ seeded all-time entries (and match your existing seeds)
  { name: "100", score: 88 },
  { name: "Mia 2", score: 88 },
];

// Hardcoded “starting point” highs (seeded once; future highs overwrite).
// For each region (West, East, EU) we provide a list of player names and scores.
const HARDCODED_TOP_SCORES_BY_REGION = {
  // West seeds removed – region retired

  East: [
    { name: "[3D] F A R I S E O", score: 59 },
    { name: "[WORST] Crazy Mode", score: 58 },
    { name: "|RXS", score: 56 },
    { name: "zP", score: 55 },
    { name: "YT @alwaysfear9263", score: 54 },
    { name: "[SN] MAD COW!!!", score: 50 },
    { name: "[WOVD] risim", score: 46 },
    { name: "The best ever", score: 43 },
    { name: "[KILLER]discord:sjuxes", score: 41 },
    { name: "chills on like 50b ms 4", score: 41 },
    { name: "th’ YT: @sn.Mystic 3", score: 41 },
    { name: "Fz 240hz fan", score: 41 },
    { name: "[3D] F A R I S E O", score: 40 },
    { name: "[DVL] F O R E S T", score: 39 },
    { name: "LK MASTERPERFS", score: 38 },
  ],

  EU: [
    // Blackstar (all EU) + screenshot all-time entries + NEW R5=105
    { name: "BL/\\CKST/\\R2", score: 115 },
    { name: "BL/\\CKST/\\R", score: 109 },
    { name: "[tgrs] WIVATE", score: 108 },        // from screenshot
    { name: "BL/\\CKST/\\R5", score: 105 },       // NEW (EU)
    { name: "BL/\\CKST/\\R4", score: 104 },
    { name: "2", score: 102 },
    { name: "@Flying Tacos rusty af", score: 101 },
    { name: "Lx* <3 INS 2", score: 100 },         // from screenshot
    { name: "mia xo", score: 100 },               // from screenshot
    { name: "SCAVENGER", score: 98 },
    { name: "BL/\\CKST/\\R3", score: 94 },
    { name: "banana llama", score: 94 },
    { name: "[RR] prime", score: 94 },
    { name: "mino", score: 93 },
    { name: "Lx* <3 INS", score: 89 },
    { name: "Skilla 00.1", score: 89 },
    { name: "100", score: 88 },
    { name: "Mia 2", score: 88 },
  ],
};


function seedHardcodedTopScoresOnce() { // Only seed once per process.
Subsequent loads skip. if (bdTopMeta.seededHardcoded) return;

// Seed region maps and global using per‑region hardcoded highs for
(const [regionKey, arr] of
Object.entries(HARDCODED_TOP_SCORES_BY_REGION)) { for (const e of (arr
|| [])) { updateBdTopScore(regionKey, e.name, e.score, “Seed
(hardcoded)”); } } // Mark seeded and dirty so persistence writes seeds
to RTDB bdTopMeta.seededHardcoded = true; bdTopDirty = true; }

// Helper to prevent ugly wrapping function clampName(s, n=45){ s =
String(s || ““); s = s.replace(/+/g,” “).trim(); return s.length > n ?
(s.slice(0, n - 1) +”…“) : s; }

// Helper to encode arbitrary names into valid RTDB keys. Illegal
characters // such as ‘.’, ‘#’, ‘$’, ‘/’, ‘[’, ’]’ are encoded via
encodeURIComponent. // encodeURIComponent does not encode ‘.’, so we
replace it with ‘%2E’. Empty // values map to ‘’. This helper is used
when persisting top scores. function toRtdbKey(raw) { const s =
String(raw ?? ’’).trim(); if (!s) return ’’; return
encodeURIComponent(s).replace(/./g, ‘%2E’); }

// Persistence for Top Scores async function loadBdTop() { try { const
snap = await rtdb.ref(‘config/bdTopScores’).get(); if (snap.exists()) {
const val = snap.val() || {}; // Iterate over global and each region.
West removed – dynamic via BD_REGIONS const keys = [‘global’,
…BD_REGIONS]; for (const k of keys) { if (val[k]) { for (const
[storedKey, entry] of Object.entries(val[k] || {})) { // Retrieve the
display name from the entry when present; fallback to key const
displayName = (entry && typeof entry === ‘object’ && entry.name) ?
entry.name : storedKey; bdTop[k] ??= new Map();
bdTop[k].set(displayName, entry); } } } console.log(‘[BdTop] Loaded
scores from RTDB’); } // Run seeds after loading persistence (keeps
highest) seedHardcodedTopScoresOnce(); } catch (e) {
console.error(‘[BdTop] Failed to load:’, e.message); } // West stats
seeding removed (West retired) }

async function saveBdTop() { const payload = {}; // Build payload for
global and each region using safe keys and original names const keys =
[‘global’, …BD_REGIONS]; for (const k of keys) { payload[k] = {}; if
(!(bdTop[k] instanceof Map)) continue; for (const [name, entry] of
bdTop[k].entries()) { const key = toRtdbKey(name); // Preserve original
name in value. Ensure score is numeric. payload[k][key] = { …(typeof
entry === ‘object’ && entry ? entry : { score: Number(entry) || 0 }),
name }; } } try { await rtdb.ref(‘config/bdTopScores’).set(payload); }
catch (e) { console.error(‘[BdTop] Save failed:’, e.message); } }

// — End Battledome Helpers —

async function hasEmerald(uid) { // Prefer RTDB; fall back to Firestore
if needed try { const snap = await
rtdb.ref(users/${uid}/codesUnlocked).get(); const codes = snap.exists()
? (snap.val() || {}) : {}; if (codes.emerald === true || codes.diamond
=== true || codes.content === true) return true; } catch (_) {}

try { const fsDoc = await
admin.firestore().collection(‘users’).doc(uid).get(); if (fsDoc.exists)
{ const u = fsDoc.data() || {}; // a few fallbacks seen in your data if
(u.codesUnlocked?.emerald === true || u.codesUnlocked?.diamond === true
|| u.postsUnlocked === true || u.canPost === true) { return true; } } }
catch (_) {}

return false; }

async function setKCAvatar(uid, url) { // RTDB await
rtdb.ref(users/${uid}).update({ avatar: url, photoURL: url,
avatarSource: ‘discord’, avatarUpdatedAt: Date.now(), }); // Firestore
(best-effort) try { await
admin.firestore().collection(‘users’).doc(uid).set({ avatar: url,
photoURL: url, avatarSource: ‘discord’, avatarUpdatedAt: Date.now(), },
{ merge: true }); } catch (_) {} }

async function clearKCAvatar(uid) { const { FieldValue } =
admin.firestore; // RTDB: remove override fields by setting to null
await rtdb.ref(users/${uid}).update({ avatar: null, photoURL: null,
avatarSource: null, avatarUpdatedAt: Date.now(), }); // Firestore
(best-effort) try { await
admin.firestore().collection(‘users’).doc(uid).set({ avatar:
FieldValue.delete(), photoURL: FieldValue.delete(), avatarSource:
FieldValue.delete(), avatarUpdatedAt: Date.now(), }, { merge: true }); }
catch (_) {} }

function parseVideoLink(link=’’) { link = String(link).trim();

// YouTube ID const yt =
link.match(/(?:youtu.be/|youtube.com/(?:watch?.*v=|embed/|v/|shorts/))([-]{11})/);
if (yt) return { type: ‘youtube’, ytId: yt[1] };

// TikTok video ID const tt = link.match(/tiktok.com/.*/video/()/); if
(tt) return { type: ‘tiktok’, videoId: tt[1] };

return null; }

function clipLink(d={}) { if (d.type === ‘youtube’ && d.ytId) return
https://youtu.be/${d.ytId}; else if (d.type === ‘tiktok’ && d.videoId)
return https://www.tiktok.com/embed/v2/${d.videoId}; return ’‘; }
function clipThumb(d={}) { if (d.type === ’youtube’ && d.ytId) return
https://i.ytimg.com/vi/${d.ytId}/hqdefault.jpg; return null; // TikTok:
no stable thumbnail without scraping } function reactCount(reactions={})
{ const perEmoji = {}; for (const e of Object.keys(reactions||{}))
perEmoji[e] = Object.keys(reactions[e]||{}).length; const total =
Object.values(perEmoji).reduce((a,b)=>a+b,0); return { perEmoji, total
}; } function sitePostUrl(ownerUid, postId) { // If you have a deep link
on the site, put it here; otherwise link to feed: return
https://kcevents.uk/#socialfeed; }

function getClipsState(interaction) { interaction.client.clipsCache ??=
new Map(); const key = interaction.message?.interaction?.id ||
interaction.id; // root command id return
interaction.client.clipsCache.get(key); } function
getClipByIdx(interaction, idx) { const state =
getClipsState(interaction); if (!state || !Array.isArray(state.list))
throw new Error(‘Clips state missing/expired’); const item =
state.list[idx]; if (!item) throw new Error(‘Unknown clip index’);
return { state, item, idx }; } function clipDbPath(item) { return
users/${item.ownerUid}/posts/${item.postId}; }

async function postingUnlocked(uid) { // same idea as your site:
emerald/diamond/content OR explicit flags try { const snap = await
withTimeout(rtdb.ref(users/${uid}).get(), 6000, RTDB users/${uid});
const u = snap.exists() ? (snap.val() || {}) : {}; const codes =
u.codesUnlocked || {}; return !!(codes.emerald || codes.diamond ||
codes.content || u.postsUnlocked || u.canPost); } catch { return false;
} }

// Optional: global flag like your site uses async function
postsDisabledGlobally() { try { const s = await
withTimeout(rtdb.ref(‘config/postsDisabled’).get(), 4000, ‘RTDB
config/postsDisabled’); return !!s.val(); } catch { return false; } }

// —– Shared helpers for new commands —–

// —– START: Clan Helper Functions —– function getUserScores(uid,
usersData, badgesData) { const user = usersData[uid] || {}; const stats
= user.stats || {}; const b = badgesData[uid] || {}; const offence =
Number(stats.offence ?? b.offence ?? 0) || 0; const defence =
Number(stats.defence ?? b.defence ?? 0) || 0; const overall =
Number(stats.overall ?? b.overall ?? 0) || 0; return { offence, defence,
overall, total: offence + defence + overall }; }

function computeClanScore(clan, usersData, badgesData) { if (!clan ||
!clan.members) return 0; let sum = 0; for (const uid of
Object.keys(clan.members)) { sum += getUserScores(uid, usersData,
badgesData).total; } return sum; } // —– END: Clan Helper Functions —–

// Helper to clamp string lengths const clamp = (s, n=100) => (s ||
’’).toString().slice(0, n);

function normalize(name=’‘){ return
name.toLowerCase().replace(/[^a-z0-9]/g,’’); }

function countReactions(reactionsObj = {}) { // reactions: { “😀”: {
uid:true, … }, “🔥”: { uid:true }, … } let n = 0; for (const emo of
Object.keys(reactionsObj || {})) { n += Object.keys(reactionsObj[emo] ||
{}).length; } return n; }

function countComments(commentsObj = {}) { // comments: { commentId: {
text, uid, time, replies:{ replyId:{…} } } } let n = 0; for (const cid
of Object.keys(commentsObj || {})) { n += 1; const r =
commentsObj[cid]?.replies || {}; n += Object.keys(r).length; } return n;
}

// Map of uid -> displayName/email for quick lookups async function
getAllUserNames() { const CACHE_DURATION = 60 * 1000; // 60 seconds
const now = Date.now(); if (now - globalCache.userNamesFetchedAt <
CACHE_DURATION) { return globalCache.userNames; }

const snap = await withTimeout(rtdb.ref(‘users’).get(), 8000, ‘RTDB
users’); const out = new Map(); if (snap.exists()) { const all =
snap.val() || {}; for (const uid of Object.keys(all)) { const u =
all[uid] || {}; out.set(uid, u.displayName || u.email || ‘(unknown)’); }
} globalCache.userNames = out; globalCache.userNamesFetchedAt = now;
return out; }

// Gather all posts across users; return an array of {ownerUid, postId,
data, score, reacts, comments} async function fetchAllPosts({ platform =
‘all’ } = {}) { const usersSnap = await
withTimeout(rtdb.ref(‘users’).get(), 8000, ‘RTDB users’); const users =
usersSnap.exists() ? usersSnap.val() : {}; const results = []; const
started = Date.now(); let totalPostsSeen = 0;

const tasks = Object.keys(users).map(async uid => { if (Date.now() -
started > 7500 || totalPostsSeen > 500) return; const postsSnap = await
withTimeout(rtdb.ref(users/${uid}/posts).get(), 6000,
RTDB users/${uid}/posts); if (!postsSnap.exists()) return;

    postsSnap.forEach(p => {
      if (Date.now() - started > 7500 || totalPostsSeen > 500) return;
      const post = p.val() || {};
      if (post.draft) return; // skip drafts
      if (post.publishAt && Date.now() < post.publishAt) return; // skip scheduled future posts

      const type = (post.type || '').toLowerCase();
      if (platform === 'youtube' && type !== 'youtube') return;
      if (platform === 'tiktok' && type !== 'tiktok') return;

      const reacts = countReactions(post.reactions || {});
      const comments = countComments(post.comments || {});
      const score = reacts + comments * 2; // simple “popular” score (same spirit as site)

      results.push({
        ownerUid: uid,
        postId: p.key,
        data: post,
        reacts,
        comments,
        score,
      });
      totalPostsSeen++;
    });

});

await Promise.allSettled(tasks); return results; }

async function fetchLatestMessages(limit = 10) { const OVERFETCH =
Math.max(limit * 3, 30);

async function snapToMsgs(snap) { const arr = []; if (snap?.exists &&
snap.exists()) { snap.forEach(c => { const v = c.val() || {}; // filter:
must look like a message const isMsg = typeof v === ‘object’ && (typeof
v.text === ‘string’ || typeof v.user === ‘string’ || typeof v.uid ===
‘string’); if (isMsg) arr.push({ key: c.key, …(v || {}) }); }); // sort
newest first by time (fallback to key if missing) arr.sort((a, b) =>
((b.time || 0) - (a.time || 0)) || (b.key > a.key ? 1 : -1)); } return
arr.slice(0, limit).map(m => ({ …m, path: messages/${m.key} })); }

// Try indexed query first try { const snap = await withTimeout(
rtdb.ref(‘messages’).orderByChild(‘time’).limitToLast(OVERFETCH).get(),
8000, ‘RTDB messages recent’ ); return await snapToMsgs(snap); } catch
(e) { // Fallback if index missing or anything else
console.warn(‘[messages] falling back to unordered fetch:’, e?.message
|| e); const snap = await withTimeout(
rtdb.ref(‘messages’).limitToLast(OVERFETCH).get(), 8000, ‘RTDB messages
fallback’ ); return await snapToMsgs(snap); } }

// Build an embed showing a page of 10 messages (title, text, reply
count) function buildMessagesEmbed(list, nameMap) { const desc =
list.map((m, i) => { const who = m.user || nameMap.get(m.uid) ||
m.username || m.displayName || m.name || ‘(unknown)’; const when =
m.time ? new Date(m.time).toLocaleString() : ‘—’; const rawText = m.text
?? m.message ?? m.content ?? m.body ?? ’‘; const textStr =
String(rawText ||’‘); const text = textStr.length > 100 ?
textStr.slice(0, 100) +’…’ : (textStr || null); const replies =
m.replies ? Object.keys(m.replies).length : 0; return
**${i + 1}. ${who}** — _${when}_\n— ${text || '_no text_'}\nReplies: **${replies}**;
}).join(‘’);

return new EmbedBuilder() .setTitle(‘Messageboard — latest 10’)
.setDescription(desc || ‘No messages yet.’)
.setColor(DEFAULT_EMBED_COLOR) .setFooter({ text: ‘KC Bot • /messages’
}); }

function messageIndexRows(count) { // buttons 1..count (max 10), plus a
Refresh row const rows = []; for (let i = 0; i < count; i += 5) { const
row = new ActionRowBuilder(); for (let j = i; j < Math.min(i + 5,
count); j++) { row.addComponents( new ButtonBuilder()
.setCustomId(msg:view:${j}) .setLabel(String(j + 1))
.setStyle(ButtonStyle.Secondary) ); } rows.push(row); } rows.push( new
ActionRowBuilder().addComponents( new
ButtonBuilder().setCustomId(‘msg:refresh’).setLabel(‘Refresh’).setStyle(ButtonStyle.Primary)
) ); return rows; }

function fmtTime(ms){ const d = new Date(ms || Date.now()); return
d.toLocaleString(); }

function buildMessageDetailEmbed(msg, nameMap) { const who = msg.user ||
nameMap.get(msg.uid) || msg.username || msg.displayName || msg.name ||
‘(unknown)’; const when = msg.time ? new Date(msg.time).toLocaleString()
: ‘—’; const rawText = msg.text ?? msg.message ?? msg.content ??
msg.body ?? ’‘; const text = String(rawText ||’’); const likes =
msg.likes || (msg.likedBy ? Object.keys(msg.likedBy).length : 0) || 0;
const replies = msg.replies ? Object.keys(msg.replies).length : 0;

return new EmbedBuilder() .setTitle(who) .setDescription(text.slice(0,
4096)) .addFields( { name: ‘Likes’, value: String(likes), inline: true
}, { name: ‘Replies’, value: String(replies), inline: true }, )
.setColor(DEFAULT_EMBED_COLOR) .setFooter({ text:
Posted ${when} • KC Bot • /messages }); }

function messageDetailRows(idx, list, path, hasReplies = true) { const
max = list.length - 1; return [ new ActionRowBuilder().addComponents(
new
ButtonBuilder().setCustomId(msg:openIdx:${Math.max(idx-1,0)}).setLabel(‘◀
Prev’).setStyle(ButtonStyle.Secondary).setDisabled(idx<=0), new
ButtonBuilder().setCustomId(msg:openIdx:${Math.min(idx+1,max)}).setLabel(‘Next
▶’).setStyle(ButtonStyle.Secondary).setDisabled(idx>=max), new
ButtonBuilder().setCustomId(msg:thread:${encPath(path)}:0).setLabel(‘Open
thread’).setStyle(ButtonStyle.Secondary).setDisabled(!hasReplies), new
ButtonBuilder().setCustomId(‘msg:back’).setLabel(‘Back to
list’).setStyle(ButtonStyle.Secondary), new
ButtonBuilder().setCustomId(msg:refreshOne:${idx}).setLabel(‘Refresh’).setStyle(ButtonStyle.Secondary),
), new ActionRowBuilder().addComponents( new
ButtonBuilder().setCustomId(msg:like:${encPath(path)}).setLabel(‘❤️
Like/Unlike’).setStyle(ButtonStyle.Primary), new
ButtonBuilder().setCustomId(msg:reply:${encPath(path)}).setLabel(‘↩️
Reply’).setStyle(ButtonStyle.Primary), ), ]; }

async function loadNode(path) { const snap = await
withTimeout(rtdb.ref(path).get(), 6000, RTDB ${path}); return
snap.exists() ? { key: path.split(‘/’).slice(-1)[0], path,
…(snap.val()||{}) } : null; }

async function loadReplies(path) { const snap = await
withTimeout(rtdb.ref(${path}/replies).get(), 6000,
RTDB ${path}/replies); const list = []; if (snap.exists()) {
snap.forEach(c => list.push({ key: c.key, path:
${path}/replies/${c.key}, …(c.val()||{}) }));
list.sort((a,b)=>(b.time||0)-(a.time||0)); } return list; }

function buildThreadEmbed(parent, children, page=0, pageSize=10,
nameMap) { const start = page*pageSize; const slice =
children.slice(start, start+pageSize); const lines = slice.map((r,i)=>{
const who = r.user || nameMap.get(r.uid) || r.username || r.displayName
|| r.name || ‘(unknown)’; const raw = r.text ?? r.message ?? r.content
?? r.body ?? ’‘; const txt = String(raw ||’‘).slice(0,120) ||’(no
text)‘; return **${i+1}. ${who}** — ${txt}; }).join(’‘) ||’No replies
yet’;

const parentWho = parent?.user || nameMap.get(parent?.uid) ||
parent?.username || parent?.displayName || parent?.name || ‘(unknown)’;

return new EmbedBuilder() .setTitle(Thread — ${parentWho})
.setDescription(lines) .setColor(DEFAULT_EMBED_COLOR) .setFooter({ text:
KC Bot • /messages }); }

function threadRows(parentPath, children, page=0, pageSize=10) { const
rows = []; const maxPage = Math.max(0,
Math.ceil(children.length/pageSize)-1); const start = page*pageSize;

const numRow = new ActionRowBuilder(); for (let i=0;
i<Math.min(children.length-start,5); i++) { numRow.addComponents( new
ButtonBuilder()
.setCustomId(msg:openChild:${encPath(children[start+i].path)})
.setLabel(String(i+1)) .setStyle(ButtonStyle.Secondary) ); } if
(numRow.components.length) rows.push(numRow);

const ctrl = new ActionRowBuilder().addComponents( new ButtonBuilder()
.setCustomId(msg:threadPrev:${encPath(parentPath)}:${Math.max(page-1,0)})
.setLabel(‘◀ Page’) .setStyle(ButtonStyle.Secondary)
.setDisabled(page<=0),

    new ButtonBuilder()
      .setCustomId(`msg:threadNext:${encPath(parentPath)}:${Math.min(page+1,maxPage)}`)
      .setLabel('Page ▶')
      .setStyle(ButtonStyle.Secondary)
      .setDisabled(page>=maxPage),

    new ButtonBuilder()
      .setCustomId(`msg:openPath:${encPath(parentPath)}`)
      .setLabel('Back to message')
      .setStyle(ButtonStyle.Secondary),

    new ButtonBuilder()
      .setCustomId('msg:list') // Use a unique ID
      .setLabel('Back to list')
      .setStyle(ButtonStyle.Secondary),

);

rows.push(ctrl); return rows; }

// ===== Clan Battles Helpers ===== const BATTLES_PAGE_SIZE = 10;

/** * Build an embed listing a page of clan battles. Each line is
numbered and includes * the clan names, scheduled date/time and the
current number of participants. * For past battles the winner will be
shown instead of a join count. @param {Array<[string,object]>} list
Array of [battleId, battleData] pairs * @param {string} filterType One
of ‘all’, ‘my’ or ‘past’ * @param {object} clansData Map of clanId ->
clan object * @param {object} usersData Map of userId -> user object
(unused here but passed for future use) * @returns {EmbedBuilder} */
function buildBattlesListEmbed(list = [], filterType = ‘all’, clansData
= {}, usersData = {}) { const titleMap = { all: ‘Upcoming Clan Battles’,
my: ‘Your Clan Battles’, past: ‘Past Clan Battles’ }; const title =
titleMap[filterType] || ‘Clan Battles’;

const now = Date.now(); const desc = list.slice(0,
BATTLES_PAGE_SIZE).map(([bid, b], idx) => { const c1 =
clansData[b.challengerId] || {}; const c2 = clansData[b.targetId] || {};
const name1 = c1.name || ‘Unknown’; const name2 = c2.name || ‘Unknown’;
let line = **${idx + 1}. ${name1} vs ${name2}**; // Format date/time if
available if (b.scheduledTime) { const d = new Date(b.scheduledTime); //
Use UK locale for date/time as user is in Europe/London const dateStr =
d.toLocaleDateString(‘en-GB’, { timeZone: ‘Europe/London’ }); const
timeStr = d.toLocaleTimeString(‘en-GB’, { timeZone: ‘Europe/London’,
hour: ‘2-digit’, minute: ‘2-digit’ }); line += — ${dateStr} ${timeStr};
} // Past battles show winner if (filterType === ‘past’ || b.status ===
‘finished’) { const win = clansData[b.winnerId] || {}; if (win.name)
line += — Winner: ${win.name}; } else { // Upcoming: show participant
count const count = b.participants ? Object.keys(b.participants).length
: 0; line += — Participants: ${count}; } return line; }).join(‘’) || ‘No
battles found.’;

return new EmbedBuilder() .setTitle(title) .setDescription(desc)
.setColor(DEFAULT_EMBED_COLOR) .setFooter({ text: ‘KC Bot •
/clanbattles’ }); }

/** * Build an embed showing details for a single clan battle. Includes
server, date/time, * rules, a list of participants and the winner if the
battle has finished. @param {string} battleId Battle identifier * @param
{object} battle The battle data * @param {object} clansData Map of
clanId -> clan object * @param {object} usersData Map of userId -> user
object * @returns {EmbedBuilder} */ function
buildBattleDetailEmbed(battleId, battle = {}, clansData = {}, usersData
= {}, includeDesc = false) { const c1 = clansData[battle.challengerId]
|| {}; const c2 = clansData[battle.targetId] || {}; const title =
${c1.name || 'Unknown'} vs ${c2.name || 'Unknown'}; const embed = new
EmbedBuilder() .setTitle(title) .setColor(DEFAULT_EMBED_COLOR)
.setFooter({ text: ‘KC Bot • /clanbattles’ });

// Main description: scheduled time if (battle.scheduledTime) { const d
= new Date(battle.scheduledTime); const dateStr =
d.toLocaleDateString(‘en-GB’, { timeZone: ‘Europe/London’ }); const
timeStr = d.toLocaleTimeString(‘en-GB’, { timeZone: ‘Europe/London’,
hour: ‘2-digit’, minute: ‘2-digit’ });
embed.setDescription(Scheduled for **${dateStr} ${timeStr}**); } // Add
server embed.addFields({ name: ‘Server’, value: battle.server || ‘N/A’,
inline: true }); // Add rules embed.addFields({ name: ‘Rules’, value:
battle.rules || ‘N/A’, inline: true }); // Participants list const parts
= battle.participants || {}; const partNames =
Object.keys(parts).map(uid => { const u = usersData[uid] || {}; return
u.displayName || u.username || u.email || uid; }); const partCount =
partNames.length; let partValue = partCount > 0 ? partNames.join(‘,’) :
‘No participants yet’; if (partValue.length > 1024) { // Truncate if too
long partValue = partNames.slice(0, 30).join(‘,’) +
… (+${partCount - 30} more); } embed.addFields({ name:
Participants (${partCount}), value: partValue, inline: false }); //
Winner if finished if (battle.status === ‘finished’) { const win =
clansData[battle.winnerId] || {}; const winName = win.name || ‘Unknown’;
embed.addFields({ name: ‘Winner’, value: winName, inline: false }); }

// Optional extended description // Some battles may include a
description property (or rules text) that provides more // context about
the event. When includeDesc is true and a description exists, // append
it as a separate field. This mirrors the web UI’s info toggle. if
(includeDesc) { const descText = battle.description || battle.desc ||
battle.rules || null; if (descText) { // Clamp very long descriptions to
avoid exceeding embed limits let val = String(descText); if
(val.length > 1024) { val = val.slice(0, 1021) + ‘…’; }
embed.addFields({ name: ‘Description’, value: val, inline: false }); } }
return embed; }

/** * Build an embed for a help page. Each page includes a title,
optional image, and * a standard list of resource links. The color is
consistent with other embeds. @param {object} page - An object with an
image property (URL string). * @returns {EmbedBuilder} */ function
buildHelpEmbed(page = {}) { const embed = new EmbedBuilder()
.setTitle(‘KC Events — Help’) .setColor(DEFAULT_EMBED_COLOR); if
(page.image) { embed.setImage(page.image); } embed.addFields({ name:
‘Links’, value: [ ‘Full Messageboard : https://kcevents.uk/#chatscroll’,
‘Full Clips : https://kcevents.uk/#socialfeed’, ‘Full Voting :
https://kcevents.uk/#voting’, ].join(‘’), }); return embed; }

function buildClipsListEmbed(list=[], page=0, nameMap) { const start =
page * CLIPS.PAGE_SIZE; const slice = list.slice(start, start +
CLIPS.PAGE_SIZE);

const lines = slice.map((p, i) => { const d = p.data || {}; const who =
nameMap.get(p.ownerUid) || ‘(unknown)’; const cap = (d.caption ||
’‘).trim() ||’(no caption)‘; const link = clipLink(d); const meta =
👍 ${p.reacts} • 💬 ${p.comments}; const idx = start + i + 1; return
**${idx}.** ${cap}${link ? — ${link}` : ''}\nUploader: **${who}** •
${meta}`; }).join(’‘) || ’No clips found.’;

return new EmbedBuilder()
.setTitle(Top ${Math.min(list.length, CLIPS.PAGE_SIZE)} Clips)
.setDescription(lines) .setColor(DEFAULT_EMBED_COLOR) .setFooter({ text:
‘KC Bot • /clips’ }); }

function clipsListRows(listLen, page=0) { const rows = []; const start =
page * CLIPS.PAGE_SIZE; const visible = Math.min(CLIPS.PAGE_SIZE,
Math.max(0, listLen - start));

const num = new ActionRowBuilder(); for (let i = 0; i < visible; i++) {
num.addComponents( new ButtonBuilder() .setCustomId(c:o:${start+i}) //
open idx .setLabel(String(start + i + 1))
.setStyle(ButtonStyle.Secondary) ); } if (num.components.length)
rows.push(num);

const maxPage = Math.max(0, Math.ceil(listLen/CLIPS.PAGE_SIZE)-1);
rows.push( new ActionRowBuilder().addComponents( new
ButtonBuilder().setCustomId(c:p:${Math.max(page-1,0)}).setLabel(‘◀
Page’).setStyle(ButtonStyle.Secondary).setDisabled(page<=0), new
ButtonBuilder().setCustomId(c:p:${Math.min(page+1,maxPage)}).setLabel(‘Page
▶’).setStyle(ButtonStyle.Secondary).setDisabled(page>=maxPage), new
ButtonBuilder().setCustomId(‘c:rf’).setLabel(‘Refresh’).setStyle(ButtonStyle.Primary),
) ); return rows; }

function buildClipDetailEmbed(item, nameMap) { const d = item.data ||
{}; const who = nameMap.get(item.ownerUid) || ‘(unknown)’; const link =
clipLink(d); const cap = (d.caption || ’‘).trim() ||’(no caption)‘;
const { perEmoji, total } = reactCount(d.reactions || {}); const
reactsLine = CLIPS.REACTS.map(e => ${e} ${perEmoji[e]||0}).join(’ ’);
const comments = Object.keys(d.comments || {}).length;

const e = new EmbedBuilder() .setTitle(cap.slice(0,256)) .setURL(link ||
undefined) .setDescription([ Uploader: **${who}**,
Reactions: ${reactsLine}  (total ${total}), Comments: **${comments}**,
link, ].filter(Boolean).join(‘’)) .setColor(DEFAULT_EMBED_COLOR)
.setFooter({ text: ‘KC Bot • /clips’ });

const thumb = clipThumb(d); if (thumb) e.setImage(thumb); // shows a
preview; Discord will auto-embed YT URLs when clicked

return e; }

function clipsDetailRows(interactionOrMessage, postPath) { const rows =
[]; const row1 = new ActionRowBuilder(); const client =
interactionOrMessage.client;

// Use interaction.message if available (from button), otherwise
interaction (from slash command reply) const message =
interactionOrMessage.message || interactionOrMessage;

for (const emo of POST_EMOJIS) { const sid = _cacheForMessage(
client.reactCache, message, // Use the message object { postPath, emoji:
emo } ); row1.addComponents( new ButtonBuilder()
.setCustomId(clips:react:${sid}) .setLabel(emo)
.setStyle(ButtonStyle.Secondary) ); } if (row1.components.length)
rows.push(row1);

const sidView = _cacheForMessage( client.reactorsCache, message, // Use
the message object { postPath } );

const sidComments = _cacheForMessage( client.commentsCache, message, //
Use the message object { postPath, page: 0 } );

const commentSid = cacheModalTarget({ postPath }); const row2 = new
ActionRowBuilder().addComponents( new ButtonBuilder()
.setCustomId(clips:reactors:${sidView}) .setLabel(‘👀 View reactors’)
.setStyle(ButtonStyle.Secondary), new ButtonBuilder()
.setCustomId(clips:comments:${sidComments}) .setLabel(‘💬 View
comments’) .setStyle(ButtonStyle.Secondary), new ButtonBuilder()
.setCustomId(clips:comment:${commentSid}) .setLabel(‘💬 Comment’)
.setStyle(ButtonStyle.Primary), new ButtonBuilder()
.setCustomId(‘clips:back’) // Changed from ‘c:b’ to be specific
.setLabel(‘Back to list’) .setStyle(ButtonStyle.Secondary), );
rows.push(row2);

return rows; }

async function loadClipComments(postPath) { const snap = await
rtdb.ref(${postPath}/comments).get(); if (!snap.exists()) return [];
const comments = []; snap.forEach(child => { const data = child.val();
comments.push({ key: child.key, uid: data.uid, user: data.user, // Will
be replaced by nameMap lookup later text: data.text, time: data.time,
repliesCount: data.replies ? Object.keys(data.replies).length : 0, });
}); return comments.sort((a, b) => b.time - a.time); }

function buildClipCommentsEmbed(item, comments, page, nameMap) { const
pageSize = 10; const start = page * pageSize; const pageComments =
comments.slice(start, start + pageSize);

    const description = pageComments.length > 0
        ? pageComments.map((c, i) => {
            const displayName = nameMap.get(c.uid) || c.user || '(unknown)';
            const truncatedText = c.text.length > 120 ? `${c.text.substring(0, 117)}...` : c.text;
            const time = new Date(c.time).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false }) + ', ' + new Date(c.time).toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
            return `${start + i + 1}) **${displayName}** — ${truncatedText}  *(${time})*`;
        }).join('\n')
        : "No comments yet.";

    const caption = item.data?.caption || 'Clip';

    return new EmbedBuilder()
        .setTitle(`Comments — "${caption.slice(0, 200)}"`)
        .setDescription(description)
        .setColor(DEFAULT_EMBED_COLOR)
        .setFooter({ text: 'KC Bot • /clips' });

}

function commentsRows(sid, page, maxPage) { const row = new
ActionRowBuilder(); const prevTarget = Math.max(page - 1, 0); const
nextTarget = Math.min(page + 1, maxPage);

    if (maxPage > 0) {
        row.addComponents(
            new ButtonBuilder()
                .setCustomId(`clips:comments:prev:${sid}:${prevTarget}`)
                .setLabel('◀ Page')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(page <= 0),
            new ButtonBuilder()
                .setCustomId(`clips:comments:next:${sid}:${nextTarget}`)
                .setLabel('Page ▶')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(page >= maxPage),
        );
    }
    row.addComponents(
        new ButtonBuilder()
            .setCustomId('clips:backDetail') // Go back to clip detail, not list
            .setLabel('Back to clip')
            .setStyle(ButtonStyle.Secondary)
    );
    return row;

}

// Votes -> scores async function loadVoteScores() { const [cfgSnap,
votesSnap] = await Promise.all([
withTimeout(rtdb.ref(‘config/liveLeaderboardEnabled’).get(), 6000, ‘RTDB
config’), withTimeout(rtdb.ref(‘votes’).get(), 8000, ‘RTDB votes’), ]);

const live = cfgSnap.exists() ? cfgSnap.val() !== false : true; const
votes = votesSnap.exists() ? votesSnap.val() : {};

const off = {}, def = {}; for (const key of Object.keys(votes)) { const
v = votes[key] || {}; const o = normalize(v.bestOffence || ’‘); const d
= normalize(v.bestDefence ||’’); if (o) off[o] = (off[o] || 0) + 1; if
(d) def[d] = (def[d] || 0) + 1; }

const usersSnap = await withTimeout(rtdb.ref(‘users’).get(), 8000, ‘RTDB
users’); const users = usersSnap.exists() ? usersSnap.val() : {}; const
normToDisplay = {}; for (const uid of Object.keys(users)) { const name =
users[uid]?.displayName || users[uid]?.email || ’’; const k =
normalize(name); if (k && !normToDisplay[k]) normToDisplay[k] = name; }

const sortPairs = obj => Object.entries(obj) .map(([k, n]) => ({ name:
normToDisplay[k] || k, count: n })) .sort((a, b) => b.count - a.count)
.slice(0, 10);

return { live, offence: sortPairs(off), defence: sortPairs(def) }; }

function buildVoteEmbed(scores) { const offLines =
scores.offence.map((x, i) =>
**${i + 1}. ${x.name}** — \${x.count}\``).join('\n') || '_No votes yet_';  const defLines = scores.defence.map((x, i) => `**${i +
1}. ${x.name}** — \`${x.count}``).join(‘’) || ‘No votes yet’;

const e = new EmbedBuilder()
.setTitle(Live Voting Scores ${scores.live ? '' : '(offline)'})
.addFields( { name: ‘Best Offence’, value: offLines, inline: false }, {
name: ‘Best Defence’, value: defLines, inline: false }, )
.setColor(DEFAULT_EMBED_COLOR) .setFooter({ text: ‘KC Bot •
/votingscores’ });

return e; }

async function loadLeaderboardData() { const [usersSnap, badgesSnap] =
await Promise.all([ withTimeout(rtdb.ref(‘users’).get(), 6000, ‘RTDB
users’), withTimeout(rtdb.ref(‘badges’).get(), 6000, ‘RTDB badges’), ]);

const users = (usersSnap && usersSnap.exists()) ? usersSnap.val() : {};
const badges = (badgesSnap && badgesSnap.exists()) ? badgesSnap.val() :
{};

return Object.entries(users).map(([uid, u]) => { const b = badges[uid]
|| {}; return { name: u.displayName || u.email || ‘(unknown)’, colour:
u.profileCustomization?.nameColor || null, overall: parseInt(b.overall
|| 0), offence: parseInt(b.offence || 0), defence: parseInt(b.defence ||
0), }; }); }

function buildLbEmbed(rows, catIdx, page) { const cat = LB.CATS[catIdx];
const start = page * LB.PAGE_SIZE; const slice = […rows] .sort((a,b) =>
(b[cat.key]||0) - (a[cat.key]||0)) .slice(start, start + LB.PAGE_SIZE);

const lines = slice.map((r, i) => { const rank = start + i + 1; return
**${rank}.** ${r.name} — \${r[cat.key] ||
0}`;   });   const embed = new EmbedBuilder()     .setTitle(Leaderboard
— ${cat.label}`) .setDescription(lines.join(‘’) || ‘No data’)
.setColor(DEFAULT_EMBED_COLOR) .setFooter({ text: ‘KC Bot •
/leaderboard’ }); return embed; }

function lbRow(catIdx, page) { return new
ActionRowBuilder().addComponents( new
ButtonBuilder().setCustomId(lb:cat:${(catIdx+2)%3}:${page}).setLabel(‘◀
Category’).setStyle(ButtonStyle.Secondary), new
ButtonBuilder().setCustomId(lb:page:${catIdx}:${Math.max(page-1,0)}).setLabel(‘◀
Page’).setStyle(ButtonStyle.Secondary), new
ButtonBuilder().setCustomId(lb:page:${catIdx}:${page+1}).setLabel(‘Page
▶’).setStyle(ButtonStyle.Secondary), new
ButtonBuilder().setCustomId(lb:cat:${(catIdx+1)%3}:${page}).setLabel(‘Category
▶’).setStyle(ButtonStyle.Secondary), ); }

async function getKCUidForDiscord(discordId) { try { const snap = await
withTimeout( rtdb.ref(discordLinks/${discordId}).get(), 8000, //
increased timeout RTDB discordLinks/${discordId} ); return snap.exists()
? (snap.val() || {}).uid || null : null; } catch { return null; // don’t
throw prior to replying } }

function clampStr(val, max, fallback = ‘—’) { if (val == null) return
fallback; const s = String(val); if (s.length === 0) return fallback;
return s.length > max ? s.slice(0, max) : s; }

async function getKCProfile(uid) { const firestore = admin.firestore();

// 1) RTDB reads (preferred) const [userSnapRT, badgeSnapRT,
postsSnapRT] = await Promise.allSettled([
withTimeout(rtdb.ref(users/${uid}).get(), 6000, RTDB users/${uid}),
withTimeout(rtdb.ref(badges/${uid}).get(), 6000, RTDB badges/${uid}),
withTimeout(rtdb.ref(users/${uid}/posts).get(), 6000,
RTDB users/${uid}/posts), ]);

const safeVal = s => (s && s.status === ‘fulfilled’ && s.value &&
s.value.exists()) ? s.value.val() : null; let user = safeVal(userSnapRT)
|| {}; let badges = safeVal(badgeSnapRT) || {}; let posts =
safeVal(postsSnapRT) || {};

// 2) Firestore fallbacks (if RTDB missing) if (!user ||
Object.keys(user).length === 0) { try { const fsUser = await
withTimeout( firestore.collection(‘users’).doc(uid).get(), 6000,
FS users/${uid} ); if (fsUser.exists) user = fsUser.data() || {}; }
catch (e) { console.warn(‘FS user read timeout/fail:’, e.message); } }
if (!posts || Object.keys(posts).length === 0) { try { const fsPosts =
await withTimeout(
firestore.collection(‘users’).doc(uid).collection(‘posts’).get(), 6000,
FS users/${uid}/posts ); if (!fsPosts.empty) { posts = {};
fsPosts.forEach(d => { posts[d.id] = d.data(); }); } } catch (e) {
console.warn(‘FS posts read timeout/fail:’, e.message); } } // (Badges
also sometimes appear in FS; keep RTDB as source of truth per site)

// ——– Field normalisation (match your site) ——– const about =
user.about ?? user.aboutMe ?? user.bio ?? ‘No “About Me” set.’;

const displayName = user.displayName ?? user.name ?? user.username ??
‘Anonymous User’;

const streak = Number.isFinite(user.loginStreak) ?
String(user.loginStreak) : ‘—’;

// Profile customization colour (tint embed) const custom =
user.profileCustomization || {}; const nameColor = custom.nameColor ||
null; const gradientColor = custom.gradient ?
firstHexFromGradient(custom.gradient) : null;

// Posts visible if content unlocked (diamond/emerald codes or explicit
content) const codesUnlocked = user.codesUnlocked || {}; const
postingAllowed = !!(codesUnlocked.content || codesUnlocked.diamond ||
codesUnlocked.emerald || user.postsUnlocked || user.canPost);

// Build at most 3 post lines as: • “Caption” — let postLines = []; if
(postingAllowed && posts) { const list = Object.entries(posts)
.filter(([,p]) => p && !p.draft && (!p.publishAt || p.publishAt <
Date.now())) // Filter out drafts/scheduled .sort((a, b) =>
(b[1]?.createdAt || 0) - (a[1]?.createdAt || 0)) .slice(0, 3);

    for (const [, p] of list) {
      const cap = (p?.caption || '').trim();
      let link = '';

      if (p?.type === 'youtube' && p?.ytId) {
        link = `https://youtu.be/${p.ytId}`;
      } else if (p?.type === 'tiktok' && p?.videoId) {
        // We only store videoId, so use TikTok’s embed URL which always works.
        link = `https://www.tiktok.com/embed/v2/${p.videoId}`;
      }

      const capPretty = cap ? `"${cap.slice(0, 80)}"` : '(no caption)';
      postLines.push(`• ${capPretty}${link ? ` — ${link}` : ''}`);
    }

} const postsField = !postingAllowed ? ‘Posts locked. Unlock posting on
your profile.’ : (Object.keys(posts).length === 0 ? ‘This user has no
posts.’ : (postLines.join(‘’) || ‘This user has no posts.’));

// Badges summary – same three counted on site +
verified/diamond/emerald const counts = { offence:
parseInt(badges.offence ?? badges.bestOffence ?? 0) || 0, defence:
parseInt(badges.defence ?? badges.bestDefence ?? 0) || 0, overall:
parseInt(badges.overall ?? badges.overallWins ?? 0) || 0, }; const
isVerified = !!(user.emailVerified === true || (user.badges &&
user.badges.verified === true)); const hasDiamond =
!!codesUnlocked.diamond; const hasEmerald = !!codesUnlocked.emerald;

// Convert to human lines (we’ll keep emojis inside Discord text) const
e = EMOJI; const badgeLines = []; if (isVerified)
badgeLines.push(${e.verified ?? '✅'} Verified); if (counts.offence > 0)
badgeLines.push(${e.offence ?? '🏹'} Best Offence x${counts.offence});
if (counts.defence > 0)
badgeLines.push(${e.defence ?? '🛡️'} Best Defence x${counts.defence});
if (counts.overall > 0)
badgeLines.push(${e.overall  ?? '🌟'} Overall Winner x${counts.overall});
if (hasDiamond) badgeLines.push(${e.diamond ?? '💎'} Diamond User); if
(hasEmerald) badgeLines.push(${e.emerald ?? '🟩'} Emerald User);

const customBadges = user.customBadges || {}; for (const key of
Object.keys(customBadges)) { const b = customBadges[key] || {}; const
piece = [b.icon ?? b.emoji, b.name ?? b.label].filter(Boolean).join(’
’); if (piece) badgeLines.push(piece); }

return { id: uid, // Pass the UID through displayName, about, streak,
badgesText: badgeLines.length ? badgeLines.join(‘’) : ‘No badges yet.’,
postsText: postsField, postingUnlocked: postingAllowed, // Pass this
through for the /post command check // embed colour preference:
nameColor > gradient first colour > default embedColor:
hexToInt(nameColor) || hexToInt(gradientColor) || null, }; }

function norm(s=’‘){ return s.toLowerCase().replace(/[^a-z0-9]/g,’’); }

async function getVerifiedNameMap() { const snap = await
withTimeout(rtdb.ref(‘users’).get(), 8000, ‘RTDB users for voting’);
const map = {}; // normalizedName -> original displayName/email if
(snap.exists()) { const all = snap.val() || {}; for (const uid of
Object.keys(all)) { const u = all[uid] || {}; if (u.emailVerified) {
const name = u.displayName || u.email || ’’; const k = norm(name); if (k
&& !map[k]) map[k] = name; } } } return map; }

// ———- Discord Client ———- const client = new Client({ intents:
[GatewayIntentBits.Guilds], partials: [Partials.Channel], });

// Cache for /help pages navigation. Map key = parentId (unique per
invocation). // Each entry: { pages: Array, index: number, userId:
string } client.helpCache = new Map();

// — Short-id caches for /clips actions —// Map key = “|”
client.reactCache = new Map(); client.reactorsCache = new Map();
client.commentsCache = new Map(); client.modalCache = new Map();

function _makeShortId(len = 8) { const abc =
‘abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789’; let s
= ’’; for (let i = 0; i < len; i++) s +=
abc[Math.floor(Math.random()*abc.length)]; return s; } function
cacheModalTarget(payload, ttlMs = 10 * 60 * 1000) { const sid =
_makeShortId(10); client.modalCache.set(sid, payload); setTimeout(() =>
client.modalCache.delete(sid), ttlMs).unref?.(); return sid; } function
readModalTarget(sid) { return client.modalCache.get(sid) || null; }
function _cacheForMessage(map, message, payload, ttlMs = 10 * 60 * 1000)
{ const short = _makeShortId(); const key = ${message.id}|${short};
map.set(key, payload); setTimeout(() => map.delete(key),
ttlMs).unref?.(); return short; } function _readFromCache(map, message,
shortId) { // PATCH: Ensure message is valid before accessing .id const
msgId = message?.id; if (!msgId) return null; return
map.get(${msgId}|${shortId}) || null; }

// ———- Slash Commands (definitions) ———- const linkCmd = new
SlashCommandBuilder() .setName(‘link’) .setDescription(‘Link your
Discord to your KC Events account’);

const badgesCmd = new SlashCommandBuilder() .setName(‘badges’)
.setDescription(‘Show a KC Events profile’) .addUserOption(opt =>
opt.setName(‘user’) .setDescription(‘Show someone else’)
.setRequired(false) );

const whoamiCmd = new SlashCommandBuilder() .setName(‘whoami’)
.setDescription(‘Show your Discord ID and resolved KC UID’);

const dumpCmd = new SlashCommandBuilder() .setName(‘dumpme’)
.setDescription(‘Debug: dump raw keys for your mapped KC UID’);

const lbCmd = new SlashCommandBuilder() .setName(‘leaderboard’)
.setDescription(‘Show the live KC Events leaderboard’);

const clipsCmd = new SlashCommandBuilder() .setName(‘clips’)
.setDescription(‘Top 5 most popular clips’) .addStringOption(o =>
o.setName(‘platform’) .setDescription(‘Filter by platform’)
.addChoices({ name:‘All’, value:‘all’ }, { name:‘YouTube’,
value:‘youtube’ }, { name:‘TikTok’, value:‘tiktok’ })
.setRequired(false) );

const latestFiveCmd = new SlashCommandBuilder() .setName(‘latestfive’)
.setDescription(‘Post the 5 most recently uploaded clips here’)
.addStringOption(o => o.setName(‘platform’) .setDescription(‘Filter by
platform’) .addChoices( { name: ‘All’, value: ‘all’ }, { name:
‘YouTube’, value: ‘youtube’ }, { name: ‘TikTok’, value: ‘tiktok’ }, )
.setRequired(false) );

const messagesCmd = new SlashCommandBuilder() .setName(‘messages’)
.setDescription(‘Show the latest 10 messageboard posts’);

const votingCmd = new SlashCommandBuilder() .setName(‘votingscores’)
.setDescription(‘Show current live voting scores (Offence/Defence)’);

const avatarCmd = new SlashCommandBuilder() .setName(‘syncavatar’)
.setDescription(‘Use your Discord avatar on KC (Emerald users)’)
.addStringOption(o => o.setName(‘action’) .setDescription(‘Choose what
to do’) .addChoices( { name: ‘Set (use Discord avatar)’, value: ‘set’ },
{ name: ‘Revert (remove override)’, value: ‘revert’ }, )
.setRequired(true) );

const postCmd = new SlashCommandBuilder() .setName(‘post’)
.setDescription(‘Create a YouTube or TikTok post on your KC profile’)
.addStringOption(o => o.setName(‘link’) .setDescription(‘YouTube or
TikTok link’) .setRequired(true)) .addStringOption(o =>
o.setName(‘caption’) .setDescription(‘Caption (max 140 chars)’)
.setRequired(true)) .addBooleanOption(o => o.setName(‘draft’)
.setDescription(‘Save as draft (default: false)’) .setRequired(false))
.addStringOption(o => o.setName(‘schedule_at’) .setDescription(‘Schedule
publish time ISO (e.g. 2025-08-21T10:00)’) .setRequired(false));

const postMessageCmd = new SlashCommandBuilder() .setName(‘postmessage’)
.setDescription(‘Post a message to the message board’)
.addStringOption(o => o.setName(‘text’) .setDescription(‘The message to
post’) .setRequired(true) );

const helpCmd = new SlashCommandBuilder() .setName(‘help’)
.setDescription(‘Links to the full KC features’);

const voteCmd = new SlashCommandBuilder() .setName(‘vote’)
.setDescription(‘Vote Best Offence, Best Defence and rate the event’);

const compareCmd = new SlashCommandBuilder() .setName(‘compare’)
.setDescription(‘Compare your KC badges with another player’)
.addUserOption(o => o.setName(‘user’) .setDescription(‘The other Discord
user’) .setRequired(true) );

const setClipsChannelCmd = new SlashCommandBuilder()
.setName(‘setclipschannel’) .setDescription(‘Choose the channel where
new KC clips will be posted.’) .addChannelOption(opt =>
opt.setName(‘channel’) .setDescription(‘Text or Announcement channel in
this server’) .addChannelTypes(ChannelType.GuildText,
ChannelType.GuildAnnouncement) .setRequired(true) );

// Clan listing command const clansCmd = new SlashCommandBuilder()
.setName(‘clans’) .setDescription(‘Browse KC clans and view details’);

// New: Clan Battles command // This command will allow users to view
upcoming clan battles, filter between all battles, // battles involving
their own clan, and past battles. Users who are members of either //
participating clan can sign up to join an upcoming battle. Selecting a
battle reveals // additional details such as server, date/time, rules
and a list of participants. A join // button is shown if the current
user is eligible to participate. This ties into the // battles data
stored under battles in Firebase RTDB. const clanBattlesCmd = new
SlashCommandBuilder() .setName(‘clanbattles’) .setDescription(‘View clan
battles and sign up if your clan is participating’);

// Command: Send Clan Challenge // Allows clan owners to challenge
another clan to a battle. Owners will provide the target clan name or ID
and then fill out // server, date/time and rules via a modal. Only
owners can use this command. const sendClanChallengeCmd = new
SlashCommandBuilder() .setName(‘sendclanchallenge’)
.setDescription(‘Challenge another clan to a battle (owner only)’)
.addStringOption(opt => opt.setName(‘clan’) .setDescription(‘Name or ID
of the target clan’) .setRequired(true) );

// Command: Incoming Challenges // Allows clan owners to view pending
challenges for their clan and accept or decline them. const
incomingChallengesCmd = new SlashCommandBuilder()
.setName(‘incomingchallenges’) .setDescription(‘View and respond to
pending clan battle challenges (owner only)’);

// Command: Get Clan Roles // Assigns the clan role to the invoking
user. If the role doesn’t exist, it will be created with the clan’s name
and icon. const getClanRolesCmd = new SlashCommandBuilder()
.setName(‘getclanroles’) .setDescription(‘Assign yourself your clan role
(creates it if missing)’);

// Command: Set Events Channel // Configures which channel new accepted
clan battles will be announced in this server. Requires Manage Guild.
const setEventsChannelCmd = new SlashCommandBuilder()
.setName(‘seteventschannel’) .setDescription(‘Choose the channel where
new clan battles will be announced.’) .addChannelOption(opt =>
opt.setName(‘channel’) .setDescription(‘Text or Announcement channel in
this server’) .addChannelTypes(ChannelType.GuildText,
ChannelType.GuildAnnouncement) .setRequired(true) );

// Command: BD to KC (NEW) // Allows a user to opt in or out of
recording their Battledome highscore to // their KC account. When
enabled, the bot will update the user’s // bdHighscore in the RTDB
whenever their Battledome name matches // their KC name exactly. Users
may override the KC name used for // matching via the optional name
argument. If no name is provided, // the bot falls back to the user’s KC
displayName. A user must // already be linked to a KC account via /link
for this command to work. const bdToKcCmd = new SlashCommandBuilder()
.setName(‘bdtokc’) .setDescription(‘Opt in/out of tracking your
Battledome highscore to your KC profile’) .addStringOption(o =>
o.setName(‘action’) .setDescription(‘Enable or disable highscore
tracking’) .addChoices( { name: ‘Enable’, value: ‘enable’ }, { name:
‘Disable’, value: ‘disable’ } ) .setRequired(true) ) .addStringOption(o
=> o.setName(‘name’) .setDescription(‘KC name to match (defaults to your
KC profile name)’) .setRequired(false) );

// Command: Battledome // View Battledome servers and who is currently
playing const battledomeCmd = new SlashCommandBuilder()
.setName(‘battledome’) .setDescription(‘View Battledome servers and who
is currently playing’);

// Command: Set Battledome Update Channel (NEW) const
setBattledomeChannelCmd = new SlashCommandBuilder()
.setName(‘setbattledomechannel’) .setDescription(‘Choose the channel for
Battledome join/leave updates’) .addChannelOption(opt =>
opt.setName(‘channel’) .setDescription(‘Text or Announcement channel’)
.addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement)
.setRequired(true) );

// Command: Set Join Logs Channel (NEW) // Allows server admins to
choose a channel where Battledome join/leave notifications // will be
posted. When set, join alerts ping users in the specified channel //
instead of sending DMs. const setJoinLogsChannelCmd = new
SlashCommandBuilder() .setName(‘setjoinlogschannel’)
.setDescription(‘Choose a channel for Battledome join logs (pings
instead of DMs)’) .addChannelOption(opt => opt.setName(‘channel’)
.setDescription(‘Text or Announcement channel’)
.addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement)
.setRequired(true) );

// Command: Notify BD (NEW) const notifyBdCmd = new
SlashCommandBuilder() .setName(‘notifybd’) .setDescription(‘Manage
Battledome notifications’) .addStringOption(o => o.setName(‘region’)
.setDescription(‘Region to subscribe to’) .addChoices( { name: ‘East
Coast (NY)’, value: ‘East’ }, { name: ‘EU’, value: ‘EU’ } )
.setRequired(false) ) .addIntegerOption(o => o.setName(‘threshold’)
.setDescription(‘Only ping if player count reaches this number
(optional)’) .setMinValue(1) .setMaxValue(200) .setRequired(false) )
.addStringOption(o => o.setName(‘action’) .setDescription(‘Manage
subscription’) .addChoices( { name: ‘Subscribe (default)’, value: ‘sub’
}, { name: ‘Unsubscribe’, value: ‘unsub’ }, { name: ‘Turn Off All’,
value: ‘clear’ } ) .setRequired(false) );

// Command: Battledome Leaderboard (NEW) const battledomeLbCmd = new
SlashCommandBuilder() .setName(‘battledomelb’) .setDescription(‘Show
live leaderboard for a Battledome region’) .addStringOption(o =>
o.setName(‘region’) .setDescription(‘Select region’) .addChoices( {
name: ‘East Coast (NY)’, value: ‘East’ }, { name: ‘EU’, value: ‘EU’ } )
.setRequired(true) );

// Command: Battledome Top (NEW) const battledomeTopCmd = new
SlashCommandBuilder() .setName(‘battledometop’) .setDescription(‘Show
all-time top scores recorded across Battledomes’) .addStringOption(o =>
o.setName(‘region’) .setDescription(‘Filter by region’) .addChoices( {
name: ‘All Regions’, value: ‘All’ }, { name: ‘East Coast’, value: ‘East’
}, { name: ‘EU’, value: ‘EU’ } ) .setRequired(false) );

// Command: Battledome Status (NEW) // Provides a snapshot of all
Battledome regions showing who is online, // players in the dome,
players this hour and a top‑10 leaderboard for // each region. This
command can be used at any time to fetch the // latest snapshot on
demand. const battledomeStatusCmd = new SlashCommandBuilder()
.setName(‘battledomestatus’) .setDescription(‘Show live status for all
Battledome regions and top 10 players’);

// Command: Recently Joined (NEW) // Displays recent join and leave
activity across all Battledomes. The // window of activity is
configurable via BD_RECENT_WINDOW_MS (defaults to 15 minutes). const
recentlyJoinedCmd = new SlashCommandBuilder() .setName(‘recentlyjoined’)
.setDescription(‘Show recent join/leave activity across Battledome
regions’);

// Command: Compare BD Scores (NEW) // Compare the top Battledome scores
across regions and globally. Shows the top // players for each region
side by side. This differs from /battledometop // because it presents a
comparative view rather than a single region. const compareBdScoresCmd =
new SlashCommandBuilder() .setName(‘comparebdscores’)
.setDescription(‘Compare top Battledome scores across regions’);

// Command: Slither server leaderboard (NEW) // Fetches the real‑time
status of a specific Slither server from NTL and // displays the top
players, totals and update time. Accepts a numeric // server id
corresponding to NTL’s listing (e.g. 6622). Results are // cached for
about 15 seconds to avoid hammering NTL. const serverCmd = new
SlashCommandBuilder() .setName(‘server’) .setDescription(‘Show Slither
leaderboard for a specific server id (NTL)’) .addIntegerOption(opt =>
opt.setName(‘id’) .setDescription(‘Server id from NTL list (e.g. 6622)’)
.setRequired(true) );

// Register (include it in commands array) const commandsJson = [
linkCmd, badgesCmd, whoamiCmd, dumpCmd, lbCmd, clipsCmd, messagesCmd,
votingCmd, avatarCmd, postCmd, postMessageCmd, helpCmd, voteCmd,
compareCmd, setClipsChannelCmd, latestFiveCmd, clansCmd, clanBattlesCmd,
sendClanChallengeCmd, incomingChallengesCmd, getClanRolesCmd,
setEventsChannelCmd, battledomeCmd, setBattledomeChannelCmd,
setJoinLogsChannelCmd, notifyBdCmd, bdToKcCmd, battledomeLbCmd,
battledomeTopCmd, battledomeStatusCmd, recentlyJoinedCmd,
compareBdScoresCmd, serverCmd].map(c => c.toJSON());

// ———- Register commands on startup ———- async function
registerCommands() { const rest = new REST({ version: ‘10’
}).setToken(process.env.DISCORD_BOT_TOKEN); const clientId =
process.env.DISCORD_CLIENT_ID; const guildId =
process.env.DISCORD_GUILD_ID;

try { if (guildId) { console.log(‘Registering guild commands…’); await
rest.put(Routes.applicationGuildCommands(clientId, guildId), { body:
commandsJson }); console.log(‘Guild commands registered ✅’); } else {
console.log(‘Registering global commands…’); await
rest.put(Routes.applicationCommands(clientId), { body: commandsJson });
console.log(‘Global commands registered ✅ (may take a few minutes to
appear)’); } } catch (err) { console.error(‘Failed to register
commands:’, err); } }

// ———- Interaction handling ———- let clientReady = false; const
MAX_AGE_MS = 15000; // be generous; we’ll still try to ack
client.on(‘interactionCreate’, async (interaction) => { if
(!clientReady) { try { // Use safeReply for consistency, though reply is
fine here await safeReply(interaction, { content: ‘Starting up, try
again in a second.’, ephemeral: true }); } catch {} return; } const age
= Date.now() - interaction.createdTimestamp; if (age > MAX_AGE_MS) {
console.warn([old interaction ~${age}ms] attempting to acknowledge anyway);
}

const seen = globalThis.__seen ??= new Set(); if
(seen.has(interaction.id)) {
console.warn([INT] duplicate seen: ${interaction.id}); return; }
seen.add(interaction.id); setTimeout(() => seen.delete(interaction.id),
60_000);

console.log([INT] ${interaction.isChatInputCommand() ? interaction.commandName : interaction.customId} from ${interaction.user?.tag} age=${Date.now()-interaction.createdTimestamp}ms);

// — Slash Commands — if (interaction.isChatInputCommand()) { const {
commandName } = interaction; const ephemeral =
isEphemeralCommand(commandName);

    // Commands that don't defer (they reply or show a modal immediately)
    if (commandName === 'link') {
      try {
        await safeReply(interaction, { content: `Click to link your account: ${process.env.AUTH_BRIDGE_START_URL}?state=${encodeURIComponent(interaction.user.id)}`, ephemeral: true });
      } catch (err) {
        console.error(`[${commandName}]`, err);
        // Final fallback
        await safeReply(interaction, { content: 'Sorry, something went wrong.', ephemeral: true });
      }
      return;
    }

    if (commandName === 'vote') {
      try {
        // This command replies with a modal
        await showVoteModal(interaction);
      } catch (err) {
        console.error(`[${commandName}]`, err);
        // If showModal fails, we must send a reply
        await safeReply(interaction, { content: 'Sorry, something went wrong.', ephemeral: true });
      }
      return;
    }

    // New: sendclanchallenge opens a modal and must not be deferred
    if (commandName === 'sendclanchallenge') {
      try {
        await handleSendClanChallenge(interaction);
      } catch (err) {
        console.error(`[${commandName}]`, err);
        // If showModal fails, we must send a reply
        await safeReply(interaction, { content: 'Sorry, something went wrong.', ephemeral: true });
      }
      return;
    }

    // --- PATCH (Step 2/4): All other commands are deferred and wrapped ---
    await safeDefer(interaction, { ephemeral });
    try {
        if (commandName === 'whoami') {
            const kcUid = await getKCUidForDiscord(interaction.user.id) || 'not linked';
            await safeReply(interaction, { content: `Discord ID: \`${interaction.user.id}\`\nKC UID: \`${kcUid}\``, ephemeral: true });
        }
        else if (commandName === 'dumpme') {
            const discordId = interaction.user.id;
            const uid = await getKCUidForDiscord(discordId);
            if (!uid) {
                return await safeReply(interaction, { content: 'Not linked. Run `/link` first.', ephemeral: true });
            }
            const [userRT, badgesRT, postsRT] = await Promise.all([
                withTimeout(rtdb.ref(`users/${uid}`).get(), 6000, `RTDB users/${uid}`),
                withTimeout(rtdb.ref(`badges/${uid}`).get(), 6000, `RTDB badges/${uid}`),
                withTimeout(rtdb.ref(`users/${uid}/posts`).get(), 6000, `RTDB users/${uid}/posts`),
            ]);
            const firestore = admin.firestore();
            const [userFS, postsFS] = await Promise.all([
                withTimeout(firestore.collection('users').doc(uid).get(), 6000, `FS users/${uid}`),
                withTimeout(firestore.collection('users').doc(uid).collection('posts').get(), 6000, `FS users/${uid}/posts`),
            ]);
            const brief = (val) => {
                const v = val && typeof val.val === 'function' ? val.val() : val;
                if (!v || typeof v !== 'object') return v ?? null;
                const keys = Object.keys(v);
                const sample = {};
                for (const k of keys.slice(0, 8)) sample[k] = v[k];
                return { keys, sample };
            };
            const payload = {
                uid,
                rtdb: { user: brief(userRT), badges: brief(badgesRT), postsCount: postsRT.exists() ? Object.keys(postsRT.val() || {}).length : 0 },
                firestore: { userExists: userFS.exists, userKeys: userFS.exists ? Object.keys(userFS.data() || {}) : [], postsCount: postsFS.size }
            };
            // --- FIX (Item 1): Fix unterminated string ---
            const json = "```json\n" + JSON.stringify(payload, null, 2) + "\n```";
            await safeReply(interaction, { content: json, ephemeral: true });
            // --- END FIX ---
        }
        else if (commandName === 'leaderboard') {
            const rows = await loadLeaderboardData();
            const catIdx = 0, page = 0;
            const embed = buildLbEmbed(rows, catIdx, page);
            interaction.client.lbCache ??= new Map(); 
            interaction.client.lbCache.set(interaction.id, rows);
            await safeReply(interaction, { content: '', embeds: [embed], components: [lbRow(catIdx, page)] });
        }
        else if (commandName === 'clips') {
            const platform = (interaction.options.getString('platform') || 'all').toLowerCase();
            const all = await fetchAllPosts({ platform });
            if (!all.length) {
                return safeReply(interaction, { content: 'No clips found.' });
            }
            all.sort((a,b)=>b.score-a.score);
            const list = all.slice(0, CLIPS.MAX_LIST);
            const nameMap = await getAllUserNames();
            interaction.client.clipsCache ??= new Map();
            const state = { list, nameMap, page: 0, currentPostPath: null }; // Add currentPostPath
            interaction.client.clipsCache.set(interaction.id, state);
            const embed = buildClipsListEmbed(list, 0, nameMap);
            await safeReply(interaction, { content: '', embeds:[embed], components: clipsListRows(list.length, 0) });
        }
        else if (commandName === 'latestfive') {
            const platform = (interaction.options.getString('platform') || 'all').toLowerCase();
            const all = await fetchAllPosts({ platform });
            if (!all.length) {
              return await safeReply(interaction, { content: 'No clips found.', ephemeral: true });
            }
        
            all.sort((a, b) => (b.data?.createdAt || 0) - (a.data?.createdAt || 0));
            const list = all.slice(0, 5);
        
            const nameMap = await getAllUserNames();
        
            const channel = interaction.channel;
            const me = interaction.guild?.members.me ?? await interaction.guild?.members.fetchMe().catch(() => null);
            if (!me || !channel?.permissionsFor(me)?.has([
              PermissionFlagsBits.ViewChannel,
              PermissionFlagsBits.SendMessages,
              PermissionFlagsBits.EmbedLinks,
            ])) {
              return await safeReply(interaction, { content: 'I need **View Channel**, **Send Messages**, and **Embed Links** here.', ephemeral: true });
            }
        
            // Send initial reply *before* looping
            await safeReply(interaction, { content: `Posting ${list.length} latest clip${list.length > 1 ? 's' : ''}...`, ephemeral: true });

            for (const item of list) {
              const embed = buildClipDetailEmbed(item, nameMap);
              // Use followUp (via safeReply) or channel.send for subsequent messages
              const msg = await channel.send({ embeds: [embed] });
        
              const postPath = clipDbPath(item);
              const rows = clipsDetailRows(msg, postPath); // Pass the message object
              await msg.edit({ components: rows });
        
              await new Promise(r => setTimeout(r, 300));
            }
        
            // Final update to the original reply
            await safeReply(interaction, { content: `✅ Posted ${list.length} latest clip${list.length > 1 ? 's' : ''} here.`, ephemeral: true });
        }
        else if (commandName === 'messages') {
            const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
            const embed = buildMessagesEmbed(list || [], nameMap || new Map());
            await safeReply(interaction, { content: '', embeds: [embed], components: messageIndexRows((list || []).length) });
        }
        else if (commandName === 'votingscores') {
            const scores = await loadVoteScores();
            const embed = buildVoteEmbed(scores);
            const row = new ActionRowBuilder().addComponents(
                new ButtonBuilder().setCustomId('votes:refresh').setLabel('Refresh').setStyle(ButtonStyle.Primary)
            );
            await safeReply(interaction, { content: '', embeds: [embed], components: [row] });
        }
        else if (commandName === 'badges') {
            const target = interaction.options.getUser('user') || interaction.user;
            const discordId = target.id;
            const kcUid = await getKCUidForDiscord(discordId);
            if (!kcUid) {
                return await safeReply(interaction, {
                    content: target.id === interaction.user.id
                    ? 'I can’t find your KC account. Use `/link` to connect it first.'
                    : `I can’t find a KC account linked to **${target.tag}**.`
                });
            }
            const profile = await withTimeout(getKCProfile(kcUid), 8000, `getKCProfile(${kcUid})`);
            if (!profile) {
                return await safeReply(interaction, { content: 'No profile data found.' });
            }
            const title = clampStr(`${profile.displayName} — KC Profile`, 256, 'KC Profile');
            const description = clampStr(profile.about, 4096);
            const badgesVal = clampStr(profile.badgesText, 1024);
            const streakVal = clampStr(profile.streak, 1024, '—');
            const postsVal = clampStr(profile.postsText, 1024);
            // Fetch Battledome highscore if available. Stored under users/<uid>/bdHighscore
            let bdHighscoreVal = '—';
            try {
              const hsSnap = await rtdb.ref(`users/${kcUid}/bdHighscore`).get();
              if (hsSnap.exists()) {
                const val = hsSnap.val();
                if (typeof val === 'object' && val !== null) {
                  bdHighscoreVal = String(val.score ?? '—');
                } else {
                  bdHighscoreVal = String(val ?? '—');
                }
              }
            } catch {}
            const discordAvatar = target.displayAvatarURL({ extension: 'png', size: 128 });
            const embed = new EmbedBuilder()
                .setTitle(title)
                .setThumbnail(discordAvatar)
                .setDescription(description)
                .addFields(
                    { name: 'Badges', value: badgesVal, inline: false },
                    { name: 'Streak', value: streakVal, inline: true },
                    { name: 'Battledome Highscore', value: bdHighscoreVal, inline: true },
                    { name: 'Posts', value: postsVal, inline: false },
                )
                .setColor(profile.embedColor || DEFAULT_EMBED_COLOR)
                .setFooter({ text: 'KC Bot • /badges' });
            const row = new ActionRowBuilder().addComponents(
                new ButtonBuilder()
                    .setStyle(ButtonStyle.Link)
                    .setLabel('Full Profile')
                    .setURL(`https://kcevents.uk/#loginpage?uid=${kcUid}`)
            );
            await safeReply(interaction, { content: '', embeds: [embed], components: [row] });
        }
        else if (commandName === 'syncavatar') {
            const discordId = interaction.user.id;
            const uid = await getKCUidForDiscord(discordId);
            if (!uid) {
                return await safeReply(interaction, { content: 'You are not linked. Use `/link` first.', ephemeral: true });
            }
            const allowed = await hasEmerald(uid);
            if (!allowed) {
                return await safeReply(interaction, { content: 'This feature requires Emerald/profile customisation.', ephemeral: true });
            }
            const action = interaction.options.getString('action');
            if (action === 'set') {
                const url = interaction.user.displayAvatarURL({ extension: 'png', size: 256 });
                await setKCAvatar(uid, url);
                await safeReply(interaction, { content: '✅ Your KC profile picture has been updated to your Discord avatar.', ephemeral: true });
            } else if (action === 'revert') {
                await clearKCAvatar(uid);
                await safeReply(interaction, { content: '✅ Avatar override removed. Your KC profile will use the default/site picture again.', ephemeral: true });
            } else {
                await safeReply(interaction, { content: 'Unknown action.', ephemeral: true });
            }
        }
        else if (commandName === 'post') {
            const discordId = interaction.user.id;
            const uid = await getKCUidForDiscord(discordId);
            if (!uid) {
                return await safeReply(interaction, { content: 'You are not linked. Use `/link` first.', ephemeral: true });
            }
            if (await postsDisabledGlobally()) {
                return await safeReply(interaction, { content: '🚫 Posting is currently disabled by admins.', ephemeral: true });
            }
            const allowed = await postingUnlocked(uid);
            if (!allowed) {
                return await safeReply(interaction, { content: '❌ You don’t have posting unlocked. (Emerald/Diamond or Content access required.)', ephemeral: true });
            }
            const link = interaction.options.getString('link') || '';
            const caption = (interaction.options.getString('caption') || '').slice(0, 140);
            const draft = !!interaction.options.getBoolean('draft');
            const scheduleAtIso = interaction.options.getString('schedule_at') || '';
            const parsed = parseVideoLink(link);
            if (!parsed) {
                return await safeReply(interaction, { content: 'Invalid link. Please provide a YouTube or TikTok link.', ephemeral: true });
            }
            const publishAt = scheduleAtIso ? Date.parse(scheduleAtIso) : null;
            if (scheduleAtIso && !publishAt) {
                return await safeReply(interaction, { content: 'Invalid schedule format. Use ISO 8601 (e.g., 2025-08-21T10:00:00Z)', ephemeral: true });
            }
            const postData = { ...parsed, caption, createdAt: admin.database.ServerValue.TIMESTAMP, createdBy: uid, draft: !!draft, publishAt: Number.isFinite(publishAt) ? publishAt : null };
            const ref = rtdb.ref(`users/${uid}/posts`).push();
            await withTimeout(ref.set(postData), 6000, `write post ${ref.key}`);
            
            await safeReply(interaction, {
                content: [
                  '✅ **Post created!**',
                  `• **Type:** ${postData.type}`,
                  `• **Caption:** ${caption || '(none)'}`,
                  publishAt ? `• **Scheduled:** ${new Date(publishAt).toLocaleString()}` : (draft ? '• **Saved as draft**' : '• **Published immediately**')
                ].join('\n'),
                ephemeral: true
              });
        }
        else if (commandName === 'postmessage') {
            const discordId = interaction.user.id;
            const uid = await getKCUidForDiscord(discordId);
            if (!uid) {
                return await safeReply(interaction, { content: 'You must link your KC account first with /link.', ephemeral: true });
            }
            const nameMap = await getAllUserNames();
            const userName = nameMap.get(uid) || interaction.user.username;
            const text = interaction.options.getString('text');
            const now = Date.now();
            const message = { text, uid, user: userName, time: now, createdAt: now };
            await rtdb.ref('messages').push(message);
            await safeReply(interaction, { content: '✅ Message posted!', ephemeral: true });
        }
        else if (commandName === 'help') {
            // Build multi-page help with navigation buttons. The first two pages use new images
            // supplied by the user; the last page uses the original help image. Store the
            // pages in the helpCache keyed by the interaction ID so that button interactions
            // can page through them later.
            const pages = [
              { image: 'https://raw.githubusercontent.com/kevinmidnight7-sudo/kc-events-discord-bot/da405cc9608290a6bbdb328b13393c16c8a7f116/link%203.png' },
              { image: 'https://raw.githubusercontent.com/kevinmidnight7-sudo/kc-events-discord-bot/da405cc9608290a6bbdb328b13393c16c8a7f116/link4.png' },
              { image: 'https://kevinmidnight7-sudo.github.io/messageboardkc/link.png' },
            ];
            const parentId = interaction.id;
            // Initialise helpCache if needed and store the pages for this session
            interaction.client.helpCache.set(parentId, {
              pages,
              index: 0,
              userId: interaction.user.id,
            });
            // Automatically expire this cache entry after 15 minutes
            setTimeout(() => interaction.client.helpCache.delete(parentId), 15 * 60 * 1000);
            const embed = buildHelpEmbed(pages[0]);
            const prevBtn = new ButtonBuilder()
              .setCustomId(`help:prev:${parentId}`)
              .setLabel('◀')
              .setStyle(ButtonStyle.Secondary)
              .setDisabled(true);
            const nextBtn = new ButtonBuilder()
              .setCustomId(`help:next:${parentId}`)
              .setLabel('▶')
              .setStyle(ButtonStyle.Secondary)
              .setDisabled(pages.length <= 1);
            const row = new ActionRowBuilder().addComponents(prevBtn, nextBtn);
            await safeReply(interaction, { embeds: [embed], components: [row] });
        }
        else if (commandName === 'compare') {
            const youDiscordId = interaction.user.id;
            const otherUser = interaction.options.getUser('user');
            const youUid = await getKCUidForDiscord(youDiscordId);
            const otherUid = await getKCUidForDiscord(otherUser.id);
            if (!youUid)  return safeReply(interaction, { content: 'Link your KC account first with /link.' });
            if (!otherUid) return safeReply(interaction, { content: `I can’t find a KC account linked to **${otherUser.tag}**.` });
            const [youBadgesSnap, otherBadgesSnap, youProfile, otherProfile, youCB, otherCB] = await Promise.all([
                withTimeout(rtdb.ref(`badges/${youUid}`).get(), 6000, 'RTDB badges you'),
                withTimeout(rtdb.ref(`badges/${otherUid}`).get(), 6000, 'RTDB badges other'),
                getKCProfile(youUid),
                getKCProfile(otherUid),
                loadCustomBadges(youUid),
                loadCustomBadges(otherUid),
            ]);
            const youBadges = youBadgesSnap.exists() ? (youBadgesSnap.val() || {}) : {};
            const otherBadges = otherBadgesSnap.exists() ? (otherBadgesSnap.val() || {}) : {};
            const counts = b => ({
                offence: parseInt(b.offence || b.bestOffence || 0) || 0,
                defence: parseInt(b.defence || b.bestDefence || 0) || 0,
                overall: parseInt(b.overall  || b.overallWins  || 0) || 0,
            });
            const a = counts(youBadges);
            const b = counts(otherBadges);
            const left = [`Offence: **${a.offence}**`, `Defence: **${a.defence}**`, `Overall: **${a.overall}**`, youCB.length ? `Custom: ${youCB.join(', ')}` : null].filter(Boolean).join('\n');
            const right = [`Offence: **${b.offence}**`, `Defence: **${b.defence}**`, `Overall: **${b.overall}**`, otherCB.length ? `Custom: ${otherCB.join(', ')}` : null].filter(Boolean).join('\n');
            const embed = new EmbedBuilder()
                .setTitle('Badge comparison')
                .addFields(
                    { name: youProfile.displayName || 'You',   value: left  || 'No badges.', inline: true },
                    { name: otherProfile.displayName || otherUser.tag, value: right || 'No badges.', inline: true },
                )
                .setColor(DEFAULT_EMBED_COLOR)
                .setFooter({ text: 'KC Bot • /compare' });
            await safeReply(interaction, { content: '', embeds:[embed] });
        }
        else if (commandName === 'setclipschannel') {
            // This function now runs inside the try/catch
            await handleSetClipsChannel(interaction);
        }
        else if (commandName === 'clans') {
          // fetch clans, users and badges from RTDB
          const [clansSnap, usersSnap, badgesSnap] = await Promise.all([
            withTimeout(rtdb.ref('clans').get(), 6000, 'RTDB clans'),
            withTimeout(rtdb.ref('users').get(), 6000, 'RTDB users'),
            withTimeout(rtdb.ref('badges').get(), 6000, 'RTDB badges'),
          ]);
          const clansData = clansSnap.val() || {};
          const usersData = usersSnap.val() || {};
          const badgesData = badgesSnap.val() || {};
        
          // build an array of clans with memberCount and score
          const entries = Object.entries(clansData).map(([id, clan]) => {
            const memberCount = clan.members ? Object.keys(clan.members).length : 0;
            const score = computeClanScore(clan, usersData, badgesData);
            return { id, ...clan, memberCount, score };
          });
        
          if (entries.length === 0) {
            return safeReply(interaction, { content: 'There are no clans yet.', embeds: [] });
          }
        
          // sort by score (desc) and pick the top 20
          entries.sort((a, b) => b.score - a.score);
          const top = entries.slice(0, 20);
        
          // build select menu options, clamping lengths
          const options = top.map(c => ({
            label: clamp(c.name, 100),
            description: clamp(`${c.memberCount} members • ${c.score} points`, 100),
            value: c.id,
          }));
        
          const select = new StringSelectMenuBuilder()
            .setCustomId(`clans_select:${interaction.id}`) // Keyed to interaction ID
            .setPlaceholder('Select a clan to view details')
            .addOptions(options);
        
          // store data in a cache keyed by the interaction ID
          interaction.client.clanCache ??= new Map();
          interaction.client.clanCache.set(interaction.id, {
            entries,
            usersData,
            badgesData,
          });
          // Set a timeout to clear this cache
          setTimeout(() => interaction.client.clanCache?.delete(interaction.id), 15 * 60 * 1000).unref();
        
          // send an embed with the menu
          const embed = new EmbedBuilder()
            .setTitle('KC Clans')
            .setDescription('Select a clan below to view its members, owner, description and score.')
            .setColor(DEFAULT_EMBED_COLOR);
        
          await safeReply(interaction, {
            content: '',
            embeds: [embed],
            components: [ new ActionRowBuilder().addComponents(select) ],
          });
        }

        // Handle /clanbattles command
        else if (commandName === 'clanbattles') {
          // Defer early to acknowledge the command
          await safeDefer(interaction);
          try {
            const discordId = interaction.user.id;
            const uid = await getKCUidForDiscord(discordId);
            // Fetch battles, clans and users concurrently
            const [battlesSnap, clansSnap, usersSnap] = await Promise.all([
              withTimeout(rtdb.ref('battles').get(), 8000, 'RTDB battles'),
              withTimeout(rtdb.ref('clans').get(), 8000, 'RTDB clans'),
              withTimeout(rtdb.ref('users').get(), 8000, 'RTDB users'),
            ]);
            const battlesData = battlesSnap && typeof battlesSnap.exists === 'function' && battlesSnap.exists() ? (battlesSnap.val() || {}) : {};
            const clansData   = clansSnap   && typeof clansSnap.exists   === 'function' && clansSnap.exists()   ? (clansSnap.val()   || {}) : {};
            const usersData   = usersSnap   && typeof usersSnap.exists   === 'function' && usersSnap.exists()   ? (usersSnap.val()   || {}) : {};
            // Determine the user’s clan ID, if any
            let userClanId = null;
            if (uid) {
              for (const cid of Object.keys(clansData)) {
                const c = clansData[cid];
                if (c && c.members && c.members[uid]) {
                  userClanId = cid;
                  break;
                }
              }
            }
            // Convert battles into arrays and sort
            const entries = Object.entries(battlesData);
            const upcoming = entries.filter(([_, b]) => b && b.status === 'accepted');
            upcoming.sort((a, b) => ((a[1].scheduledTime || 0) - (b[1].scheduledTime || 0)));
            const past = entries.filter(([_, b]) => b && b.status === 'finished');
            past.sort((a, b) => ((b[1].scheduledTime || 0) - (a[1].scheduledTime || 0)));
            const my = upcoming.filter(([_, b]) => userClanId && (b.challengerId === userClanId || b.targetId === userClanId));
            // Prepare cache
            const cache = {
              lists: { all: upcoming, my: my, past: past },
              filter: 'all',
              clansData,
              usersData,
              uid,
              userClanId
            };
            interaction.client.battleCache ??= new Map();
            interaction.client.battleCache.set(interaction.id, cache);
            // Clean cache after 15 minutes
            setTimeout(() => interaction.client.battleCache?.delete(interaction.id), 15 * 60 * 1000).unref();
            // Build embed and component rows for the default filter
            const list = cache.lists.all;
            const embed = buildBattlesListEmbed(list, 'all', clansData, usersData);
            const rows = [];
            const max = Math.min(list.length, BATTLES_PAGE_SIZE);
            for (let i = 0; i < max; i += 5) {
              const row = new ActionRowBuilder();
              for (let j = i; j < Math.min(i + 5, max); j++) {
                row.addComponents(
                  new ButtonBuilder()
                    .setCustomId(`cb:detail:${interaction.id}:${j}`)
                    .setLabel(String(j + 1))
                    .setStyle(ButtonStyle.Secondary)
                );
              }
              rows.push(row);
            }
            const filterRow = new ActionRowBuilder();
            ['all', 'my', 'past'].forEach(ft => {
              const label = ft === 'all' ? 'All' : (ft === 'my' ? 'My Clan' : 'Past');
              const style = ft === 'all' ? ButtonStyle.Primary : ButtonStyle.Secondary;
              filterRow.addComponents(
                new ButtonBuilder()
                  .setCustomId(`cb:filter:${interaction.id}:${ft}`)
                  .setLabel(label)
                  .setStyle(style)
              );
            });
            rows.push(filterRow);
            await safeReply(interaction, { embeds: [embed], components: rows });
          } catch (err) {
            console.error('[clanbattles]', err);
            await safeReply(interaction, { content: '❌ Failed to load clan battles.', embeds: [], components: [] });
          }
        }
        // Handle /incomingchallenges command (view and respond to pending challenges)
        else if (commandName === 'incomingchallenges') {
          await handleIncomingChallenges(interaction);
        }
        // Handle /getclanroles command (create or assign clan role)
        else if (commandName === 'getclanroles') {
          await handleGetClanRoles(interaction);
        }
        // Handle /seteventschannel command (configure battle announcements)
        else if (commandName === 'seteventschannel') {
          await handleSetEventsChannel(interaction);
        }
        // Handle /battledome command
        else if (commandName === 'battledome') {
          await safeDefer(interaction);
          // Use hard-coded server list instead of fetching
          const servers = Object.values(BD_SERVERS);
        
          const options = servers.map((s, idx) => ({
            label: clampStr(s.name, 100, 'Unknown'),
            description: clampStr(s.region || 'Unknown Region', 100, '—'),
            value: String(idx),
          }));
        
          const parentId = interaction.id;
          interaction.client.bdCache ??= new Map();
          interaction.client.bdCache.set(parentId, { servers }); // Store the array we just built
          setTimeout(() => interaction.client.bdCache?.delete(parentId), 15 * 60 * 1000).unref?.();
        
          const select = new StringSelectMenuBuilder()
            .setCustomId(`bd_select:${parentId}`)
            .setPlaceholder('Select a Battledome server…')
            .addOptions(options);
        
          const embed = new EmbedBuilder()
            .setTitle('Battledome Servers')
            .setDescription('Pick a server to see who is online + who is in the dome right now.')
            .setColor(DEFAULT_EMBED_COLOR)
            .setFooter({ text: 'KC Bot • /battledome' });
        
          return safeReply(interaction, {
            embeds: [embed],
            components: [new ActionRowBuilder().addComponents(select)],
          });
        }
        // Handle /setbattledomechannel
        else if (commandName === 'setbattledomechannel') {
          if (!interaction.inGuild()) return safeReply(interaction, { content: 'Run in a server.', ephemeral: true });
          const picked = interaction.options.getChannel('channel', true);
          if (picked.guildId !== interaction.guildId) return safeReply(interaction, { content: 'Channel not in this server.', ephemeral: true });
          if (typeof picked.isThread === 'function' && picked.isThread()) return safeReply(interaction, { content: 'No threads.', ephemeral: true });
          
          const chan = await interaction.client.channels.fetch(picked.id).catch(()=>null);
          if (!chan || !chan.isTextBased?.()) return safeReply(interaction, { content: 'Invalid text channel.', ephemeral: true });

          const invokerOk = interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
          if (!invokerOk) return safeReply(interaction, { content: 'Manage Server permission required.', ephemeral: true });

          const me = interaction.guild.members.me ?? await interaction.guild.members.fetchMe().catch(()=>null);
          if (!me || !chan.permissionsFor(me).has([PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.EmbedLinks])) {
             return safeReply(interaction, { content: 'I need View, Send, and Embed perms there.', ephemeral: true });
          }

          await rtdb.ref(`config/bdDestinations/${interaction.guildId}`).set({
            channelId: chan.id,
            updatedBy: interaction.user.id,
            updatedAt: admin.database.ServerValue.TIMESTAMP
          });
          globalCache.bdDestinations.set(interaction.guildId, chan.id);
          // Clear any existing status message record for this guild. This ensures
          // that the next update will send a new dashboard message in the new channel.
          await rtdb.ref(`config/bdStatusMessage/${interaction.guildId}`).set({
            channelId: chan.id,
            messageId: null,
            updatedAt: admin.database.ServerValue.TIMESTAMP,
            updatedBy: interaction.user.id
          });
          globalCache.bdStatusMessages.set(interaction.guildId, { channelId: chan.id, messageId: null });
          // Trigger an immediate status update to send or edit the status message in the new channel
          updateBdStatusMessages().catch(() => {});
          return safeReply(interaction, { content: `✅ Battledome updates will post to <#${chan.id}>.`,  
            ephemeral: true });
        }
        // Handle /setjoinlogschannel
        else if (commandName === 'setjoinlogschannel') {
          if (!interaction.inGuild()) return safeReply(interaction, { content: 'Run in a server.',  ephemeral: true });
          const picked = interaction.options.getChannel('channel', true);
          if (picked.guildId !== interaction.guildId) return safeReply(interaction, { content: 'Channel not in this server.',  ephemeral: true });
          if (typeof picked.isThread === 'function' && picked.isThread()) return safeReply(interaction, { content: 'No threads.',  ephemeral: true });
          const chan = await interaction.client.channels.fetch(picked.id).catch(() => null);
          if (!chan || !chan.isTextBased?.()) return safeReply(interaction, { content: 'Invalid text channel.',  ephemeral: true });
          const invokerOk = interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild);
          if (!invokerOk) return safeReply(interaction, { content: 'Manage Server permission required.',  ephemeral: true });
          const me = interaction.guild.members.me ?? await interaction.guild.members.fetchMe().catch(() => null);
          if (!me || !chan.permissionsFor(me).has([PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.EmbedLinks])) {
             return safeReply(interaction, { content: 'I need View, Send, and Embed perms there.',  ephemeral: true });
          }
          await rtdb.ref(`config/bdJoinLogsChannel/${interaction.guildId}`).set({
            channelId: chan.id,
            updatedBy: interaction.user.id,
            updatedAt: admin.database.ServerValue.TIMESTAMP
          });
          globalCache.bdJoinLogs.set(interaction.guildId, chan.id);
          return safeReply(interaction, { content: `✅ Battledome join logs will post to <#${chan.id}>.`,  ephemeral: true });
        }
        // Handle /notifybd
        else if (commandName === 'notifybd') {
          const region = interaction.options.getString('region');
          const threshold = interaction.options.getInteger('threshold');
          const action = interaction.options.getString('action') || 'sub';
          const userId = interaction.user.id;
          const guildId = interaction.guildId;

          if (!guildId) return safeReply(interaction, { content: 'Run in a server.', ephemeral: true });

          const ref = rtdb.ref(`bdNotify/${guildId}/${userId}`);
          // Determine the user's requested action with a default of 'toggle'.
          // This will allow simple toggling when no action is provided.
          const userActionRaw = interaction.options.getString('action');
          const userAction = userActionRaw ? userActionRaw.toLowerCase() : 'toggle';

          // Toggle all region alerts when neither a region nor a threshold is provided.
          // This branch allows `/notifybd` to enable or disable personal Battledome alerts
          // across all servers without specifying a region. Explicit actions such as
          // "on", "off", "sub", "unsub", or "clear" override the toggle behaviour.
          if (!region && typeof threshold !== 'number') {
            // Fetch existing subscriptions for this user
            let currentSubs = [];
            try {
              const subsSnap = await ref.child('regions').get();
              if (subsSnap.exists()) {
                currentSubs = Object.keys(subsSnap.val() || {});
              }
            } catch {}
            // Helper to subscribe to all regions
            async function subscribeAllRegions() {
              const updateData = { enabled: true, mode: 'join', onlyNamedJoins: false };
              for (const r of BD_REGIONS) {
                await ref.child(`regions/${r}`).update(updateData);
              }
              await ref.child('updatedAt').set(admin.database.ServerValue.TIMESTAMP);
            }
            // Helper to unsubscribe from all regions
            async function unsubscribeAllRegions() {
              await ref.remove();
            }
            // Explicitly disable alerts
            if (['clear','off','unsub'].includes(userAction)) {
              await unsubscribeAllRegions();
              return safeReply(interaction, { content: '🔕 Unsubscribed from all Battledome alerts.',  ephemeral: true });
            }
            // Explicitly enable alerts
            if (['on','sub'].includes(userAction)) {
              await subscribeAllRegions();
              return safeReply(interaction, { content: '🔔 Battledome alerts enabled for all regions.',  ephemeral: true });
            }
            // Toggle based on current state
            if (currentSubs.length > 0) {
              await unsubscribeAllRegions();
              return safeReply(interaction, { content: '🔕 Battledome alerts disabled.',  ephemeral: true });
            } else {
              await subscribeAllRegions();
              return safeReply(interaction, { content: '🔔 Battledome alerts enabled for all regions.',  ephemeral: true });
            }
          }

          if (action === 'clear') {
            await ref.remove();
            return safeReply(interaction, { content: '🔕 Unsubscribed from all Battledome alerts.', ephemeral: true });
          }

          // Only require a region when a threshold is provided. When no
          // threshold is given, the command will toggle all alerts at once.
          if (!region && typeof threshold === 'number') {
            return safeReply(interaction, { content: 'Please specify a region.',  ephemeral: true });
          }

          if (action === 'unsub') {
             await ref.child(`regions/${region}`).remove();
             return safeReply(interaction, { content: `🔕 Unsubscribed from **${region}** alerts.`, ephemeral: true });
          } else {
             // Subscribe
             // Prepare subscription payload. Always enable alerts. When a threshold is specified, use
             // 'active' mode and default onlyNamedJoins to false.
             const updateData = {
                 enabled: true,
                 // Use the provided threshold for active alerts; undefined means legacy join mode
                 ...(typeof threshold === 'number' ? { threshold } : {}),
                 // Explicitly store mode so downstream logic can differentiate
                 mode: typeof threshold === 'number' ? 'active' : 'join',
                 // Only ping on named joins when in join mode
                 onlyNamedJoins: false,
                 // Reset alert state on subscribe/threshold change. This ensures the next
                 // poll can trigger when the threshold is crossed.
                 state: 'below',
             };

             await ref.child(`regions/${region}`).update(updateData);
             await ref.child('updatedAt').set(admin.database.ServerValue.TIMESTAMP);

             const msg = typeof threshold === 'number'
                ? `🔔 Subscribed to **${region}**! You will be notified when online players reach **${threshold}**.`
                : `🔔 Subscribed to **${region}** join alerts! (All named joins)`;
             
             return safeReply(interaction, { content: msg, ephemeral: true });
          }
        }
        // Handle /bdtokc (NEW)
        else if (commandName === 'bdtokc') {
          // Allows a linked user to enable or disable Battledome highscore tracking.
          // If enabled, the bot will write the player’s best Battledome score
          // to users/<kcUid>/bdHighscore whenever their player name exactly
          // matches the KC name. Users may override the KC name used for
          // matching via the optional `name` argument.
          const discordId = interaction.user.id;
          const action = (interaction.options.getString('action') || 'enable').toLowerCase();
          const overrideName = interaction.options.getString('name');
          // Ensure the user has linked their KC account
          const kcUid = await getKCUidForDiscord(discordId);
          if (!kcUid) {
            return safeReply(interaction, { content: 'You are not linked. Use `/link` first.',  ephemeral: true });
          }
          if (action === 'disable') {
            // Disable highscore tracking. We don’t delete the node entirely
            // so the mapping remains visible but disabled.
            await rtdb.ref(`config/bdToKC/${discordId}`).update({
              kcUid,
              // Preserve any existing kcName or override it if provided
              ...(overrideName ? { kcName: overrideName } : {}),
              enabled: false,
              updatedAt: admin.database.ServerValue.TIMESTAMP,
            });
            return safeReply(interaction, { content: '✅ Battledome highscore tracking disabled.',  ephemeral: true });
          }
          // Determine KC name for matching. Prefer override, else fetch profile.
          let kcName = overrideName;
          if (!kcName) {
            try {
              const profile = await getKCProfile(kcUid);
              if (profile && profile.displayName) kcName = profile.displayName;
            } catch {}
          }
          // Persist opt‑in mapping
          await rtdb.ref(`config/bdToKC/${discordId}`).set({
            kcUid,
            kcName: kcName || null,
            enabled: true,
            updatedAt: admin.database.ServerValue.TIMESTAMP,
          });
          // Acknowledge
          const nameInfo = kcName ? ` Player names must match **${kcName}** exactly.` : '';
          return safeReply(interaction, { content: `✅ Battledome highscore tracking enabled.${nameInfo}`,  ephemeral: true });
        }

        // Handle /battledomelb (Updated for Cache)
        else if (commandName === 'battledomelb') {
          await safeDefer(interaction);
          const region = interaction.options.getString('region');
          const serverConfig = BD_SERVERS[region];
          if (!serverConfig) return safeReply(interaction, { content: 'Unknown region.', ephemeral: true });

          // Use SWR Cache
          let info;
          let fromCache = false;
          let isStale = false;
          let fetchTime = 0;

          try {
            const r = await getBdInfoCached(serverConfig.url);
            info = r.data;
            fromCache = r.fromCache;
            isStale = r.stale;
            fetchTime = r.fetchedAt;
            // Removed throw for missing data to allow graceful fallback
          } catch (e) {
            // Quiet fail
          }

          if (!info || !info.players) {
             // Show Warming Up Embed instead of error
             const warmEmbed = new EmbedBuilder()
                .setTitle(`Battledome Leaderboard — ${serverConfig.name}`)
                .setDescription('_Cache warming up. Please try again in ~10 seconds._')
                .setFooter({ text: 'Cache warming up' })
                .setColor(DEFAULT_EMBED_COLOR);
             return safeReply(interaction, { embeds: [warmEmbed] });
          }

          const top15 = info.players.slice(0, 15);
          const lines = top15.map((p, i) => {
             const idle = p.inactive && p.inactive > 60 ? ` *(idle ${p.inactive}s)*` : '';
             return `**${i+1}. ${p.name}** — ${p.score}${idle}`;
          }).join('\n');

          // Footer: indicate live vs cached with relative age
          const ageSec = fetchTime ? Math.floor((Date.now() - fetchTime) / 1000) : 0;
          const cacheNote = fromCache ? `⚠️ Cached (${ageSec}s ago)` : `✅ Live (${ageSec}s ago)`;

          const embed = new EmbedBuilder()
            .setTitle(`Battledome Leaderboard — ${serverConfig.name}`)
            .setDescription(lines || '_No players listed._')
            .setFooter({ text: `KC Bot • /battledomelb • ${cacheNote}` })
            .setColor(DEFAULT_EMBED_COLOR);
          
          return safeReply(interaction, { embeds: [embed] });
        }
        // Handle /battledometop (NEW)
        else if (commandName === 'battledometop') {
            await safeDefer(interaction);
            const region = interaction.options.getString('region') || 'All';
            const entries = [];

            if (region === 'All') {
                bdTop.global.forEach((v, k) => {
                    const obj = (v && typeof v === 'object')
                        ? { name: k, score: v.score ?? 0, seenAt: v.seenAt ?? 0, serverName: v.serverName ?? '' }
                        : { name: k, score: Number(v) || 0, seenAt: 0, serverName: '' };
                    entries.push(obj);
                });
            } else if (bdTop[region]) {
                bdTop[region].forEach((v, k) => {
                    const obj = (v && typeof v === 'object')
                        ? { name: k, score: v.score ?? 0, seenAt: v.seenAt ?? 0, serverName: v.serverName ?? '' }
                        : { name: k, score: Number(v) || 0, seenAt: 0, serverName: '' };
                    entries.push(obj);
                });
            } else {
                return safeReply(interaction, { content: 'Unknown region.', ephemeral: true });
            }

            // Sort by score desc and pick top 15
            entries.sort((a, b) => (b.score || 0) - (a.score || 0));
            const top15 = entries.slice(0, 15);

            if (top15.length === 0) {
                return safeReply(
                    interaction,
                    { content: `No top scores recorded yet for **${region}**. Check back later!`, ephemeral: true }
                );
            }

            // Build nicely aligned leaderboard lines. Clamp names to 28 characters and pad so numbers align.
            const lines = top15.map((p, i) => {
                const nm = clampName(p.name, 28).padEnd(30, ' ');
                const sc = String(p.score).padStart(5, ' ');
                return `\`${String(i + 1).padStart(2, ' ')}.\` **${nm}** \`— ${sc}\``;
            }).join('\n');

            const lastUpdate = bdTopMeta.lastUpdatedAt || Date.now();
            // The top list is persisted, so we treat it as cached data; show relative update time
            const embed = new EmbedBuilder()
                .setTitle(region === 'All' ? 'All‑Time Top LB' : `All‑Time Top LB (${region})`)
                .setDescription(lines)
                .setFooter({ text: `Updated <t:${Math.floor(lastUpdate / 1000)}:R> • Cached` })
                .setColor(DEFAULT_EMBED_COLOR);

            return safeReply(interaction, { embeds: [embed] });
        }
        // Handle /battledomestatus (NEW)
        else if (commandName === 'battledomestatus') {
            // Use the unified status embed instead of multiple embeds. This
            // compresses all regions into one cohesive embed for easier
            // reading and to match the updated dashboard style.
            await safeDefer(interaction);
            const embed = buildBdStatusUnifiedEmbed();
            return safeReply(interaction, { embeds: [embed] });
        }
        // Handle /recentlyjoined (NEW)
        else if (commandName === 'recentlyjoined') {
            // Produce a single embed summarising recent activity across all
            // Battledome regions instead of one embed per region.
            await safeDefer(interaction);
            const embed = buildBdRecentUnifiedEmbed();
            return safeReply(interaction, { embeds: [embed] });
        }
        // Handle /comparebdscores (NEW)
        else if (commandName === 'comparebdscores') {
            // Use the unified score comparison embed. This single embed
            // displays the top players for Global, West, East and EU in
            // separate fields.
            await safeDefer(interaction);
            try {
              const embed = buildBdScoreCompareEmbed();
              return safeReply(interaction, { embeds: [embed] });
            } catch (err) {
              console.error('[comparebdscores]', err);
              return safeReply(interaction, { content: 'Failed to build BD score comparison.', ephemeral: true });
            }
        }

        // Handle /server (NEW)
        else if (commandName === 'server') {
            // Fetch realtime leaderboard for a specific Slither server id
            const id = interaction.options.getInteger('id');
            try {
              const { servers, fromCache, fetchedAt } = await getSlitherServersCached();
              const s = servers.get(id);
              if (!s) {
                return safeReply(interaction, {
                  content: `I couldn’t find server **${id}** on NTL right now. Try again in a few seconds.`,
                  embeds: [],
                  components: [],
                  // Return as an ordinary message (not suppressed) since /server is public
                  // and this command is not marked as ephemerally private.
                });
              }
              // Build leaderboard lines. Escape backticks in names.
              const lines = (s.leaderboard.length ? s.leaderboard : []).map(e => {
                const nm = (e.name || '(no name)').replace(/`/g, 'ˋ');
                return `\`${String(e.rank).padStart(2,' ')}.\` **${nm}** — \`${e.score}\``;
              }).join('\n') || '_No leaderboard entries found._';
              const ageSec = Math.floor((Date.now() - fetchedAt) / 1000);
              const cacheNote = fromCache ? `Cached (${ageSec}s)` : `Live (${ageSec}s)`;
              const embed = new EmbedBuilder()
                .setTitle(`🐍 Slither Server ${s.id}`)
                .setDescription(`**${s.ipPort}** — ${s.region}`)
                .addFields(
                  { name: 'Leaderboard (Top 10)', value: lines },
                  { name: 'Total Score', value: String(s.totalScore ?? '—'), inline: true },
                  { name: 'Total Players', value: String(s.totalPlayers ?? '—'), inline: true },
                  { name: 'Updated', value: String(s.updated ?? '—'), inline: true },
                )
                .setFooter({ text: `NTL • ${cacheNote}` });
              return safeReply(interaction, { embeds: [embed] });
            } catch (err) {
              console.error('[server]', err);
              return safeReply(interaction, { content: 'Sorry, failed to fetch server data. Please try again later.', embeds: [], components: [] });
            }
        }

    } catch (err) {
        console.error(`[${commandName}]`, err);
        await safeReply(interaction, { 
            content: '❌ Sorry — something went wrong while processing your request.', 
            ephemeral: true 
        });
    }
    // --- END PATCH (Step 2/4) ---

} // — Button Handlers — // — Modal Submit Handlers — else if
(interaction.isModalSubmit()) { const modalId = interaction.customId ||
’‘; // Handle Battledome alert settings modal submissions. Custom ID
format: // bd:alert_settings_modal:. Extract values, validate, save and
// refresh the dashboard. if
(modalId.startsWith(’bd:alert_settings_modal’)) { try { const parts =
modalId.split(‘:’); const guildId = parts[2] || interaction.guildId; //
Read text input values const minPlayersStr =
interaction.fields.getTextInputValue(‘minPlayers’) || ’‘; const
cooldownStr = interaction.fields.getTextInputValue(’cooldownMinutes’) ||
’‘; let minPlayersVal = undefined; let cooldownVal = undefined; if
(minPlayersStr) { const mp = parseInt(minPlayersStr.replace(//g,’‘),
10); if (!isNaN(mp)) { minPlayersVal = Math.max(1, Math.min(100, mp)); }
} if (cooldownStr) { const cd = parseInt(cooldownStr.replace(//g,’‘),
10); if (!isNaN(cd)) { cooldownVal = Math.max(1, Math.min(1440, cd)); }
} // Persist settings const updates = {}; if (typeof minPlayersVal ===
’number’) updates[config/bdAlertSettings/${guildId}/minPlayers] =
minPlayersVal; if (typeof cooldownVal === ‘number’)
updates[config/bdAlertSettings/${guildId}/cooldownMinutes] =
cooldownVal; if (Object.keys(updates).length > 0) { await
rtdb.ref().update(updates); } await
rtdb.ref(config/bdAlertSettings/${guildId}/updatedAt).set(admin.database.ServerValue.TIMESTAMP);
// Acknowledge await safeReply(interaction, { content: ‘✅ Alert
settings saved.’, embeds: [], components: [], ephemeral: true }); //
Refresh dashboard updateBdStatusMessages().catch(() => {}); } catch
(err) { console.error(‘[bd alert settings modal]’, err); await
safeReply(interaction, { content: ‘❌ Failed to save settings.’, embeds:
[], components: [], ephemeral: true }); } return; } } else if
(interaction.isButton()) { const id = interaction.customId; // Handle
Battledome dashboard buttons (bd: prefix). These controls // provide a
clean UI for managing alerts, refreshing data and // toggling advanced
details. All BD buttons include the guild ID // after the action for
scoping (e.g. bd:controls:). We // process BD buttons before any other
button logic. if (id && id.startsWith(‘bd:’)) { const parts =
id.split(‘:’); // parts: [‘bd’, action, guildId] const action = parts[1]
|| ’‘; const guildId = parts[2] || interaction.guildId; // Controls:
display an ephemral control panel summarising settings if (action ===
’controls’) { // Fetch current settings (alerts enabled, alert settings,
user server prefs) // Alerts enabled per guild let alertsEnabled = true;
try { const snap = await
rtdb.ref(config/bdAlertsEnabled/${guildId}).get(); if (snap.exists() &&
snap.val() === false) alertsEnabled = false; } catch {} // Alert
settings: minPlayers and cooldown (in minutes) let minPlayers =
undefined; let cooldownMinutes = undefined; try { const snap = await
rtdb.ref(config/bdAlertSettings/${guildId}).get(); if (snap.exists()) {
const val = snap.val() || {}; if (typeof val.minPlayers === ‘number’)
minPlayers = val.minPlayers; if (typeof val.cooldownMinutes ===
‘number’) cooldownMinutes = val.cooldownMinutes; } } catch {} // User
server subscriptions let servers = []; try { const subSnap = await
rtdb.ref(bdNotify/${guildId}/${interaction.user.id}/regions).get(); if
(subSnap.exists()) { servers = Object.keys(subSnap.val() || {}); } }
catch {} const serverList = servers.length ? servers.join(‘,’) : ‘None’;
// DM preference for this user in this guild (default false if missing)
let dmEnabled = false; try { const snap = await
rtdb.ref(config/bdAlertPrefs/${guildId}/${interaction.user.id}/dmEnabled).get();
if (snap.exists() && snap.val() === true) dmEnabled = true; } catch {}
// Build embed including DM notification status const embed = new
EmbedBuilder() .setTitle(‘Battledome Controls’) .setDescription(‘Manage
your alert preferences and settings.’) .setColor(DEFAULT_EMBED_COLOR)
.addFields( { name: ‘Alerts’, value: alertsEnabled ? ‘✅ Enabled’ : ‘❌
Disabled’, inline: true }, { name: ‘Servers’, value: serverList, inline:
true }, { name: ‘Cooldown’, value: cooldownMinutes ?
${cooldownMinutes} min : ‘—’, inline: true }, { name: ‘Min Players’,
value: minPlayers ? String(minPlayers) : ‘—’, inline: true }, { name:
‘DM Notifications’, value: dmEnabled ? ‘✅ Enabled’ : ‘❌ Disabled’,
inline: true } ); // Build control buttons const toggleBtn = new
ButtonBuilder() .setCustomId(bd:toggle_alerts:${guildId})
.setLabel(alertsEnabled ? ‘Disable Alerts’ : ‘Enable Alerts’)
.setStyle(alertsEnabled ? ButtonStyle.Danger : ButtonStyle.Success);
const settingsBtn = new ButtonBuilder()
.setCustomId(bd:alert_settings:${guildId}) .setLabel(‘Alert Settings’)
.setStyle(ButtonStyle.Secondary); const serversBtn = new ButtonBuilder()
.setCustomId(bd:open_server_filter:${guildId}) .setLabel(‘Select
Servers’) .setStyle(ButtonStyle.Secondary); // New button for toggling
DM notifications const dmBtn = new ButtonBuilder()
.setCustomId(bd:toggle_dm:${guildId}) .setLabel(dmEnabled ? ‘Disable
DMs’ : ‘Enable DMs’) .setStyle(dmEnabled ? ButtonStyle.Danger :
ButtonStyle.Success); const row = new
ActionRowBuilder().addComponents(toggleBtn, settingsBtn, serversBtn,
dmBtn); await safeReply(interaction, { embeds: [embed], components:
[row], ephemeral: true }); return; } // Refresh: manually refresh BD
data with a 5s cooldown per guild if (action === ‘refresh’) { const last
= bdManualRefreshAt.get(guildId) || 0; const now = Date.now(); const
diff = now - last; if (diff < 5000) { const secondsLeft =
Math.ceil((5000 - diff) / 1000); return safeReply(interaction, {
content: ⏳ Please wait ${secondsLeft}s before refreshing again.,
ephemeral: true }); } bdManualRefreshAt.set(guildId, now); // Trigger
fresh fetch for all regions in sequence for (const regionKey of
BD_REGIONS) { try { await checkRegion(regionKey); } catch {} } //
Immediately update the status message updateBdStatusMessages().catch(()
=> {}); return safeReply(interaction, { content: ‘🔄 Refreshing
Battledome data…’, ephemeral: true }); } // Details: toggle advanced
view for this guild if (action === ‘details’) { let showAdvanced =
false; try { const snap = await
rtdb.ref(config/bdShowAdvanced/${guildId}).get(); if (snap.exists() &&
snap.val() === true) showAdvanced = true; } catch {} // Toggle
showAdvanced = !showAdvanced; await
rtdb.ref(config/bdShowAdvanced/${guildId}).set(showAdvanced); // Update
message updateBdStatusMessages().catch(() => {}); return
safeReply(interaction, { content: ‘📊 Details view updated.’, ephemeral:
true }); } // Toggle alerts: enable/disable all alerts for this guild if
(action === ‘toggle_alerts’) { let alertsEnabled = true; try { const
snap = await rtdb.ref(config/bdAlertsEnabled/${guildId}).get(); if
(snap.exists() && snap.val() === false) alertsEnabled = false; } catch
{} alertsEnabled = !alertsEnabled; await
rtdb.ref(config/bdAlertsEnabled/${guildId}).set(alertsEnabled); //
update status message to reflect new toggle if needed
updateBdStatusMessages().catch(() => {}); return safeReply(interaction,
{ content: alertsEnabled ? ‘🔔 Alerts enabled.’ : ‘🔕 Alerts disabled.’,
ephemeral: true }); } // Toggle DM notifications: enable/disable DM
alerts for this user in this guild if (action === ‘toggle_dm’) { //
fetch current dmEnabled (default false) let dmEnabled = false; try {
const snap = await
rtdb.ref(config/bdAlertPrefs/${guildId}/${interaction.user.id}/dmEnabled).get();
if (snap.exists() && snap.val() === true) dmEnabled = true; } catch {}
// invert dmEnabled = !dmEnabled; // persist await
rtdb.ref(config/bdAlertPrefs/${guildId}/${interaction.user.id}).update({
dmEnabled, updatedAt: admin.database.ServerValue.TIMESTAMP, updatedBy:
interaction.user.id }); // respond return safeReply(interaction, {
content: dmEnabled ? ‘✅ DM notifications enabled.’ : ‘✅ DM
notifications disabled.’, embeds: [], components: [], }); } // Alert
settings: open a modal to edit minPlayers and cooldown if (action ===
‘alert_settings’) { // Build modal const modal = new ModalBuilder()
.setCustomId(bd:alert_settings_modal:${guildId}) .setTitle(‘Edit
Battledome Alert Settings’); const minPlayersInput = new
TextInputBuilder() .setCustomId(‘minPlayers’) .setLabel(‘Minimum players
to trigger (1–100)’) .setStyle(TextInputStyle.Short)
.setRequired(false); const cooldownInput = new TextInputBuilder()
.setCustomId(‘cooldownMinutes’) .setLabel(‘Cooldown in minutes
(1–1440)’) .setStyle(TextInputStyle.Short) .setRequired(false);
modal.addComponents( new
ActionRowBuilder().addComponents(minPlayersInput), new
ActionRowBuilder().addComponents(cooldownInput) ); await
interaction.showModal(modal); return; } // Open server filter: present a
multi-select for region subscriptions if (action ===
‘open_server_filter’) { // Fetch current selections let selected = [];
try { const snap = await
rtdb.ref(bdNotify/${guildId}/${interaction.user.id}/regions).get(); if
(snap.exists()) { selected = Object.keys(snap.val() || {}); } } catch {}
const select = new StringSelectMenuBuilder()
.setCustomId(bd:server_filter_select:${guildId}) .setMinValues(0)
.setMaxValues(BD_REGIONS.length) .setPlaceholder(‘Select regions’)
.addOptions( { label: ‘East Coast (NY)’, value: ‘East’, default:
selected.includes(‘East’), description: ‘Subscribe to East Battledome
alerts’ }, { label: ‘EU’, value: ‘EU’, default: selected.includes(‘EU’),
description: ‘Subscribe to EU Battledome alerts’ } ); const row = new
ActionRowBuilder().addComponents(select); const embed = new
EmbedBuilder() .setTitle(‘Select Battledome Servers’)
.setDescription(‘Choose which regions you want alerts for.’)
.setColor(DEFAULT_EMBED_COLOR); await safeReply(interaction, { embeds:
[embed], components: [row], ephemeral: true }); return; } return; } //
Handle multi‑page /help navigation buttons. Custom IDs are of the form
// help:<prev|next>:. Only the original invoker may use these controls.
if (id && id.startsWith(‘help:’)) { const parts = id.split(‘:’); const
dir = parts[1]; const parentId = parts[2]; const cache =
interaction.client.helpCache?.get(parentId); if (!cache) { return
safeReply(interaction, { content: ‘This help message has expired. Run
/help again.’, ephemeral: true }); } if (interaction.user.id !==
cache.userId) { return safeReply(interaction, { content: ‘Only the
person who ran this command can use these controls.’, ephemeral: true
}); } // Compute new page index let newIndex = cache.index; if (dir ===
‘next’) newIndex += 1; else if (dir === ‘prev’) newIndex -= 1; newIndex
= Math.max(0, Math.min(cache.pages.length - 1, newIndex)); cache.index =
newIndex; const embedHelp = buildHelpEmbed(cache.pages[newIndex]); const
prevDisabled = newIndex === 0; const nextDisabled = newIndex ===
cache.pages.length - 1; const prevBtn2 = new ButtonBuilder()
.setCustomId(help:prev:${parentId}) .setLabel(‘◀’)
.setStyle(ButtonStyle.Secondary) .setDisabled(prevDisabled); const
nextBtn2 = new ButtonBuilder() .setCustomId(help:next:${parentId})
.setLabel(‘▶’) .setStyle(ButtonStyle.Secondary)
.setDisabled(nextDisabled); const rowHelp = new
ActionRowBuilder().addComponents(prevBtn2, nextBtn2); await
safeDefer(interaction, { intent: ‘update’ }); // When updating an
existing message, omit the ephemeral flag; the interaction // is already
replied to. return safeReply(interaction, { embeds: [embedHelp],
components: [rowHelp] }); } try { // For clan battle controls (cb:
prefix) if (id.startsWith(‘cb:’)) { const parts = id.split(‘:’); //
cb::: const action = parts[1]; const parentId = parts[2]; const param =
parts[3]; // Restrict interactions to the user who ran the command const
invokerId = interaction.message?.interaction?.user?.id; if (invokerId &&
invokerId !== interaction.user.id) { return safeReply(interaction, {
content: ‘Only the person who ran this command can use these controls.’,
ephemeral: true }); } // Fetch cache const cache =
interaction.client.battleCache?.get(parentId); if (!cache) { return
safeReply(interaction, { content: ‘This battle list has expired. Run
/clanbattles again.’, ephemeral: true }); } // Handle actions if (action
=== ‘filter’) { const newFilter = param; if
([‘all’,‘my’,‘past’].includes(newFilter)) { return
safeReply(interaction, { content: ‘Invalid filter.’, ephemeral: true });
} cache.filter = newFilter; const list = cache.lists[newFilter] || [];
// Build embed and rows const embed = buildBattlesListEmbed(list,
newFilter, cache.clansData, cache.usersData); const rows = []; const max
= Math.min(list.length, BATTLES_PAGE_SIZE); for (let i = 0; i < max; i
+= 5) { const row = new ActionRowBuilder(); for (let j = i; j <
Math.min(i + 5, max); j++) { row.addComponents( new ButtonBuilder()
.setCustomId(cb:detail:${parentId}:${j}) .setLabel(String(j + 1))
.setStyle(ButtonStyle.Secondary) ); } rows.push(row); } const filterRow
= new ActionRowBuilder(); [‘all’, ‘my’, ‘past’].forEach(ft => { const
label = ft === ‘all’ ? ‘All’ : (ft === ‘my’ ? ‘My Clan’ : ‘Past’); const
style = ft === newFilter ? ButtonStyle.Primary : ButtonStyle.Secondary;
filterRow.addComponents( new ButtonBuilder()
.setCustomId(cb:filter:${parentId}:${ft}) .setLabel(label)
.setStyle(style) ); }); rows.push(filterRow); await
safeDefer(interaction, { intent: ‘update’ }); return
safeReply(interaction, { embeds: [embed], components: rows }); } else if
(action === ‘detail’) { const idx = parseInt(param, 10); const
filterType = cache.filter || ‘all’; const list = cache.lists[filterType]
|| []; if (isNaN(idx) || idx < 0 || idx >= list.length) { return
safeReply(interaction, { content: ‘Invalid selection.’, ephemeral: true
}); } const [battleId, battle] = list[idx]; // Build detail embed //
Build detail embed (default view does not show extended description)
const embed = buildBattleDetailEmbed(battleId, battle, cache.clansData,
cache.usersData, /* includeDesc / false); // Determine join eligibility
let joinBtn; const canJoin = ( battle.status !== ‘finished’ && cache.uid
&& cache.userClanId && (battle.challengerId === cache.userClanId ||
battle.targetId === cache.userClanId) && !(battle.participants &&
battle.participants[cache.uid]) ); if (canJoin) { joinBtn = new
ButtonBuilder() .setCustomId(cb:join:${parentId}:${battleId})
.setLabel(‘Join’) .setStyle(ButtonStyle.Success); } else { // If already
joined, show Leave button; otherwise disabled Join const joined =
battle.participants && battle.participants[cache.uid]; if (joined) {
joinBtn = new ButtonBuilder()
.setCustomId(cb:leave:${parentId}:${battleId}) .setLabel(‘Leave’)
.setStyle(ButtonStyle.Danger); } else { joinBtn = new ButtonBuilder()
.setCustomId(‘cb:join:disabled’) .setLabel(‘Join’)
.setStyle(ButtonStyle.Secondary) .setDisabled(true); } } // Info button
to toggle extended description const infoBtn = new ButtonBuilder()
.setCustomId(cb:info:${parentId}:${battleId}:show) .setLabel(‘Info’)
.setStyle(ButtonStyle.Secondary); const backBtn = new ButtonBuilder()
.setCustomId(cb:list:${parentId}) .setLabel(‘Back’)
.setStyle(ButtonStyle.Secondary); const row = new
ActionRowBuilder().addComponents(joinBtn, infoBtn, backBtn); await
safeDefer(interaction, { intent: ‘update’ }); return
safeReply(interaction, { embeds: [embed], components: [row] }); } else
if (action === ‘list’) { const filterType = cache.filter || ‘all’; const
list = cache.lists[filterType] || []; const embed =
buildBattlesListEmbed(list, filterType, cache.clansData,
cache.usersData); const rows = []; const max = Math.min(list.length,
BATTLES_PAGE_SIZE); for (let i = 0; i < max; i += 5) { const row = new
ActionRowBuilder(); for (let j = i; j < Math.min(i + 5, max); j++) {
row.addComponents( new ButtonBuilder()
.setCustomId(cb:detail:${parentId}:${j}) .setLabel(String(j + 1))
.setStyle(ButtonStyle.Secondary) ); } rows.push(row); } const filterRow
= new ActionRowBuilder(); [‘all’, ‘my’, ‘past’].forEach(ft => { const
label = ft === ‘all’ ? ‘All’ : (ft === ‘my’ ? ‘My Clan’ : ‘Past’); const
style = ft === filterType ? ButtonStyle.Primary : ButtonStyle.Secondary;
filterRow.addComponents( new ButtonBuilder()
.setCustomId(cb:filter:${parentId}:${ft}) .setLabel(label)
.setStyle(style) ); }); rows.push(filterRow); await
safeDefer(interaction, { intent: ‘update’ }); return
safeReply(interaction, { embeds: [embed], components: rows }); } else if
(action === ‘join’) { const battleId = param; // Ensure user is eligible
if (!cache.uid) { return safeReply(interaction, { content: ‘Link your KC
account with /link first to join battles.’, ephemeral: true }); } if
(!cache.userClanId) { return safeReply(interaction, { content: ‘You must
be in a clan to join this battle.’, ephemeral: true }); } // Find battle
in cache lists let battleRef = null; for (const k of
Object.keys(cache.lists)) { const arr = cache.lists[k]; for (let i = 0;
i < arr.length; i++) { if (arr[i][0] === battleId) { battleRef =
arr[i][1]; break; } } if (battleRef) break; } if (!battleRef) { return
safeReply(interaction, { content: ‘Battle not found. It may have
expired.’, ephemeral: true }); } // Check eligibility again based on
battle data const inClan = (battleRef.challengerId === cache.userClanId
|| battleRef.targetId === cache.userClanId); const alreadyJoined =
battleRef.participants && battleRef.participants[cache.uid]; if
(!inClan) { return safeReply(interaction, { content: ‘You are not a
member of either participating clan.’, ephemeral: true }); } if
(battleRef.status === ‘finished’) { return safeReply(interaction, {
content: ‘This battle has already finished.’, ephemeral: true }); } if
(alreadyJoined) { return safeReply(interaction, { content: ‘You have
already joined this battle.’, ephemeral: true }); } // Perform join in
database try { await withTimeout(
rtdb.ref(battles/${battleId}/participants/${cache.uid}).set(cache.userClanId),
8000, join battle ${battleId} ); } catch (err) { console.error(‘[join
battle]’, err); return safeReply(interaction, { content: ‘Failed to join
the battle. Please try again later.’, ephemeral: true }); } // Update
local cache for (const k of Object.keys(cache.lists)) { const arr =
cache.lists[k]; for (let i = 0; i < arr.length; i++) { if (arr[i][0] ===
battleId) { arr[i][1].participants = arr[i][1].participants || {};
arr[i][1].participants[cache.uid] = cache.userClanId; } } } // Build
updated detail view (default view without description) const embed =
buildBattleDetailEmbed(battleId, battleRef, cache.clansData,
cache.usersData, / includeDesc / false); // Show a Leave button now that
the user has joined const leaveBtn = new ButtonBuilder()
.setCustomId(cb:leave:${parentId}:${battleId}) .setLabel(‘Leave’)
.setStyle(ButtonStyle.Danger); // Info button remains available to
toggle description const infoBtn2 = new ButtonBuilder()
.setCustomId(cb:info:${parentId}:${battleId}:show) .setLabel(‘Info’)
.setStyle(ButtonStyle.Secondary); const backBtn2 = new ButtonBuilder()
.setCustomId(cb:list:${parentId}) .setLabel(‘Back’)
.setStyle(ButtonStyle.Secondary); const row = new
ActionRowBuilder().addComponents(leaveBtn, infoBtn2, backBtn2); await
safeDefer(interaction, { intent: ‘update’ }); return
safeReply(interaction, { embeds: [embed], components: [row] }); } else
if (action === ‘leave’) { const battleId = param; // Ensure user is
linked and in a clan if (!cache.uid) { return safeReply(interaction, {
content: ‘Link your KC account with /link first to leave battles.’,
ephemeral: true }); } if (!cache.userClanId) { return
safeReply(interaction, { content: ‘You must be in a clan to leave this
battle.’, ephemeral: true }); } // Find battle reference in cache let
battleRef = null; for (const k of Object.keys(cache.lists)) { const arr
= cache.lists[k]; for (let i = 0; i < arr.length; i++) { if (arr[i][0]
=== battleId) { battleRef = arr[i][1]; break; } } if (battleRef) break;
} if (!battleRef) { return safeReply(interaction, { content: ‘Battle not
found. It may have expired.’, ephemeral: true }); } const joined =
battleRef.participants && battleRef.participants[cache.uid]; if
(!joined) { return safeReply(interaction, { content: ‘You are not
currently joined in this battle.’, ephemeral: true }); } // Remove from
DB try { await withTimeout(
rtdb.ref(battles/${battleId}/participants/${cache.uid}).remove(), 8000,
leave battle ${battleId} ); } catch (err) { console.error(‘[leave
battle]’, err); return safeReply(interaction, { content: ‘Failed to
leave the battle. Please try again later.’, ephemeral: true }); } //
Update local cache for (const k of Object.keys(cache.lists)) { const arr
= cache.lists[k]; for (let i = 0; i < arr.length; i++) { if (arr[i][0]
=== battleId) { if (arr[i][1].participants) { delete
arr[i][1].participants[cache.uid]; } } } } // Build updated detail view
const embed = buildBattleDetailEmbed(battleId, battleRef,
cache.clansData, cache.usersData, / includeDesc */ false); // Determine
new join/leave button let btn; const canJoinAgain = ( battleRef.status
!== ‘finished’ && cache.uid && cache.userClanId &&
(battleRef.challengerId === cache.userClanId || battleRef.targetId ===
cache.userClanId) && !(battleRef.participants &&
battleRef.participants[cache.uid]) ); if (canJoinAgain) { btn = new
ButtonBuilder() .setCustomId(cb:join:${parentId}:${battleId})
.setLabel(‘Join’) .setStyle(ButtonStyle.Success); } else { btn = new
ButtonBuilder() .setCustomId(‘cb:join:disabled’) .setLabel(‘Join’)
.setStyle(ButtonStyle.Secondary) .setDisabled(true); } const infoBtn2 =
new ButtonBuilder() .setCustomId(cb:info:${parentId}:${battleId}:show)
.setLabel(‘Info’) .setStyle(ButtonStyle.Secondary); const backBtn2 = new
ButtonBuilder() .setCustomId(cb:list:${parentId}) .setLabel(‘Back’)
.setStyle(ButtonStyle.Secondary); const row2 = new
ActionRowBuilder().addComponents(btn, infoBtn2, backBtn2); await
safeDefer(interaction, { intent: ‘update’ }); return
safeReply(interaction, { embeds: [embed], components: [row2] }); } else
if (action === ‘info’) { // Toggle additional description view for a
battle // Expected customId format: cb:info::: const battleId = param;
const mode = parts[4] || ‘show’; // Ensure we still have cache and
battle reference const battleLists = cache.lists; let battleRef = null;
for (const k of Object.keys(battleLists)) { for (const entry of
battleLists[k]) { if (entry[0] === battleId) { battleRef = entry[1];
break; } } if (battleRef) break; } if (!battleRef) { return
safeReply(interaction, { content: ‘Battle not found. It may have
expired.’, ephemeral: true }); } // Determine whether to show extended
description const includeDesc = mode === ‘show’; // Build embed with or
without description const embed = buildBattleDetailEmbed(battleId,
battleRef, cache.clansData, cache.usersData, includeDesc); // Determine
join eligibility (unchanged) let joinBtn; const canJoin = (
battleRef.status !== ‘finished’ && cache.uid && cache.userClanId &&
(battleRef.challengerId === cache.userClanId || battleRef.targetId ===
cache.userClanId) && !(battleRef.participants &&
battleRef.participants[cache.uid]) ); if (canJoin) { joinBtn = new
ButtonBuilder() .setCustomId(cb:join:${parentId}:${battleId})
.setLabel(‘Join’) .setStyle(ButtonStyle.Success); } else { // Already
joined or not eligible const joined = battleRef.participants &&
battleRef.participants[cache.uid]; const label = joined ? ‘Joined’ :
‘Join’; joinBtn = new ButtonBuilder() .setCustomId(‘cb:join:disabled’)
.setLabel(label) .setStyle(ButtonStyle.Secondary) .setDisabled(true); }
// Toggle Info button const nextMode = includeDesc ? ‘hide’ : ‘show’;
const infoLabel = includeDesc ? ‘Hide Info’ : ‘Info’; const infoBtn =
new ButtonBuilder()
.setCustomId(cb:info:${parentId}:${battleId}:${nextMode})
.setLabel(infoLabel) .setStyle(ButtonStyle.Secondary); // Back button to
list const backBtn3 = new ButtonBuilder()
.setCustomId(cb:list:${parentId}) .setLabel(‘Back’)
.setStyle(ButtonStyle.Secondary); const row = new
ActionRowBuilder().addComponents(joinBtn, infoBtn, backBtn3); await
safeDefer(interaction, { intent: ‘update’ }); return
safeReply(interaction, { embeds: [embed], components: [row] }); } //
Unknown cb action falls through } // Handle clan challenge
accept/decline buttons (cc: prefix) else if (id.startsWith(‘cc:’)) {
const parts = id.split(‘:’); // cc::: const action = parts[1]; const
parentId = parts[2]; const battleId = parts[3]; // Restrict to the user
who invoked the command const invokerId =
interaction.message?.interaction?.user?.id; if (invokerId && invokerId
!== interaction.user.id) { return safeReply(interaction, { content:
‘Only the person who ran this command can use these controls.’,
ephemeral: true }); } // Retrieve the cached list const cache =
interaction.client.challengeCache?.get(parentId); if (!cache) { return
safeReply(interaction, { content: ‘This challenge list has expired. Run
/incomingchallenges again.’, ephemeral: true }); } // Ensure this battle
is still pending const idx = cache.pendingList.findIndex(([bid]) => bid
=== battleId); if (idx < 0) { return safeReply(interaction, { content:
‘Challenge not found. It may have been updated.’, ephemeral: true }); }
const [bid, battle] = cache.pendingList[idx]; // Confirm the user is
still the owner of their clan let uid; try { uid = await
getKCUidForDiscord(interaction.user.id); } catch (_) { uid = null; } if
(!uid) { return safeReply(interaction, { content: ‘Link your KC account
first with /link.’, ephemeral: true }); } // Reload clan to check owner
const clanSnap = await rtdb.ref(clans/${cache.clanId}).get(); const clan
= clanSnap.exists() ? clanSnap.val() || {} : {}; if (getOwnerUid(clan)
!== uid) { return safeReply(interaction, { content: ‘You must be a Clan
Owner to run this command!’, ephemeral: true }); } // Update battle
status try { if (action === ‘accept’) { await
rtdb.ref(battles/${battleId}).update({ status: ‘accepted’, acceptedBy:
uid, acceptedAt: admin.database.ServerValue.TIMESTAMP }); } else if
(action === ‘decline’) { await rtdb.ref(battles/${battleId}).update({
status: ‘declined’, declinedBy: uid, declinedAt:
admin.database.ServerValue.TIMESTAMP }); } else { return
safeReply(interaction, { content: ‘Unknown action.’, ephemeral: true });
} } catch (e) { console.error(‘[cc action] failed to update battle:’,
e); return safeReply(interaction, { content: ‘Failed to update the
challenge. Try again later.’, ephemeral: true }); } // Remove from cache
list cache.pendingList.splice(idx, 1); // If no more pending, clear
message if (cache.pendingList.length === 0) {
interaction.client.challengeCache.delete(parentId); await
safeDefer(interaction, { intent: ‘update’ }); return
safeReply(interaction, { content: ‘All challenges processed.’, embeds:
[], components: [] }); } // Otherwise, rebuild embed and rows const
clansSnap = await rtdb.ref(‘clans’).get(); const clansData =
clansSnap.exists() ? clansSnap.val() || {} : {}; const embed = new
EmbedBuilder() .setTitle(‘Incoming Clan Challenges’)
.setDescription(‘Below are the pending clan battle challenges. Use the
buttons to accept or decline.’) .setColor(DEFAULT_EMBED_COLOR)
.setFooter({ text: ‘KC Bot • /incomingchallenges’ });
cache.pendingList.slice(0, 10).forEach(([bid2, b2], idx2) => { const c1
= clansData[b2.challengerId] || {}; const c2 = clansData[b2.targetId] ||
{}; const d = b2.scheduledTime ? new Date(b2.scheduledTime) : null;
const dateStr = d ? d.toLocaleDateString(‘en-GB’, { timeZone:
‘Europe/London’ }) : ‘N/A’; const timeStr = d ?
d.toLocaleTimeString(‘en-GB’, { timeZone: ‘Europe/London’, hour:
‘2-digit’, minute: ‘2-digit’ }) : ’‘; const title =
${c1.name || b2.challengerId} vs ${c2.name || b2.targetId}; const lines
= []; if (b2.server) lines.push(Server: ${b2.server}); if
(b2.scheduledTime) lines.push(Date: ${dateStr} ${timeStr}); if
(b2.rules) lines.push(Rules: ${b2.rules}); embed.addFields({ name:
${idx2 + 1}. ${title}, value: lines.join(’‘) ||’00b’ }); }); const rows
= []; cache.pendingList.slice(0, 10).forEach(([bid2], idx2) => { const
row = new ActionRowBuilder().addComponents( new
ButtonBuilder().setCustomId(cc:accept:${parentId}:${bid2}).setLabel(‘Accept’).setStyle(ButtonStyle.Success),
new
ButtonBuilder().setCustomId(cc:decline:${parentId}:${bid2}).setLabel(‘Decline’).setStyle(ButtonStyle.Danger),
); rows.push(row); }); // Update the message await
safeDefer(interaction, { intent: ‘update’ }); return
safeReply(interaction, { embeds: [embed], components: rows }); }

      // Handle battle announcement buttons (battle: prefix)
      else if (id.startsWith('battle:')) {
        const parts = id.split(':');
        // battle:<action>:<battleId>[:<mode>]
        const action = parts[1];
        const battleId = parts[2];
        const mode = parts[3] || 'show';
        // Load battle data
        const bSnap = await rtdb.ref(`battles/${battleId}`).get();
        if (!bSnap.exists()) {
          return safeReply(interaction, { content: 'Battle not found. It may have expired.', ephemeral: true });
        }
        const battle = bSnap.val() || {};
        // Load clans and users
        const [clansSnap2, usersSnap2] = await Promise.all([
          rtdb.ref('clans').get(),
          rtdb.ref('users').get(),
        ]);
        const clansData2 = clansSnap2.exists() ? clansSnap2.val() || {} : {};
        const usersData2 = usersSnap2.exists() ? usersSnap2.val() || {} : {};
        let uid2 = null;
        try { uid2 = await getKCUidForDiscord(interaction.user.id); } catch (_) {}
        // Process join and leave actions
        if (action === 'join') {
          if (!uid2) {
            return safeReply(interaction, { content: 'Link your KC account with /link first to join battles.', ephemeral: true });
          }
          // Determine the user's clan
          let userClanId = null;
          for (const [cid, c] of Object.entries(clansData2)) {
            if (c.members && c.members[uid2]) {
              userClanId = cid;
              break;
            }
          }
          if (!userClanId) {
            return safeReply(interaction, { content: 'You must be in a clan to join this battle.', ephemeral: true });
          }
          if (!(battle.challengerId === userClanId || battle.targetId === userClanId)) {
            return safeReply(interaction, { content: 'You are not a member of either participating clan.', ephemeral: true });
          }
          if (battle.status === 'finished') {
            return safeReply(interaction, { content: 'This battle has already finished.', ephemeral: true });
          }
          if (battle.participants && battle.participants[uid2]) {
            return safeReply(interaction, { content: 'You have already joined this battle.', ephemeral: true });
          }
          // Write to DB
          try {
            await rtdb.ref(`battles/${battleId}/participants/${uid2}`).set(userClanId);
            battle.participants = battle.participants || {};
            battle.participants[uid2] = userClanId;
          } catch (e) {
            console.error('[battle join] failed:', e);
            return safeReply(interaction, { content: 'Failed to join the battle. Try again later.', ephemeral: true });
          }
        }
        else if (action === 'leave') {
          // Remove the current user from participants if eligible
          if (!uid2) {
            return safeReply(interaction, { content: 'Link your KC account with /link first to leave battles.', ephemeral: true });
          }
          // Determine the user's clan
          let userClanId2 = null;
          for (const [cid, c] of Object.entries(clansData2)) {
            if (c.members && c.members[uid2]) { userClanId2 = cid; break; }
          }
          if (!userClanId2) {
            return safeReply(interaction, { content: 'You must be in a clan to leave this battle.', ephemeral: true });
          }
          if (!(battle.challengerId === userClanId2 || battle.targetId === userClanId2)) {
            return safeReply(interaction, { content: 'You are not a member of either participating clan.', ephemeral: true });
          }
          if (!battle.participants || !battle.participants[uid2]) {
            return safeReply(interaction, { content: 'You have not joined this battle.', ephemeral: true });
          }
          // Remove from DB and update local state
          try {
            await rtdb.ref(`battles/${battleId}/participants/${uid2}`).remove();
            if (battle.participants) {
              delete battle.participants[uid2];
            }
          } catch (e) {
            console.error('[battle leave] failed:', e);
            return safeReply(interaction, { content: 'Failed to leave the battle. Try again later.', ephemeral: true });
          }
        }
        // Determine if description should be included (for info action)
        const includeDesc = (action === 'info' && mode === 'show');
        const embed = buildBattleDetailEmbed(battleId, battle, clansData2, usersData2, includeDesc);
        // Determine join button state for the current user (if logged in and in clan)
        let joinBtn;
        const canJoin2 = (
          battle.status !== 'finished' &&
          uid2 && (() => {
            let userClanId = null;
            for (const [cid, c] of Object.entries(clansData2)) {
              if (c.members && c.members[uid2]) { userClanId = cid; break; }
            }
            return userClanId && (battle.challengerId === userClanId || battle.targetId === userClanId) && !(battle.participants && battle.participants[uid2]);
          })()
        );
        if (canJoin2) {
          joinBtn = new ButtonBuilder().setCustomId(`battle:join:${battleId}`).setLabel('Join').setStyle(ButtonStyle.Success);
        } else {
          // If user is logged in and already joined, offer Leave button; else disable Join
          if (uid2 && battle.participants && battle.participants[uid2]) {
            joinBtn = new ButtonBuilder().setCustomId(`battle:leave:${battleId}`).setLabel('Leave').setStyle(ButtonStyle.Danger);
          } else {
            joinBtn = new ButtonBuilder().setCustomId('battle:join:disabled').setLabel('Join').setStyle(ButtonStyle.Secondary).setDisabled(true);
          }
        }
        // Info button toggles show/hide description
        const nextMode2 = includeDesc ? 'hide' : 'show';
        const infoLabel2 = includeDesc ? 'Hide Info' : 'Info';
        const infoBtn2 = new ButtonBuilder().setCustomId(`battle:info:${battleId}:${nextMode2}`).setLabel(infoLabel2).setStyle(ButtonStyle.Secondary);
        const row2 = new ActionRowBuilder().addComponents(joinBtn, infoBtn, backBtn3);
        await safeDefer(interaction, { intent: 'update' });
        return safeReply(interaction, { embeds: [embed], components: [row2] });
      }
      // For buttons opening modals, we must reply/showModal, not defer.
      if (id.startsWith('msg:reply')) {
         const path = decPath(id.split(':')[2]);
         const modal = new ModalBuilder()
           .setCustomId(`msg:replyModal:${encPath(path)}`)
           .setTitle('Reply to message');
         const input = new TextInputBuilder()
           .setCustomId('replyText')
           .setLabel('Your reply')
           .setStyle(TextInputStyle.Paragraph)
           .setMaxLength(500);
         modal.addComponents(new ActionRowBuilder().addComponents(input));
         return interaction.showModal(modal);
      }
      if (id.startsWith('clips:comment:')) {
        const sid = id.split(':')[2];
        const payload = readModalTarget(sid);
        if (!payload) {
          return safeReply(interaction, { content: 'That action expired. Please reopen the clip.', ephemeral: true });
        }
        const modal = new ModalBuilder()
          .setCustomId(`clips:commentModal:${sid}`)
          .setTitle('Add a comment');
        const input = new TextInputBuilder()
          .setCustomId('commentText')
          .setLabel('Your comment')
          .setStyle(TextInputStyle.Paragraph)
          .setMaxLength(300)
          .setRequired(true);
        modal.addComponents(new ActionRowBuilder().addComponents(input));
        return interaction.showModal(modal);
      }

      // --- FIX (Item 2): Remove unconditional defer ---
      // We now defer *inside* branches that update the message.

      if (id.startsWith('lb:')) {
        // --- FIX (Item 2): Defer for message update ---
        await safeDefer(interaction, { intent: "update" });
        const [, , catStr, pageStr] = id.split(':');
        const catIdx = Math.max(0, Math.min(2, parseInt(catStr,10) || 0));
        const page   = Math.max(0, parseInt(pageStr,10) || 0);
        let rows = interaction.client.lbCache?.get(interaction.message.interaction?.id || '');
        if (!Array.isArray(rows)) {
          rows = await loadLeaderboardData();
          // Cache it again if it expired
          interaction.client.lbCache?.set(interaction.message.interaction?.id, rows);
        }
        const embed = buildLbEmbed(rows, catIdx, page);
        // Use safeReply, which will call .update()
        await safeReply(interaction, { embeds: [embed], components: [lbRow(catIdx, page)] });
      }
      else if (id.startsWith('c:')) { // clips list navigation
        const invokerId = interaction.message.interaction?.user?.id;
        if (invokerId && invokerId !== interaction.user.id) {
          // --- FIX (Item 2): No defer for ephemeral reply ---
          return safeReply(interaction, { content: 'Only the person who ran this command can use these controls.', ephemeral: true });
        }

        const [c, action, a, b] = id.split(':'); // c:<action>:...
        
        if (action === 'o') {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          // open detail view for idx = a
          const { state, item, idx } = getClipByIdx(interaction, parseInt(a,10));
          const postPath = clipDbPath(item);
          state.currentPostPath = postPath; // Store for 'back' button
          const embed = buildClipDetailEmbed(item, state.nameMap);
          const rows = clipsDetailRows(interaction, postPath);
          return safeReply(interaction, { embeds:[embed], components: rows });
        }
        else if (action === 'b') { // 'c:b' - back to list (deprecated by 'clips:back')
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          // back to list
          const state = getClipsState(interaction);
          state.currentPostPath = null;
          const embed = buildClipsListEmbed(state.list, state.page, state.nameMap);
          return safeReply(interaction, { embeds:[embed], components: clipsListRows(state.list.length, state.page) });
        }
        else if (action === 'p') {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          // page change
          const state = getClipsState(interaction);
          state.page = Math.max(0, parseInt(a,10) || 0);
          const embed = buildClipsListEmbed(state.list, state.page, state.nameMap);
          return safeReply(interaction, { embeds:[embed], components: clipsListRows(state.list.length, state.page) });
        }
        else if (action === 'rf') {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          // refresh list (re-fetch and re-sort)
          const state = getClipsState(interaction);
          const platform = state.platform || 'all';
          const all = await fetchAllPosts({ platform });
          all.sort((a,b)=>b.score-a.score);
          state.list = all.slice(0, CLIPS.MAX_LIST);
          state.nameMap = await getAllUserNames(); // Refresh names too
          state.page = 0; // Reset to page 0
          
          const embed = buildClipsListEmbed(state.list, state.page, state.nameMap);
          return safeReply(interaction, { embeds:[embed], components: clipsListRows(state.list.length, state.page) });
        }
      }
      else if (id.startsWith('clips:react:')) {
        // --- FIX (Item 2): No defer for ephemeral reply ---
        const shortId = id.split(':')[2];
        const payload = _readFromCache(interaction.client.reactCache, interaction.message, shortId);
        if (!payload) {
          return safeReply(interaction, { content: 'Reaction expired. Reopen the clip and try again.', ephemeral: true });
        }

        const { postPath, emoji } = payload;
        const discordId = interaction.user.id;
        const uid = await getKCUidForDiscord(discordId);
        if (!uid) {
          return safeReply(interaction, { content: 'Link your KC account with /link to react.', ephemeral: true });
        }

        const myRef = rtdb.ref(`${postPath}/reactions/${emoji}/${uid}`);
        const tx = await myRef.transaction(cur => (cur ? null : true));
        const wasReacted = !tx.snapshot.exists(); // true if we just removed it

        await rtdb.ref(`${postPath}/reactionCounts/${emoji}`).transaction(cur => (cur || 0) + (wasReacted ? -1 : 1));

        await safeReply(interaction, { content: '✅ Reaction updated.', ephemeral: true });
      }
      else if (id.startsWith('clips:reactors:')) {
        // --- FIX (Item 2): No defer for ephemeral reply ---
        const sid = id.split(':')[2];
        const payload = _readFromCache(interaction.client.reactorsCache, interaction.message, sid);
        if (!payload) {
          return safeReply(interaction, { content: 'That list expired. Reopen the clip to refresh.', ephemeral: true });
        }
        const { postPath } = payload;

        const snap = await withTimeout(rtdb.ref(`${postPath}/reactions`).get(), 6000, `RTDB ${postPath}/reactions`);
        const data = snap.exists() ? (snap.val() || {}) : {};
        const nameMap = await getAllUserNames();

        const lines = [];
        for (const emo of POST_EMOJIS) {
          const uids = Object.keys(data[emo] || {});
          if (!uids.length) continue;
          const pretty = uids.map(u => nameMap.get(u) || 'unknown').slice(0, 20).join(', ');
          const more = uids.length > 20 ? ` … +${uids.length - 20} more` : '';
          lines.push(`${emo} ${pretty}${more}`);
        }

        const embed = new EmbedBuilder()
          .setTitle('Reactors')
          .setDescription(lines.join('\n') || '_No reactions yet._')
          .setColor(DEFAULT_EMBED_COLOR)
          .setFooter({ text: 'KC Bot • /clips' });

        await safeReply(interaction, { embeds:[embed], ephemeral: true });
      }
      else if (id.startsWith('clips:comments:prev:') || id.startsWith('clips:comments:next:')) {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          const parts = id.split(':'); // ['clips','comments','prev|next', sid, page]
          const sid = parts[3];
          const page = parseInt(parts[4], 10) || 0;
          const payload = _readFromCache(interaction.client.commentsCache, interaction.message, sid);
          if (!payload) {
              return safeReply(interaction, { content: 'That comments list expired. Reopen the clip and try again.', ephemeral: true });
          }
          const { postPath } = payload;
          const state = getClipsState(interaction);
          let item = state?.list?.find(x => clipDbPath(x) === postPath);
          if (!item) {
              const snap = await rtdb.ref(postPath).get();
              item = snap.exists() ? { ownerUid: postPath.split('/')[1], postId: postPath.split('/').pop(), data: snap.val() } : null;
              if (!item) return safeReply(interaction, { content: 'Could not load clip data.' });
          }
          const nameMap = state?.nameMap || await getAllUserNames();
          const comments = await loadClipComments(postPath);
          const maxPage = Math.max(0, Math.ceil(comments.length / 10) - 1);
          const p = Math.max(0, Math.min(page, maxPage));
          const embed = buildClipCommentsEmbed(item, comments, p, nameMap);
          const rows = [commentsRows(sid, p, maxPage)];
          await safeReply(interaction, { content: '', embeds: [embed], components: rows });
      }
      else if (id.startsWith('clips:comments:')) {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          const sid = id.split(':')[2];
          const payload = _readFromCache(interaction.client.commentsCache, interaction.message, sid);
          if (!payload) {
              return safeReply(interaction, { content: 'That comments list expired. Reopen the clip and try again.', ephemeral: true });
          }
          const { postPath } = payload;
          const state = getClipsState(interaction);
          let item = state?.list?.find(x => clipDbPath(x) === postPath);
          if (!item) {
              const snap = await rtdb.ref(postPath).get();
              item = snap.exists() ? { ownerUid: postPath.split('/')[1], postId: postPath.split('/').pop(), data: snap.val() } : null;
              if (!item) return safeReply(interaction, { content: 'Could not load clip data.' });
          }
          const nameMap = state?.nameMap || await getAllUserNames();
          const comments = await loadClipComments(postPath);
          const maxPage = Math.max(0, Math.ceil(comments.length / 10) - 1);
          const page = 0;
          const embed = buildClipCommentsEmbed(item, comments, page, nameMap);
          const rows = [commentsRows(sid, page, maxPage)];
          await safeReply(interaction, { content: '', embeds: [embed], components: rows });
      }
      else if (id === 'clips:backDetail') {
        // --- FIX (Item 2): Defer for message update ---
        await safeDefer(interaction, { intent: "update" });
        // Back from comments to clip detail
        const state = getClipsState(interaction);
        if (!state || !state.currentPostPath) {
            return safeReply(interaction, { content: 'Clip data expired. Please run /clips again.' });
        }
        const item = state.list.find(x => clipDbPath(x) === state.currentPostPath);
        if (!item) {
            return safeReply(interaction, { content: 'Clip data expired. Please run /clips again.' });
        }
        const embed = buildClipDetailEmbed(item, state.nameMap);
        const rows = clipsDetailRows(interaction, state.currentPostPath);
        return safeReply(interaction, { embeds: [embed], components: rows });
      }
      else if (id === 'clips:back') {
        // --- FIX (Item 2): Defer for message update ---
        await safeDefer(interaction, { intent: "update" });
        // Back from clip detail to list
        const state = getClipsState(interaction);
        if (!state) {
            return safeReply(interaction, { content: 'Clip data expired. Please run /clips again.' });
        }
        state.currentPostPath = null;
        const embed = buildClipsListEmbed(state.list, state.page, state.nameMap);
        return safeReply(interaction, { embeds:[embed], components: clipsListRows(state.list.length, state.page) });
      }
      else if (interaction.customId.startsWith('msg:')) {
        const invokerId = interaction.message.interaction?.user?.id;
        if (invokerId && invokerId !== interaction.user.id) {
          // --- FIX (Item 2): No defer for ephemeral reply ---
          return safeReply(interaction, { content: 'Only the person who ran this command can use these controls.', ephemeral: true });
        }
        const key = interaction.message.interaction?.id || '';
        interaction.client.msgCache ??= new Map();
        let state = interaction.client.msgCache.get(key);
        if (!state) {
          const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
          state = { list, nameMap };
          interaction.client.msgCache.set(key, state);
        }
        const [ns, action, a, b] = interaction.customId.split(':');
        if (action === 'view' || action === 'openIdx') {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          const idx = Math.max(0, Math.min(parseInt(a||'0',10), (state.list.length||1)-1));
          const msg = state.list[idx];
          const fresh = await loadNode(msg.path);
          const hasReplies = !!(fresh?.replies && Object.keys(fresh.replies).length);
          // --- FIX (Item 1): Fix object spread ---
          const embed = buildMessageDetailEmbed({ ...msg, ...fresh }, state.nameMap);
          // --- END FIX ---
          await safeReply(interaction, { embeds: [embed], components: messageDetailRows(idx, state.list, msg.path, hasReplies) });
        }
        else if (action === 'back' || action === 'list') {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          const embed = buildMessagesEmbed(state.list, state.nameMap);
          await safeReply(interaction, { embeds: [embed], components: messageIndexRows(state.list.length || 0) });
        }
        else if (action === 'refresh') {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
          state.list = list;
          state.nameMap = nameMap;
          const embed = buildMessagesEmbed(list, nameMap);
          await safeReply(interaction, { embeds: [embed], components: messageIndexRows(list.length || 0) });
        }
        else if (action === 'refreshOne') {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          const idx = Math.max(0, Math.min(parseInt(a||'0',10), (state.list.length||1)-1));
          const msg = state.list[idx];
          const fresh = await loadNode(msg.path);
          const hasReplies = !!(fresh?.replies && Object.keys(fresh.replies).length);
          // --- FIX (Item 1): Fix object spread (redundant, already fixed above) ---
          const embed = buildMessageDetailEmbed({ ...msg, ...fresh }, state.nameMap);
          await safeReply(interaction, { embeds: [embed], components: messageDetailRows(idx, state.list, msg.path, hasReplies) });
        }
        else if (action === 'openPath') {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          const path = decPath(a);
          const idx = state.list.findIndex(m=>m.path===path);
          const base = idx>=0 ? state.list[idx] : await loadNode(path);
          const fresh = await loadNode(path);
          const hasReplies = !!(fresh?.replies && Object.keys(fresh.replies).length);
          // --- FIX (Item 1): Fix object spread ---
          const embed = buildMessageDetailEmbed({ ...(base || {}), ...(fresh || {}) }, state.nameMap);
          // --- END FIX ---
          await safeReply(interaction, { embeds: [embed], components: messageDetailRows(Math.max(0,idx), state.list, path, hasReplies) });
        }
        else if (action === 'thread' || action === 'threadPrev' || action === 'threadNext') {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          const path = decPath(a);
          const page = parseInt(b||'0',10) || 0;
          const parent = await loadNode(path);
          const children = await loadReplies(path);
          const embed = buildThreadEmbed(parent, children, page, 10, state.nameMap);
          await safeReply(interaction, { embeds: [embed], components: threadRows(path, children, page, 10) });
        }
        else if (action === 'openChild') {
          // --- FIX (Item 2): Defer for message update ---
          await safeDefer(interaction, { intent: "update" });
          const path = decPath(a);
          const node = await loadNode(path);
          const hasReplies = !!(node?.replies && Object.keys(node.replies).length);
          const embed = buildMessageDetailEmbed(node, state.nameMap);
          const rows = [
            new ActionRowBuilder().addComponents(
              new ButtonBuilder().setCustomId(`msg:thread:${encPath(path)}:0`).setLabel('Open thread').setStyle(ButtonStyle.Secondary).setDisabled(!hasReplies),
              new ButtonBuilder().setCustomId(`msg:openPath:${encPath(path.split('/replies/').slice(0,-1).join('/replies/'))}`).setLabel('Up one level').setStyle(ButtonStyle.Secondary),
              new ButtonBuilder().setCustomId('msg:back').setLabel('Back to list').setStyle(ButtonStyle.Secondary),
            ),
            new ActionRowBuilder().addComponents(
              new ButtonBuilder().setCustomId(`msg:like:${encPath(path)}`).setLabel('❤️ Like/Unlike').setStyle(ButtonStyle.Primary),
              new ButtonBuilder().setCustomId(`msg:reply:${encPath(path)}`).setLabel('↩️ Reply').setStyle(ButtonStyle.Primary),
            ),
          ];
          await safeReply(interaction, { embeds: [embed], components: rows });
        }
        else if (action === 'like') {
          // --- FIX (Item 2): No defer for ephemeral reply ---
          const discordId = interaction.user.id;
          const uid = await getKCUidForDiscord(discordId);
          if (!uid) return await safeReply(interaction, { content: 'Link your KC account first with /link.', ephemeral: true });
          const path = decPath(a);
          const likedSnap = await withTimeout(rtdb.ref(`${path}/likedBy/${uid}`).get(), 6000, `RTDB ${path}/likedBy`);
          const wasLiked = likedSnap.exists();
          await rtdb.ref(`${path}/likedBy/${uid}`).transaction(cur => cur ? null : true);
          await rtdb.ref(`${path}/likes`).transaction(cur => (cur||0) + (wasLiked ? -1 : 1));
          
          // We must acknowledge, but we don't want to update the embed
          // as it's disruptive. Send an ephemeral ack.
          await safeReply(interaction, { content: '✅ Like updated.', ephemeral: true });

          // --- Optional: update the message embed *after* acknowledging ---
          // This is "fire and forget" - we don't await it, and if it fails,
          // the user has already been notified of success.
          (async () => {
            try {
              const node = await loadNode(path);
              const embed = buildMessageDetailEmbed(node, state?.nameMap || new Map());
              const i = state.list.findIndex(m=>m.path===path);
              if (i>=0) {
                 // --- FIX (Item 1): Fix object spread ---
                 state.list[i] = { ...state.list[i], ...node };
                 // --- END FIX ---
              }
              // We already replied, so this must be .editReply on the *original* message
              await interaction.message.edit({ embeds: [embed] });
            } catch (e) {
              console.warn(`[msg:like] failed to update embed post-ack:`, e.message);
            }
          })();
        }
      }
      else if (interaction.customId === 'votes:refresh') {
        // --- FIX (Item 2): Defer for message update ---
        await safeDefer(interaction, { intent: "update" });
        const scores = await loadVoteScores();
        const embed = buildVoteEmbed(scores);
        const row = new ActionRowBuilder().addComponents(
          new ButtonBuilder().setCustomId('votes:refresh').setLabel('Refresh').setStyle(ButtonStyle.Primary)
        );
        await safeReply(interaction, { embeds: [embed], components: [row] });
      }
      else if (interaction.customId.startsWith('vote:delete:')) {
        // --- FIX (Item 2): Defer for message update ---
        await safeDefer(interaction, { intent: "update" });
        const uid = interaction.customId.split(':')[2];
        await withTimeout(rtdb.ref(`votes/${uid}`).remove(), 6000, `delete vote ${uid}`);
        await safeReply(interaction, { content: '🗑️ Your vote was deleted. Run `/vote` to submit a new one.', components: [], embeds: [] });
      }
    } catch (err) {
      console.error(`[button:${id}]`, err);
      const msg = '❌ Sorry, something went wrong.';
      // We must use safeReply to handle the state (deferred/not)
      await safeReply(interaction, { content: msg, ephemeral: true, embeds: [], components: [] });
    }

} // — Select Menu Handlers — else if (interaction.isStringSelectMenu())
{ const [prefix, parentInteractionId] = interaction.customId.split(‘:’);
// Handle Battledome server filter selection before other menus. Custom
ID // format: bd:server_filter_select:. This allows users to //
subscribe/unsubscribe from specific Battledome regions. if
(interaction.customId.startsWith(‘bd:server_filter_select’)) { try {
const parts = interaction.customId.split(‘:’); const guildId = parts[2]
|| interaction.guildId; const userId = interaction.user.id; const
selected = interaction.values || []; const updates = {}; for (const
regionKey of BD_REGIONS) { const path =
bdNotify/${guildId}/${userId}/regions/${regionKey}; if
(selected.includes(regionKey)) { updates[path + ‘/enabled’] = true;
updates[path + ‘/mode’] = ‘join’; } else { updates[path] = null; } }
await rtdb.ref().update(updates); await
rtdb.ref(bdNotify/${guildId}/${userId}/updatedAt).set(admin.database.ServerValue.TIMESTAMP);
const serverList = selected.length ? selected.join(‘,’) : ‘None’; return
safeReply(interaction, { content:
✅ Your Battledome servers updated to: ${serverList}, embeds: [],
components: [], ephemeral: true }); } catch (err) { console.error(‘[bd
server filter select]’, err); return safeReply(interaction, { content:
‘❌ Failed to update your servers.’, embeds: [], components: [],
ephemeral: true }); } }

    if (prefix === 'clans_select') {
      try {
        const clanId = interaction.values[0];
        const cache = interaction.client.clanCache?.get(parentInteractionId);
        if (!cache) {
          // No defer yet; reply ephemeral
          return safeReply(interaction, { 
              content: 'Clan data has expired. Run `/clans` again.',
              ephemeral: true
          });
        }

        // We know we’ll update the existing menu message
        await safeDefer(interaction, { intent: 'update' });
      
        const { entries, usersData, badgesData } = cache;
        const clan = entries.find(c => c.id === clanId);
        if (!clan) {
          return safeReply(interaction, { content: 'Clan not found.', embeds: [], components: [] });
        }
      
        // build member display names
        let memberNames = '';
        if (clan.members) {
          memberNames = Object.keys(clan.members)
            .map(uid => usersData[uid]?.displayName || uid)
            .join(', ');
          if (memberNames.length > 1024) {
            memberNames = memberNames.slice(0, 1020) + '…';
          }
        }
      
        const owner = usersData[clan.owner] || {};
      
        // recompute score in case it is needed
        const score = computeClanScore(clan, usersData, badgesData);
      
        const detailEmbed = new EmbedBuilder()
          .setTitle(`${clan.name}${clan.letterTag ? ` [${clan.letterTag}]` : ''}`)
          .setDescription(clan.description || 'No description provided.')
          .addFields(
            { name: 'Owner', value: owner.displayName || 'Unknown', inline: false },
            { name: 'Points', value: `${score}`, inline: true },
            { name: 'Members', value: memberNames || 'No members', inline: false },
          )
          .setFooter({ text: `Region: ${clan.region || 'N/A'}` })
          .setColor(DEFAULT_EMBED_COLOR);

        // Guard missing/invalid icon URL on the embed
        if (clan.icon && /^https?:\/\//i.test(clan.icon)) {
          detailEmbed.setThumbnail(clan.icon);
        }
      
        await safeReply(interaction, {
          content: '',
          embeds: [detailEmbed],
          components: [], // remove the select menu once a clan is chosen
        });

        // Delete the cache after selection
        interaction.client.clanCache?.delete(parentInteractionId);

      } catch (err) {
          console.error(`[select:${prefix}]`, err);
          await safeReply(interaction, { 
              content: '❌ Sorry, something went wrong.', 
              ephemeral: true, embeds: [], components: [] 
          });
      }
    } else if (prefix === 'bd_select') {
      try {
          // MUST defer update so Discord doesn’t time out
          await safeDefer(interaction, { intent: 'update' });

          const cache = interaction.client.bdCache?.get(parentInteractionId);
          if (!cache) {
            return safeReply(interaction, { content: 'This Battledome menu expired. Run `/battledome` again.', ephemeral: true });
          }

          const idx = parseInt(interaction.values?.[0] || '', 10);
          const s = cache.servers?.[idx];
          if (!s) {
            return safeReply(interaction, { content: 'Invalid selection.', ephemeral: true });
          }
      
          // Resolve region URL from authoritative map
          // Use s.region if available, otherwise map by name, fallback to west
          const region = s.region || BD_NAME_OVERRIDES[s.name] || "West";
          const url = BD_SERVERS[region]?.url || s.url;

          if (!url) {
            return safeReply(interaction, { content: `No URL configured for region: ${region}`, ephemeral: true });
          }
      
          let info;
          let fromCache = false;
          let fetchedAt = 0;

          try {
            const r = await getBdInfoCached(url);
            info = r?.data;
            fromCache = !!r?.fromCache;
            fetchedAt = r?.fetchedAt || 0;
          } catch (e) {
            // quietly ignore errors; we'll fall back to warming message
          }

          if (!info) {
              // Return warming up embed
              const warmEmbed = new EmbedBuilder()
                  .setTitle(`Battledome — ${s.name || 'Server'}`)
                  .addFields(
                      { name: 'Online now', value: '—', inline: true },
                      { name: 'In dome now', value: '—', inline: true },
                      { name: 'Players this hour', value: '—', inline: true },
                      { name: 'Players (0)', value: '_No snapshot yet—warming cache. Try again in a few seconds._' }
                  )
                  .setColor(DEFAULT_EMBED_COLOR)
                  .setFooter({ text: 'Cache warming up' });
              return safeReply(interaction, { embeds: [warmEmbed], components: interaction.message.components });
          }

          const players = Array.isArray(info?.players) ? info.players : [];
          const lines = players.slice(0, 40).map(p => {
            const inDome = (p.indomenow === 'Yes') ? ' 🏟️' : '';
            const rank = p.rank != null ? `#${p.rank}` : '#?';
            const score = p.score != null ? ` (${p.score})` : '';
            const name = p.name || '(unknown)';
            const idle = p.inactive > 60 ? ' *(idle)*' : '';
            return `${rank} **${name}**${score}${inDome}${idle}`;
          });

          // Compute cache note for footer
          const ageSec = fetchedAt ? Math.floor((Date.now() - fetchedAt) / 1000) : 0;
          const cacheNote = fromCache ? `⚠️ Cached (${ageSec}s ago)` : `✅ Live (${ageSec}s ago)`;

          const embed = new EmbedBuilder()
            .setTitle(info?.name ? `Battledome — ${info.name}` : `Battledome — ${s.name || 'Server'}`)
            .addFields(
              { name: 'Online now', value: String(info?.onlinenow ?? '—'), inline: true },
              { name: 'In dome now', value: String(info?.indomenow ?? '—'), inline: true },
              { name: 'Players this hour', value: String(info?.thishour ?? '—'), inline: true },
              { name: `Players (${players.length})`, value: lines.length ? lines.join('\n') : '_No players listed._' }
            )
            .setColor(DEFAULT_EMBED_COLOR)
            .setFooter({ text: `KC Bot • /battledome • ${cacheNote}` });

          return safeReply(interaction, { embeds: [embed], components: interaction.message.components });
      } catch (err) {
        console.error(`[select:${prefix}]`, err);
        await safeReply(interaction, { 
            content: '❌ Sorry, something went wrong.', 
            ephemeral: true, embeds: [], components: [] 
        });
      }
     }

} });

async function checkRegion(regionKey) { const config =
BD_SERVERS[regionKey]; if (!config) return;

// FIX: Use cached fetcher, do not throw const r = await
getBdInfoCached(config.url).catch(() => null); const info = r?.data;

// If no data (start up fail), just return if (!info ||
!Array.isArray(info.players)) return;

// Update the last info snapshot for summary embeds. This allows us to
// construct consolidated status messages across all regions. Store the
// fetchedAt time as well so we can display relative age in footers.
bdLastInfo[regionKey] = info; bdLastFetch[regionKey] = r?.fetchedAt ||
Date.now();

// Update top scores updateBdTop(regionKey, info.players);

// Optional: Announce fresh cache if (r && !r.fromCache && !r.stale) {
// Don’t await to avoid blocking poller
announceBdCacheUpdated(regionKey, r.fetchedAt).catch(() => {}); }

const state = bdState[regionKey]; const currentNames = new
Set(info.players.map(p => p.name)); const currentOnline =
info.onlinenow; // Track current in‑dome player count for threshold
logic const currentIndome = info.indomenow || 0;

// First run: just init state if (state.lastCheck === 0) {
state.lastNames = currentNames; state.lastOnline = currentOnline;
state.lastIndome = currentIndome; state.lastCheck = Date.now(); return;
}

// Diff const joins = []; const leaves = [];

for (const name of currentNames) { if (!state.lastNames.has(name))
joins.push(name); } for (const name of state.lastNames) { if
(!currentNames.has(name)) leaves.push(name); }

// Unnamed calc // Effective count of named players const namedCountPrev
= state.lastNames.size; const namedCountCurr = currentNames.size;

const unnamedPrev = Math.max(0, state.lastOnline - namedCountPrev);
const unnamedCurr = Math.max(0, currentOnline - namedCountCurr);

let unnamedDiff = unnamedCurr - unnamedPrev;

// Update state immediately state.lastNames = currentNames;
state.lastOnline = currentOnline; state.lastIndome = currentIndome;
state.lastCheck = Date.now();

if (joins.length === 0 && leaves.length === 0 && unnamedDiff === 0)
return;

// Broadcast await broadcastBdUpdate(regionKey, { joins, leaves,
unnamedDiff, info });

// Record recent join/leave events. We do this after the broadcast call
so // notifications reflect the current event. We push each player name
along // with a timestamp. Prune old entries first to keep memory
bounded. pruneBdRecent(); const now = Date.now(); for (const name of
joins) { bdRecent[regionKey].push({ name, time: now, type: ‘join’ }); }
for (const name of leaves) { bdRecent[regionKey].push({ name, time: now,
type: ‘leave’ }); }

// If there were any joins or leaves, also post a summary block to each
// configured join log channel. This emits green and red embeds
summarising // the counts and names. It iterates through all guilds that
have // configured join logs and posts to each. Errors are ignored per
guild. if (joins.length > 0 || leaves.length > 0) { for (const [guildId]
of globalCache.bdJoinLogs.entries()) { try { await
postJoinLogsBlock(guildId, regionKey, { joins, leaves }); } catch (e) {
// silent } } } }

async function broadcastBdUpdate(regionKey, { joins, leaves,
unnamedDiff, info }) { const serverName = BD_SERVERS[regionKey]?.name ||
regionKey; const onlineNow = info.onlinenow || 0; const inDomeNow =
info.indomenow || 0; // Use total players online as the metric for
threshold alerts. This // eliminates confusion between “online” vs
“in-dome” players and matches // the updated UI wording. const
playerCountMetric = onlineNow;

// Iterate through configured guilds to determine which users need to be
alerted. for (const [guildId, _channelId] of
globalCache.bdDestinations.entries()) { try { // Skip if alerts are
globally disabled for this guild let alertsEnabled = true; try { const
enabledSnap = await rtdb.ref(config/bdAlertsEnabled/${guildId}).get();
if (enabledSnap.exists() && enabledSnap.val() === false) alertsEnabled =
false; } catch {} if (!alertsEnabled) continue;

      const subSnap = await rtdb.ref(`bdNotify/${guildId}`).get();
      if (!subSnap.exists()) continue;

      const stateUpdates = {};
      const triggered = [];
      subSnap.forEach(child => {
        const userId = child.key;
        const val = child.val() || {};
        const sub = val.regions?.[regionKey];
        if (!sub || !sub.enabled) return;
        const threshold = sub.threshold;
        const mode = sub.mode || (typeof threshold === 'number' ? 'active' : 'join');
        const prevState = sub.state || 'below';
        if (mode === 'active' && typeof threshold === 'number') {
          // Compare against the online player count
          if (playerCountMetric >= threshold) {
            if (prevState !== 'above') {
              triggered.push({ userId, mode: 'active', threshold });
              stateUpdates[`bdNotify/${guildId}/${userId}/regions/${regionKey}/state`] = 'above';
            }
          } else {
            if (prevState !== 'below') {
              stateUpdates[`bdNotify/${guildId}/${userId}/regions/${regionKey}/state`] = 'below';
            }
          }
        } else {
          // join mode: trigger on any new named joins
          if (joins.length > 0) {
            triggered.push({ userId, mode: 'join' });
          }
        }
      });
      if (Object.keys(stateUpdates).length > 0) {
        rtdb.ref().update(stateUpdates).catch(e => console.error('State update failed', e));
      }
      // Notify each triggered user. Apply DM preferences and join logs rules.
      for (const { userId, mode, threshold } of triggered) {
        // Skip join‑mode alerts for DM/logging. Joins are covered by join logs blocks.
        if (mode !== 'active') {
          continue;
        }
        // Build message for threshold alerts
        let message;
        if (typeof threshold === 'number') {
          // Compose a more user‑friendly threshold message using online players
          message = `✅ ${serverName} Battledome has reached your player count (online) of **${threshold}** (now ${playerCountMetric}).`;
        } else {
          // Should not happen for active mode, but fallback
          message = `✅ ${serverName} Battledome activity notification.`;
        }
        // Determine DM preference for this user in this guild
        let dmEnabledPref = false;
        try {
          const snap = await rtdb.ref(`config/bdAlertPrefs/${guildId}/${userId}/dmEnabled`).get();
          if (snap.exists() && snap.val() === true) dmEnabledPref = true;
        } catch {}
        // Determine if a join logs channel is configured
        const joinLogsId = globalCache.bdJoinLogs?.get(guildId);
        // Delivery logic: if DM enabled, DM. Else if join logs exists, mention. Else silent.
        if (dmEnabledPref === true) {
          // DM the user only
          try {
            const user = await client.users.fetch(userId).catch(() => null);
            if (user) {
              await user.send({ content: message });
            }
          } catch {}
        } else {
          // DM disabled
          if (joinLogsId) {
            try {
              const chan = await client.channels.fetch(joinLogsId).catch(() => null);
              if (chan && chan.isTextBased?.()) {
                await chan.send({ content: `<@${userId}> ${message}` });
              }
            } catch {}
          }
          // If no join logs channel, remain silent
        }
      }
    } catch (err) {
      console.error('[BD Broadcast] Error while processing guild', guildId, err);
    }

}

// After notifying subscribers, update or send the consolidated status
message // across all guilds. This replaces sending per-update status
embeds and // keeps channels tidy. Errors are logged inside
updateBdStatusMessages(). updateBdStatusMessages().catch(() => {}); }

// Polling Loop // Polling loop for Battledome status. West region has
been retired, so only // active regions are polled. Runs every 15
seconds and fetches all regions in // parallel, then updates the status
dashboard regardless of diffs. function pollBattledome() { // Use
setInterval instead of an infinite while loop. The returned timer //
handle is intentionally not stored since the bot runs indefinitely.
setInterval(async () => { try { // Fetch all regions in parallel. If one
region fails, others still resolve. await Promise.all(BD_REGIONS.map(r
=> checkRegion(r).catch(() => {}))); // Always update the status
dashboard even when nothing changed. The // update function includes its
own mutex to prevent concurrent runs. await updateBdStatusMessages(); }
catch (e) { console.error(‘[BD Poller]’, e); } }, 15000).unref?.(); }

// ———- Startup ———- client.once(‘ready’, async () => {
console.log(Logged in as ${client.user.tag}); clientReady = true;

// Load clip destinations try { const snap = await
rtdb.ref(‘config/clipDestinations’).get();
globalCache.clipDestinations.clear(); if (snap.exists()) { const all =
snap.val() || {}; for (const [guildId, cfg] of Object.entries(all)) { if
(cfg?.channelId) globalCache.clipDestinations.set(guildId,
cfg.channelId); } }
console.log([broadcast] loaded initial clip destinations: ${globalCache.clipDestinations.size});

    rtdb.ref('config/clipDestinations').on('child_added', s => {
      const v = s.val() || {};
      if (v.channelId) globalCache.clipDestinations.set(s.key, v.channelId);
    });
    rtdb.ref('config/clipDestinations').on('child_changed', s => {
      const v = s.val() || {};
      if (v.channelId) globalCache.clipDestinations.set(s.key, v.channelId);
      else globalCache.clipDestinations.delete(s.key);
    });
    rtdb.ref('config/clipDestinations').on('child_removed', s => {
      globalCache.clipDestinations.delete(s.key);
    });

} catch (e) { console.error(‘[broadcast] failed to load clip
destinations’, e); }

// Load Battledome destinations (NEW) try { const snap = await
rtdb.ref(‘config/bdDestinations’).get();
globalCache.bdDestinations.clear(); if (snap.exists()) { const all =
snap.val() || {}; for (const [guildId, cfg] of Object.entries(all)) { if
(cfg?.channelId) globalCache.bdDestinations.set(guildId, cfg.channelId);
} }
console.log([BD] loaded initial destinations: ${globalCache.bdDestinations.size});

    rtdb.ref('config/bdDestinations').on('child_added', s => {
      const v = s.val() || {};
      if (v.channelId) globalCache.bdDestinations.set(s.key, v.channelId);
    });
    rtdb.ref('config/bdDestinations').on('child_changed', s => {
      const v = s.val() || {};
      if (v.channelId) globalCache.bdDestinations.set(s.key, v.channelId);
      else globalCache.bdDestinations.delete(s.key);
    });
    rtdb.ref('config/bdDestinations').on('child_removed', s => {
      globalCache.bdDestinations.delete(s.key);
    });

} catch (e) { console.error(‘[BD] failed to load destinations’, e); }

// Load Battledome join log channels (NEW) try { const snap = await
rtdb.ref(‘config/bdJoinLogsChannel’).get();
globalCache.bdJoinLogs.clear(); if (snap.exists()) { const all =
snap.val() || {}; for (const [guildId, cfg] of Object.entries(all)) { if
(cfg?.channelId) globalCache.bdJoinLogs.set(guildId, cfg.channelId); } }
console.log([BD] loaded initial join log channels: ${globalCache.bdJoinLogs.size});

    rtdb.ref('config/bdJoinLogsChannel').on('child_added', s => {
      const v = s.val() || {};
      if (v.channelId) globalCache.bdJoinLogs.set(s.key, v.channelId);
    });
    rtdb.ref('config/bdJoinLogsChannel').on('child_changed', s => {
      const v = s.val() || {};
      if (v.channelId) globalCache.bdJoinLogs.set(s.key, v.channelId);
      else globalCache.bdJoinLogs.delete(s.key);
    });
    rtdb.ref('config/bdJoinLogsChannel').on('child_removed', s => {
      globalCache.bdJoinLogs.delete(s.key);
    });

} catch (e) { console.error(‘[BD] failed to load join log channels’, e);
}

// Load persistent Battledome status messages (guildId -> { channelId,
messageId }) try { const snap = await
rtdb.ref(‘config/bdStatusMessage’).get();
globalCache.bdStatusMessages.clear(); if (snap.exists()) { const all =
snap.val() || {}; for (const [guildId, cfg] of Object.entries(all)) { if
(cfg && cfg.channelId) { globalCache.bdStatusMessages.set(guildId, {
channelId: cfg.channelId, messageId: cfg.messageId || null, }); } } }
console.log([BD] loaded initial status messages: ${globalCache.bdStatusMessages.size});
// Listen for updates to status messages
rtdb.ref(‘config/bdStatusMessage’).on(‘child_added’, s => { const v =
s.val() || {}; if (v && v.channelId) {
globalCache.bdStatusMessages.set(s.key, { channelId: v.channelId,
messageId: v.messageId || null, }); } });
rtdb.ref(‘config/bdStatusMessage’).on(‘child_changed’, s => { const v =
s.val() || {}; if (v && v.channelId) {
globalCache.bdStatusMessages.set(s.key, { channelId: v.channelId,
messageId: v.messageId || null, }); } });
rtdb.ref(‘config/bdStatusMessage’).on(‘child_removed’, s => {
globalCache.bdStatusMessages.delete(s.key); }); } catch (e) {
console.error(‘[BD] failed to load status messages’, e); }

// Load Battledome to KC opt‑in mappings (discordId -> { kcUid, kcName,
enabled }) try { const snap = await rtdb.ref(‘config/bdToKC’).get();
globalCache.bdToKC.clear(); globalCache.kcNameToUid.clear(); if
(snap.exists()) { const all = snap.val() || {}; for (const [discordId,
cfg] of Object.entries(all)) { if (cfg && cfg.enabled) {
globalCache.bdToKC.set(discordId, cfg); // Build reverse lookup when
enabled and name is provided if (cfg.kcName) {
globalCache.kcNameToUid.set(cfg.kcName, cfg.kcUid); } } } }
console.log([BDToKC] loaded initial mappings: ${globalCache.bdToKC.size});
// Watch for changes const bdToKCRef = rtdb.ref(‘config/bdToKC’);
bdToKCRef.on(‘child_added’, s => { const v = s.val() || {}; if (v &&
v.enabled) { globalCache.bdToKC.set(s.key, v); if (v.kcName)
globalCache.kcNameToUid.set(v.kcName, v.kcUid); } });
bdToKCRef.on(‘child_changed’, s => { const v = s.val() || {}; // Remove
any previous mapping for this discordId const prev =
globalCache.bdToKC.get(s.key); if (prev) { if (prev.kcName)
globalCache.kcNameToUid.delete(prev.kcName); } if (v && v.enabled) {
globalCache.bdToKC.set(s.key, v); if (v.kcName)
globalCache.kcNameToUid.set(v.kcName, v.kcUid); } else {
globalCache.bdToKC.delete(s.key); } }); bdToKCRef.on(‘child_removed’, s
=> { const prev = globalCache.bdToKC.get(s.key); if (prev &&
prev.kcName) { globalCache.kcNameToUid.delete(prev.kcName); }
globalCache.bdToKC.delete(s.key); }); } catch (e) {
console.error(‘[BDToKC] failed to load mappings’, e); }

// Override updateBdStatusMessages with a version that uses a mutex and
// persists message IDs in RTDB. This override runs after the //
bdStatusMessages cache has been loaded so it can safely write/modify //
records. Without this, concurrent calls or restarts can cause //
duplicate messages in the Battledome channels. updateBdStatusMessages =
async function updateBdStatusMessages() { // Return existing update if
in progress if (bdStatusUpdateInFlight) { return bdStatusUpdateInFlight;
} bdStatusUpdateInFlight = (async () => { for (const [guildId,
channelId] of globalCache.bdDestinations.entries()) { try { const
channel = await client.channels.fetch(channelId).catch(() => null); if
(!channel || !channel.isTextBased?.()) continue; // Determine advanced
view preference let showAdvanced = false; try { const snap = await
rtdb.ref(config/bdShowAdvanced/${guildId}).get(); if (snap.exists() &&
snap.val() === true) showAdvanced = true; } catch {} const embed =
buildBdStatusUnifiedEmbed({ showAdvanced }); const components =
buildBdDashboardActionRows(guildId, showAdvanced); // Get or reset
record for this guild let record =
globalCache.bdStatusMessages.get(guildId) || null; if (!record ||
record.channelId !== channelId) { record = { channelId, messageId: null
}; globalCache.bdStatusMessages.set(guildId, record); try { await
rtdb.ref(config/bdStatusMessage/${guildId}).set({ channelId: channelId,
messageId: null, updatedAt: admin.database.ServerValue.TIMESTAMP,
updatedBy: client.user?.id || ‘system’ }); } catch {} } let msgId =
record.messageId; let edited = false; if (msgId) { try { const msg =
await channel.messages.fetch(msgId); await msg.edit({ content: ’‘,
embeds: [embed], components }); edited = true; } catch {} } if (!edited)
{ const msg = await channel.send({ content:’‘, embeds: [embed],
components }); msgId = msg.id; globalCache.bdStatusMessages.set(guildId,
{ channelId, messageId: msgId }); try { await
rtdb.ref(config/bdStatusMessage/${guildId}).set({ channelId: channelId,
messageId: msgId, updatedAt: admin.database.ServerValue.TIMESTAMP,
updatedBy: client.user?.id || ’system’ }); } catch {} } } catch (e) {
console.error([BD Status] Failed to update status message for guild ${guildId}:,
e?.message || e); } } })().finally(() => { bdStatusUpdateInFlight =
null; }); return bdStatusUpdateInFlight; };

// Send initial Battledome status messages to all configured
destinations. Do this // after loading bdDestinations so that each
configured guild receives a // consolidated status embed on startup.
Subsequent updates are handled by // the poller and broadcast logic. try
{ updateBdStatusMessages().catch(() => {}); } catch {}

// Attach listeners for all existing and new users to broadcast clips
try { const usersSnap = await rtdb.ref(‘users’).get(); if
(usersSnap.exists()) {
Object.keys(usersSnap.val()).forEach(attachPostsListener); }
rtdb.ref(‘users’).on(‘child_added’, s => attachPostsListener(s.key)); }
catch (e) { console.error(‘[broadcast] failed to attach initial user
listeners’, e); }

// Load clan battle/event destination channels and attach live updates
try { const snap2 = await rtdb.ref(‘config/battleDestinations’).get();
globalCache.battleDestinations.clear(); if (snap2.exists()) { const all
= snap2.val() || {}; for (const [guildId, cfg] of Object.entries(all)) {
if (cfg?.channelId) globalCache.battleDestinations.set(guildId,
cfg.channelId); } }
console.log([battle broadcast] loaded initial battle destinations: ${globalCache.battleDestinations.size});
// Listen for updates to battle destinations
rtdb.ref(‘config/battleDestinations’).on(‘child_added’, s => { const v =
s.val() || {}; if (v.channelId)
globalCache.battleDestinations.set(s.key, v.channelId); });
rtdb.ref(‘config/battleDestinations’).on(‘child_changed’, s => { const v
= s.val() || {}; if (v.channelId)
globalCache.battleDestinations.set(s.key, v.channelId); else
globalCache.battleDestinations.delete(s.key); });
rtdb.ref(‘config/battleDestinations’).on(‘child_removed’, s => {
globalCache.battleDestinations.delete(s.key); }); } catch (e) {
console.error(‘[battle broadcast] failed to load battle destinations’,
e); } // Attach listener for clan battles to broadcast accepted battles
try { const ref = rtdb.ref(‘battles’); // Listen for new battles
ref.orderByChild(‘createdAt’).startAt(Date.now() -
5000).on(‘child_added’, s => { const battle = s.val();
maybeBroadcastBattle(s.key, battle).catch(e => console.error(‘[battle
broadcast] add error’, e)); }, err => console.error(‘[battle broadcast]
listener error (add)’, err)); // Listen for updates (e.g., pending ->
accepted) ref.on(‘child_changed’, s => { const battle = s.val();
maybeBroadcastBattle(s.key, battle).catch(e => console.error(‘[battle
broadcast] change error’, e)); }, err => console.error(‘[battle
broadcast] listener error (change)’, err)); } catch (e) {
console.error(‘[battle broadcast] failed to attach battle listeners’,
e); } });

function attachPostsListener(uid) { // Placeholder based on context of
original file having this function }

(async () => { try { console.log(‘Registering slash commands…’); await
registerCommands(); console.log(‘Slash command registration complete.’);
} catch (e) { console.error(‘registerCommands FAILED:’, e?.rawError ||
e); }

// Claim lock before client login // Start bot only after we own the
lock async function startWhenLocked() { while (true) { try { const got =
await claimBotLock(); if (got) break; console.log(‘Standby — lock held
by another instance. Retrying in 20s…’); } catch (e) {
console.warn(‘Lock claim error:’, e.message); } await new Promise(r =>
setTimeout(r, 20_000)); } console.log(‘Lock acquired — logging into
Discord…’);

    // Load persisted top scores (will also seed hardcoded scores and fetch west stats)
    await loadBdTop();

    // Start Top Score persistence loop
    setInterval(async () => {
        if(bdTopDirty) {
            bdTopDirty = false;
            await saveBdTop();
        }
    }, 30000).unref();

    // Attach process exit handlers to persist top scores on shutdown
    const gracefulSave = async (exitCode = 0) => {
      try {
        if (bdTopDirty) {
          // attempt to save without clearing the flag (avoid race conditions)
          await saveBdTop();
          bdTopDirty = false;
        }
      } catch (e) {
        console.error('[BdTop] Failed to save on exit:', e?.message || e);
      } finally {
        process.exit(exitCode);
      }
    };
    process.once('SIGINT', () => gracefulSave(0));
    process.once('SIGTERM', () => gracefulSave(0));
    process.once('uncaughtException', (err) => {
      console.error('Uncaught exception:', err);
      gracefulSave(1);
    });
    process.once('unhandledRejection', (reason) => {
      console.error('Unhandled rejection:', reason);
      gracefulSave(1);
    });

    // West stats refresh removed – West region retired

    await client.login(process.env.DISCORD_BOT_TOKEN);

    // Start Battledome Poller only after acquiring lock and login
    pollBattledome().catch(e => console.error('BD Poller failed to start:', e));

    // Start background cache warming loop. This runs forever and ensures users always see a snapshot.
    warmBdCachesForever().catch(err => console.error('[BD Warm] fatal:', err));

    setInterval(() => renewBotLock().catch(() => {}), 30_000).unref();

}

startWhenLocked().catch(e => { console.error(‘Failed to start bot:’, e);
process.exit(1); }); })();
