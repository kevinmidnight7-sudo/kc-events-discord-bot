// index.js
// Discord.js v14 + Firebase Admin (Realtime Database & Firestore)
// Commands: /link (DMs auth link), /badges (public profile embed), /whoami (debug), /dumpme (debug)

// Make dotenv optional (used locally, ignored on Render if not installed)
try { require('dotenv').config(); } catch (_) {}

// Healthcheck server for Render
const express = require('express');
const app = express();
app.get('/', (_req, res) => res.status(200).send('OK'));
app.get('/health', (_req, res) => {
  const status = {
    up: true,
    uptime: process.uptime(),
    memory: process.memoryUsage().rss,
    ts: Date.now(),
  };
  res.status(200).json(status);
});
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Healthcheck on :${PORT}`));

// --- PATCH (Step 5): Add global error hooks ---
process.on("unhandledRejection", (err) => {
  console.error("[unhandledRejection]", err);
});
process.on("uncaughtException", (err) => {
  console.error("[uncaughtException]", err);
});
// --- END PATCH ---

console.log('ENV sanity', {
  hasToken: !!process.env.DISCORD_BOT_TOKEN,
  hasClient: !!process.env.DISCORD_CLIENT_ID,
  hasGuild: !!process.env.DISCORD_GUILD_ID,          // optional
  hasDbUrl: !!process.env.FB_DATABASE_URL,
  hasSAJson: !!process.env.FB_SERVICE_ACCOUNT_JSON,
  hasSAPath: !!process.env.FB_SERVICE_ACCOUNT_PATH,
});


const {
  Client, GatewayIntentBits, Partials,
  SlashCommandBuilder, Routes, REST,
  ChannelType, PermissionFlagsBits, GuildMember,
  ActionRowBuilder, ButtonBuilder, ButtonStyle,
  ModalBuilder, TextInputBuilder, TextInputStyle,
  EmbedBuilder,
  StringSelectMenuBuilder
} = require('discord.js');

const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

// Node HTTPS module used to fetch images for role icons
const https = require('https');

const EMOJI = {
  offence:  '<:red:1407696672229818579>',
  defence:  '<:blue:1407696743474139260>',
  overall:  '<:gold:1407696766719234119>',
  verified: '<:Verified:1407697810866045008>',
  diamond:  '<:diamondi:1407696929059635212>',
  emerald:  '<:emeraldi:1407696908033593365>',
};

const LB = {
  CATS: [
    { key: 'overall', label: 'Overall Winner' },
    { key: 'offence', label: 'Best Offence' },
    { key: 'defence', label: 'Best Defence' },
  ],
  PAGE_SIZE: 10,
};

const CLIPS = {
  PAGE_SIZE: 5,
  REACTS: ['🔥','😂','😮','👍','💯'],
  MAX_LIST: 50
};

const POST_EMOJIS = ['🔥','❤️','😂','👍','👎']; // same list you show on the site

const DEFAULT_EMBED_COLOR = 0xF1C40F;

// Simple in-memory cache for frequently accessed, non-critical data
const globalCache = {
  userNames: new Map(), // uid -> displayName
  userNamesFetchedAt: 0,
  clipDestinations: new Map(), // guildId -> channelId
  // Destination channels for clan battle announcements (guildId -> channelId)
  battleDestinations: new Map(),
  // Destination channels for Battledome updates (guildId -> channelId)
  bdDestinations: new Map(),
};

// only these will be private
const isEphemeralCommand = (name) =>
  new Set(['whoami', 'dumpme', 'help', 'vote', 'syncavatar', 'post', 'postmessage', 'link', 'setclipschannel', 'latestfive', 'notifybd', 'setbattledomechannel']).has(name);

// Parse #RRGGBB -> int for embed.setColor
function hexToInt(hex) {
  if (!hex || typeof hex !== 'string') return null;
  const m = hex.match(/#([0-9a-f]{6})/i);
  return m ? parseInt(m[1], 16) : null;
}

// Extract first hex colour from a CSS gradient string
function firstHexFromGradient(grad) {
  if (!grad || typeof grad !== 'string') return null;
  const m = grad.match(/#([0-9a-f]{6})/i);
  return m ? `#${m[1]}` : null;
}

// ---------- Firebase Admin init ----------
function loadServiceAccount() {
  if (process.env.FB_SERVICE_ACCOUNT_JSON) {
    try {
      return JSON.parse(process.env.FB_SERVICE_ACCOUNT_JSON);
    } catch (e) {
      console.error('Failed to parse FB_SERVICE_ACCOUNT_JSON:', e);
      process.exit(1);
    }
  }
  if (process.env.FB_SERVICE_ACCOUNT_PATH) {
    const p = process.env.FB_SERVICE_ACCOUNT_PATH;
    try {
      return JSON.parse(fs.readFileSync(path.resolve(p), 'utf8'));
    } catch (e) {
      console.error('Failed to read FB_SERVICE_ACCOUNT_PATH:', p, e);
      process.exit(1);
    }
  }
  console.error('No service account provided. Set FB_SERVICE_ACCOUNT_JSON or FB_SERVICE_ACCOUNT_PATH.');
  process.exit(1);
}

const serviceAccount = loadServiceAccount();

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FB_DATABASE_URL,            // REQUIRED for RTDB
  storageBucket: process.env.FB_STORAGE_BUCKET || undefined,
});

const rtdb = admin.database();

// ===== Bot lock =====
const LOCK_KEY = '_runtime/botLock';
const LOCK_TTL_MS = 90_000; // 90s
const OWNER_ID = process.env.RENDER_INSTANCE_ID || `pid:${process.pid}`;

async function claimBotLock() {
  const now = Date.now();
  // IMPORTANT: correct transaction signature (no options object)
  const result = await rtdb.ref(LOCK_KEY).transaction(cur => {
    if (!cur) {
      return { owner: OWNER_ID, expiresAt: now + LOCK_TTL_MS };
    }
    if (cur.expiresAt && cur.expiresAt < now) {
      // stale -> take over
      return { owner: OWNER_ID, expiresAt: now + LOCK_TTL_MS };
    }
    // someone else owns it
    return; // abort
  }, undefined /* onComplete */, false /* applyLocally */);

  // result has .committed (compat) or .committed-like semantics; if using admin,
  // just treat "snapshot.val()?.owner === OWNER_ID" as success:
  const snap = await rtdb.ref(LOCK_KEY).get();
  const val = snap.val() || {};
  return val.owner === OWNER_ID;
}

async function renewBotLock() {
  const now = Date.now();
  await rtdb.ref(LOCK_KEY).transaction(cur => {
    if (!cur) return;               // nothing to renew
    if (cur.owner !== OWNER_ID) return; // not ours anymore
    cur.expiresAt = now + LOCK_TTL_MS;
    return cur;
  }, undefined, false);
}

// ---------- Helpers ----------
function encPath(p){ return String(p).replace(/\//g, '|'); }
function decPath(s){ return String(s).replace(/\|/g, '/'); }

async function withTimeout(promise, ms, label='op'){
  return Promise.race([
    promise,
    new Promise((_,rej)=>setTimeout(()=>rej(new Error(`Timeout ${ms}ms: ${label}`)), ms))
  ]);
}

// --- Interaction Ack Helpers ---
async function safeDefer(interaction, opts = {}) {
  try {
    if (interaction.deferred || interaction.replied) return;
    if (interaction.isChatInputCommand()) {
      return await interaction.deferReply({ ephemeral: !!opts.ephemeral });
    }
    if (interaction.isMessageComponent()) {
      if (opts.intent === "update") {
        return await interaction.deferUpdate();
      }
      return;
    }
    if (interaction.isModalSubmit()) {
      return await interaction.deferReply({ ephemeral: !!opts.ephemeral });
    }
  } catch (err) {
    console.error("safeDefer error:", err);
  }
}

async function safeReply(interaction, options) {
  try {
    if (interaction.isMessageComponent()) {
      if (interaction.deferred && !interaction.replied) {
        return await interaction.update(options);
      }
      if (interaction.replied) {
        return await interaction.followUp(options);
      }
      return await interaction.reply(options);
    }
    if (interaction.deferred) {
      if (interaction.replied) {
        return await interaction.followUp(options);
      }
      return await interaction.editReply(options);
    }
    if (interaction.replied) {
      return await interaction.followUp(options);
    }
    return await interaction.reply(options);
  } catch (err) {
    console.error("safeReply error", {
      deferred: interaction.deferred,
      replied: interaction.replied,
      isChatInput: interaction.isChatInputCommand(),
      isComponent: interaction.isMessageComponent(),
      isModal: interaction.isModalSubmit(),
    }, err);
    if (err.code !== 10062) { 
        try {
            if (!interaction.replied) {
                await interaction.followUp(options);
            }
        } catch (e) {
            console.error("safeReply followup failed", e);
        }
    }
  }
}

// --- Battledome Helpers (NEW) ---
const BD = {
  TIMEOUT_MS: 10000,
};

// Authoritative Server List (HTTP only)
const BD_SERVERS = {
  west: {
    name: "West Coast Battledome",
    url: "http://172.99.249.149:444/bdinfo.json",
    region: "west"
  },
  east: {
    name: "East Coast Battledome",
    url: "http://206.221.176.241:444/bdinfo.json",
    region: "east"
  },
  eu: {
    name: "EU Battledome",
    url: "http://51.91.19.175:444/bdinfo.json",
    region: "eu"
  }
};

// Map of Server Name -> Region Key (Fix for Battledome Selection)
const BD_NAME_OVERRIDES = {
  "West Coast Battledome": "west",
  "West Coast test BD": "west",
  "East Coast Battledome": "east",
  "New York Battledome": "east",
  "EU Battledome": "eu",
};

// Map region -> state for polling
const bdState = {
  west: { lastNames: new Set(), lastOnline: 0, lastCheck: 0 },
  east: { lastNames: new Set(), lastOnline: 0, lastCheck: 0 },
  eu:   { lastNames: new Set(), lastOnline: 0, lastCheck: 0 },
};

async function fetchJson(url, timeoutMs = BD.TIMEOUT_MS) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs).unref?.();

  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

// Tolerant parser for BD info
function parseBdInfo(json) {
  if (!json || typeof json !== 'object') return null;
  
  // Normalise list
  let players = Array.isArray(json.players) ? json.players : [];
  
  // Clean players
  players = players.map(p => ({
    name: p.name || '(unknown)',
    score: parseInt(p.score, 10) || 0,
    rank: p.rank,
    indomenow: p.indomenow, // might be undefined
    inactive: p.inactive // might be undefined (seconds)
  })).filter(p => p.name !== '(unknown)'); 

  // Sort by score desc
  players.sort((a,b) => b.score - a.score);

  return {
    name: json.name || 'Unknown Server',
    onlinenow: parseInt(json.onlinenow, 10) || 0,
    indomenow: parseInt(json.indomenow, 10) || 0,
    thishour: parseInt(json.thishour, 10) || 0,
    players
  };
}

async function fetchBdInfo(url) {
  const json = await fetchJson(url);
  return parseBdInfo(json);
}
// --- End Battledome Helpers ---

async function hasEmerald(uid) {
  // Prefer RTDB; fall back to Firestore if needed
  try {
    const snap = await rtdb.ref(`users/${uid}/codesUnlocked`).get();
    const codes = snap.exists() ? (snap.val() || {}) : {};
    if (codes.emerald === true || codes.diamond === true || codes.content === true) return true;
  } catch (_) {}

  try {
    const fsDoc = await admin.firestore().collection('users').doc(uid).get();
    if (fsDoc.exists) {
      const u = fsDoc.data() || {};
      // a few fallbacks seen in your data
      if (u.codesUnlocked?.emerald === true || u.codesUnlocked?.diamond === true || u.postsUnlocked === true || u.canPost === true) {
        return true;
      }
    }
  } catch (_) {}

  return false;
}

async function setKCAvatar(uid, url) {
  // RTDB
  await rtdb.ref(`users/${uid}`).update({
    avatar: url,
    photoURL: url,
    avatarSource: 'discord',
    avatarUpdatedAt: Date.now(),
  });
  // Firestore (best-effort)
  try {
    await admin.firestore().collection('users').doc(uid).set({
      avatar: url,
      photoURL: url,
      avatarSource: 'discord',
      avatarUpdatedAt: Date.now(),
    }, { merge: true });
  } catch (_) {}
}

async function clearKCAvatar(uid) {
  const { FieldValue } = admin.firestore;
  // RTDB: remove override fields by setting to null
  await rtdb.ref(`users/${uid}`).update({
    avatar: null,
    photoURL: null,
    avatarSource: null,
    avatarUpdatedAt: Date.now(),
  });
  // Firestore (best-effort)
  try {
    await admin.firestore().collection('users').doc(uid).set({
      avatar: FieldValue.delete(),
      photoURL: FieldValue.delete(),
      avatarSource: FieldValue.delete(),
      avatarUpdatedAt: Date.now(),
    }, { merge: true });
  } catch (_) {}
}

function parseVideoLink(link='') {
  link = String(link).trim();

  // YouTube ID
  const yt = link.match(/(?:youtu\.be\/|youtube\.com\/(?:watch\?.*v=|embed\/|v\/|shorts\/))([\w-]{11})/);
  if (yt) return { type: 'youtube', ytId: yt[1] };

  // TikTok video ID
  const tt = link.match(/tiktok\.com\/.*\/video\/(\d+)/);
  if (tt) return { type: 'tiktok', videoId: tt[1] };

  return null;
}

function clipLink(d={}) {
  if (d.type === 'youtube' && d.ytId) return `https://youtu.be/${d.ytId}`;
  else if (d.type === 'tiktok' && d.videoId) return `https://www.tiktok.com/embed/v2/${d.videoId}`;
  return '';
}
function clipThumb(d={}) {
  if (d.type === 'youtube' && d.ytId) return `https://i.ytimg.com/vi/${d.ytId}/hqdefault.jpg`;
  return null; // TikTok: no stable thumbnail without scraping
}
function reactCount(reactions={}) {
  const perEmoji = {};
  for (const e of Object.keys(reactions||{})) perEmoji[e] = Object.keys(reactions[e]||{}).length;
  const total = Object.values(perEmoji).reduce((a,b)=>a+b,0);
  return { perEmoji, total };
}
function sitePostUrl(ownerUid, postId) {
  // If you have a deep link on the site, put it here; otherwise link to feed:
  return `https://kcevents.uk/#socialfeed`;
}

function getClipsState(interaction) {
  interaction.client.clipsCache ??= new Map();
  const key = interaction.message?.interaction?.id || interaction.id; // root command id
  return interaction.client.clipsCache.get(key);
}
function getClipByIdx(interaction, idx) {
  const state = getClipsState(interaction);
  if (!state || !Array.isArray(state.list)) throw new Error('Clips state missing/expired');
  const item = state.list[idx];
  if (!item) throw new Error('Unknown clip index');
  return { state, item, idx };
}
function clipDbPath(item) {
  return `users/${item.ownerUid}/posts/${item.postId}`;
}

async function postingUnlocked(uid) {
  // same idea as your site: emerald/diamond/content OR explicit flags
  try {
    const snap = await withTimeout(rtdb.ref(`users/${uid}`).get(), 6000, `RTDB users/${uid}`);
    const u = snap.exists() ? (snap.val() || {}) : {};
    const codes = u.codesUnlocked || {};
    return !!(codes.emerald || codes.diamond || codes.content || u.postsUnlocked || u.canPost);
  } catch {
    return false;
  }
}

// Optional: global flag like your site uses
async function postsDisabledGlobally() {
  try {
    const s = await withTimeout(rtdb.ref('config/postsDisabled').get(), 4000, 'RTDB config/postsDisabled');
    return !!s.val();
  } catch { return false; }
}

// ----- Shared helpers for new commands -----

// ----- START: Clan Helper Functions -----
function getUserScores(uid, usersData, badgesData) {
  const user = usersData[uid] || {};
  const stats = user.stats || {};
  const b = badgesData[uid] || {};
  const offence = Number(stats.offence ?? b.offence ?? 0) || 0;
  const defence = Number(stats.defence ?? b.defence ?? 0) || 0;
  const overall = Number(stats.overall ?? b.overall ?? 0) || 0;
  return { offence, defence, overall, total: offence + defence + overall };
}

function computeClanScore(clan, usersData, badgesData) {
  if (!clan || !clan.members) return 0;
  let sum = 0;
  for (const uid of Object.keys(clan.members)) {
    sum += getUserScores(uid, usersData, badgesData).total;
  }
  return sum;
}
// ----- END: Clan Helper Functions -----

// Helper to clamp string lengths
const clamp = (s, n=100) => (s || '').toString().slice(0, n);

function normalize(name=''){ return name.toLowerCase().replace(/[^a-z0-9]/g,''); }

function countReactions(reactionsObj = {}) {
  // reactions: { "😀": { uid:true, ... }, "🔥": { uid:true }, ... }
  let n = 0;
  for (const emo of Object.keys(reactionsObj || {})) {
    n += Object.keys(reactionsObj[emo] || {}).length;
  }
  return n;
}

function countComments(commentsObj = {}) {
  // comments: { commentId: { text, uid, time, replies:{ replyId:{...} } } }
  let n = 0;
  for (const cid of Object.keys(commentsObj || {})) {
    n += 1;
    const r = commentsObj[cid]?.replies || {};
    n += Object.keys(r).length;
  }
  return n;
}

// Map of uid -> displayName/email for quick lookups
async function getAllUserNames() {
  const CACHE_DURATION = 60 * 1000; // 60 seconds
  const now = Date.now();
  if (now - globalCache.userNamesFetchedAt < CACHE_DURATION) {
    return globalCache.userNames;
  }

  const snap = await withTimeout(rtdb.ref('users').get(), 8000, 'RTDB users');
  const out = new Map();
  if (snap.exists()) {
    const all = snap.val() || {};
    for (const uid of Object.keys(all)) {
      const u = all[uid] || {};
      out.set(uid, u.displayName || u.email || '(unknown)');
    }
  }
  globalCache.userNames = out;
  globalCache.userNamesFetchedAt = now;
  return out;
}

// Gather all posts across users; return an array of {ownerUid, postId, data, score, reacts, comments}
async function fetchAllPosts({ platform = 'all' } = {}) {
  const usersSnap = await withTimeout(rtdb.ref('users').get(), 8000, 'RTDB users');
  const users = usersSnap.exists() ? usersSnap.val() : {};
  const results = [];
  const started = Date.now();
  let totalPostsSeen = 0;

  const tasks = Object.keys(users).map(async uid => {
    if (Date.now() - started > 7500 || totalPostsSeen > 500) return;
    const postsSnap = await withTimeout(rtdb.ref(`users/${uid}/posts`).get(), 6000, `RTDB users/${uid}/posts`);
    if (!postsSnap.exists()) return;

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

  await Promise.allSettled(tasks);
  return results;
}

async function fetchLatestMessages(limit = 10) {
  const OVERFETCH = Math.max(limit * 3, 30);

  async function snapToMsgs(snap) {
    const arr = [];
    if (snap?.exists && snap.exists()) {
      snap.forEach(c => {
        const v = c.val() || {};
        // filter: must look like a message
        const isMsg = typeof v === 'object' &&
          (typeof v.text === 'string' || typeof v.user === 'string' || typeof v.uid === 'string');
        if (isMsg) arr.push({ key: c.key, ...(v || {}) });
      });
      // sort newest first by time (fallback to key if missing)
      arr.sort((a, b) => ((b.time || 0) - (a.time || 0)) || (b.key > a.key ? 1 : -1));
    }
    return arr.slice(0, limit).map(m => ({ ...m, path: `messages/${m.key}` }));
  }

  // Try indexed query first
  try {
    const snap = await withTimeout(
      rtdb.ref('messages').orderByChild('time').limitToLast(OVERFETCH).get(),
      8000,
      'RTDB messages recent'
    );
    return await snapToMsgs(snap);
  } catch (e) {
    // Fallback if index missing or anything else
    console.warn('[messages] falling back to unordered fetch:', e?.message || e);
    const snap = await withTimeout(
      rtdb.ref('messages').limitToLast(OVERFETCH).get(),
      8000,
      'RTDB messages fallback'
    );
    return await snapToMsgs(snap);
  }
}

// Build an embed showing a page of 10 messages (title, text, reply count)
function buildMessagesEmbed(list, nameMap) {
  const desc = list.map((m, i) => {
    const who =
      m.user ||
      nameMap.get(m.uid) ||
      m.username ||
      m.displayName ||
      m.name ||
      '(unknown)';
    const when = m.time ? new Date(m.time).toLocaleString() : '—';
    const rawText = m.text ?? m.message ?? m.content ?? m.body ?? '';
    const textStr = String(rawText || '');
    const text = textStr.length > 100 ? textStr.slice(0, 100) + '…' : (textStr || null);
    const replies = m.replies ? Object.keys(m.replies).length : 0;
    return `**${i + 1}. ${who}** — _${when}_\n— ${text || '_no text_'}\nReplies: **${replies}**`;
  }).join('\n\n');

  return new EmbedBuilder()
    .setTitle('Messageboard — latest 10')
    .setDescription(desc || 'No messages yet.')
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: 'KC Bot • /messages' });
}

function messageIndexRows(count) {
  // buttons 1..count (max 10), plus a Refresh row
  const rows = [];
  for (let i = 0; i < count; i += 5) {
    const row = new ActionRowBuilder();
    for (let j = i; j < Math.min(i + 5, count); j++) {
      row.addComponents(
        new ButtonBuilder()
          .setCustomId(`msg:view:${j}`)
          .setLabel(String(j + 1))
          .setStyle(ButtonStyle.Secondary)
      );
    }
    rows.push(row);
  }
  rows.push(
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('msg:refresh').setLabel('Refresh').setStyle(ButtonStyle.Primary)
    )
  );
  return rows;
}

function fmtTime(ms){
  const d = new Date(ms || Date.now());
  return d.toLocaleString();
}

function buildMessageDetailEmbed(msg, nameMap) {
  const who =
    msg.user ||
    nameMap.get(msg.uid) ||
    msg.username ||
    msg.displayName ||
    msg.name ||
    '(unknown)';
  const when = msg.time ? new Date(msg.time).toLocaleString() : '—';
  const rawText = msg.text ?? msg.message ?? msg.content ?? msg.body ?? '';
  const text = String(rawText || '');
  const likes = msg.likes || (msg.likedBy ? Object.keys(msg.likedBy).length : 0) || 0;
  const replies = msg.replies ? Object.keys(msg.replies).length : 0;

  return new EmbedBuilder()
    .setTitle(who)
    .setDescription(text.slice(0, 4096))
    .addFields(
      { name: 'Likes', value: String(likes), inline: true },
      { name: 'Replies', value: String(replies), inline: true },
    )
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: `Posted ${when} • KC Bot • /messages` });
}

function messageDetailRows(idx, list, path, hasReplies = true) {
  const max = list.length - 1;
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId(`msg:openIdx:${Math.max(idx-1,0)}`).setLabel('◀ Prev').setStyle(ButtonStyle.Secondary).setDisabled(idx<=0),
      new ButtonBuilder().setCustomId(`msg:openIdx:${Math.min(idx+1,max)}`).setLabel('Next ▶').setStyle(ButtonStyle.Secondary).setDisabled(idx>=max),
      new ButtonBuilder().setCustomId(`msg:thread:${encPath(path)}:0`).setLabel('Open thread').setStyle(ButtonStyle.Secondary).setDisabled(!hasReplies),
      new ButtonBuilder().setCustomId('msg:back').setLabel('Back to list').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId(`msg:refreshOne:${idx}`).setLabel('Refresh').setStyle(ButtonStyle.Secondary),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId(`msg:like:${encPath(path)}`).setLabel('❤️ Like/Unlike').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId(`msg:reply:${encPath(path)}`).setLabel('↩️ Reply').setStyle(ButtonStyle.Primary),
    ),
  ];
}

async function loadNode(path) {
  const snap = await withTimeout(rtdb.ref(path).get(), 6000, `RTDB ${path}`);
  return snap.exists() ? { key: path.split('/').slice(-1)[0], path, ...(snap.val()||{}) } : null;
}

async function loadReplies(path) {
  const snap = await withTimeout(rtdb.ref(`${path}/replies`).get(), 6000, `RTDB ${path}/replies`);
  const list = [];
  if (snap.exists()) {
    snap.forEach(c => list.push({ key: c.key, path: `${path}/replies/${c.key}`, ...(c.val()||{}) }));
    list.sort((a,b)=>(b.time||0)-(a.time||0));
  }
  return list;
}

function buildThreadEmbed(parent, children, page=0, pageSize=10, nameMap) {
  const start = page*pageSize;
  const slice = children.slice(start, start+pageSize);
  const lines = slice.map((r,i)=>{
    const who =
      r.user ||
      nameMap.get(r.uid) ||
      r.username ||
      r.displayName ||
      r.name ||
      '(unknown)';
    const raw = r.text ?? r.message ?? r.content ?? r.body ?? '';
    const txt = String(raw || '').slice(0,120) || '(no text)';
    return `**${i+1}. ${who}** — ${txt}`;
  }).join('\n\n') || '_No replies yet_';

  const parentWho = parent?.user || nameMap.get(parent?.uid) || parent?.username || parent?.displayName || parent?.name || '(unknown)';

  return new EmbedBuilder()
    .setTitle(`Thread — ${parentWho}`)
    .setDescription(lines)
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: `KC Bot • /messages` });
}

function threadRows(parentPath, children, page=0, pageSize=10) {
  const rows = [];
  const maxPage = Math.max(0, Math.ceil(children.length/pageSize)-1);
  const start = page*pageSize;

  const numRow = new ActionRowBuilder();
  for (let i=0; i<Math.min(children.length-start,5); i++) {
    numRow.addComponents(
      new ButtonBuilder()
        .setCustomId(`msg:openChild:${encPath(children[start+i].path)}`)
        .setLabel(String(i+1))
        .setStyle(ButtonStyle.Secondary)
    );
  }
  if (numRow.components.length) rows.push(numRow);

  const ctrl = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`msg:threadPrev:${encPath(parentPath)}:${Math.max(page-1,0)}`)
      .setLabel('◀ Page')
      .setStyle(ButtonStyle.Secondary)
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

  rows.push(ctrl);
  return rows;
}

// ===== Clan Battles Helpers =====
const BATTLES_PAGE_SIZE = 10;

/**
 * Build an embed listing a page of clan battles. Each line is numbered and includes
 * the clan names, scheduled date/time and the current number of participants.
 * For past battles the winner will be shown instead of a join count.
 *
 * @param {Array<[string,object]>} list Array of [battleId, battleData] pairs
 * @param {string} filterType One of 'all', 'my' or 'past'
 * @param {object} clansData Map of clanId -> clan object
 * @param {object} usersData Map of userId -> user object (unused here but passed for future use)
 * @returns {EmbedBuilder}
 */
function buildBattlesListEmbed(list = [], filterType = 'all', clansData = {}, usersData = {}) {
  const titleMap = {
    all: 'Upcoming Clan Battles',
    my: 'Your Clan Battles',
    past: 'Past Clan Battles'
  };
  const title = titleMap[filterType] || 'Clan Battles';

  const now = Date.now();
  const desc = list.slice(0, BATTLES_PAGE_SIZE).map(([bid, b], idx) => {
    const c1 = clansData[b.challengerId] || {};
    const c2 = clansData[b.targetId] || {};
    const name1 = c1.name || 'Unknown';
    const name2 = c2.name || 'Unknown';
    let line = `**${idx + 1}. ${name1} vs ${name2}**`;
    // Format date/time if available
    if (b.scheduledTime) {
      const d = new Date(b.scheduledTime);
      // Use UK locale for date/time as user is in Europe/London
      const dateStr = d.toLocaleDateString('en-GB', { timeZone: 'Europe/London' });
      const timeStr = d.toLocaleTimeString('en-GB', { timeZone: 'Europe/London', hour: '2-digit', minute: '2-digit' });
      line += ` — ${dateStr} ${timeStr}`;
    }
    // Past battles show winner
    if (filterType === 'past' || b.status === 'finished') {
      const win = clansData[b.winnerId] || {};
      if (win.name) line += ` — Winner: ${win.name}`;
    } else {
      // Upcoming: show participant count
      const count = b.participants ? Object.keys(b.participants).length : 0;
      line += ` — Participants: ${count}`;
    }
    return line;
  }).join('\n\n') || 'No battles found.';

  return new EmbedBuilder()
    .setTitle(title)
    .setDescription(desc)
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: 'KC Bot • /clanbattles' });
}

/**
 * Build an embed showing details for a single clan battle. Includes server, date/time,
 * rules, a list of participants and the winner if the battle has finished.
 *
 * @param {string} battleId Battle identifier
 * @param {object} battle The battle data
 * @param {object} clansData Map of clanId -> clan object
 * @param {object} usersData Map of userId -> user object
 * @returns {EmbedBuilder}
 */
function buildBattleDetailEmbed(battleId, battle = {}, clansData = {}, usersData = {}, includeDesc = false) {
  const c1 = clansData[battle.challengerId] || {};
  const c2 = clansData[battle.targetId] || {};
  const title = `${c1.name || 'Unknown'} vs ${c2.name || 'Unknown'}`;
  const embed = new EmbedBuilder()
    .setTitle(title)
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: 'KC Bot • /clanbattles' });

  // Main description: scheduled time
  if (battle.scheduledTime) {
    const d = new Date(battle.scheduledTime);
    const dateStr = d.toLocaleDateString('en-GB', { timeZone: 'Europe/London' });
    const timeStr = d.toLocaleTimeString('en-GB', { timeZone: 'Europe/London', hour: '2-digit', minute: '2-digit' });
    embed.setDescription(`Scheduled for **${dateStr} ${timeStr}**`);
  }
  // Add server
  embed.addFields({ name: 'Server', value: battle.server || 'N/A', inline: true });
  // Add rules
  embed.addFields({ name: 'Rules', value: battle.rules || 'N/A', inline: true });
  // Participants list
  const parts = battle.participants || {};
  const partNames = Object.keys(parts).map(uid => {
    const u = usersData[uid] || {};
    return u.displayName || u.username || u.email || uid;
  });
  const partCount = partNames.length;
  let partValue = partCount > 0 ? partNames.join(', ') : 'No participants yet';
  if (partValue.length > 1024) {
    // Truncate if too long
    partValue = partNames.slice(0, 30).join(', ') + ` … (+${partCount - 30} more)`;
  }
  embed.addFields({ name: `Participants (${partCount})`, value: partValue, inline: false });
  // Winner if finished
  if (battle.status === 'finished') {
    const win = clansData[battle.winnerId] || {};
    const winName = win.name || 'Unknown';
    embed.addFields({ name: 'Winner', value: winName, inline: false });
  }

  // Optional extended description
  // Some battles may include a `description` property (or rules text) that provides more
  // context about the event. When includeDesc is true and a description exists,
  // append it as a separate field. This mirrors the web UI's info toggle.
  if (includeDesc) {
    const descText = battle.description || battle.desc || battle.rules || null;
    if (descText) {
      // Clamp very long descriptions to avoid exceeding embed limits
      let val = String(descText);
      if (val.length > 1024) {
        val = val.slice(0, 1021) + '…';
      }
      embed.addFields({ name: 'Description', value: val, inline: false });
    }
  }
  return embed;
}

/**
 * Build an embed for a help page. Each page includes a title, optional image, and
 * a standard list of resource links. The color is consistent with other embeds.
 *
 * @param {object} page - An object with an `image` property (URL string).
 * @returns {EmbedBuilder}
 */
function buildHelpEmbed(page = {}) {
  const embed = new EmbedBuilder()
    .setTitle('KC Events — Help')
    .setColor(DEFAULT_EMBED_COLOR);
  if (page.image) {
    embed.setImage(page.image);
  }
  embed.addFields({
    name: 'Links',
    value: [
      'Full Messageboard : https://kcevents.uk/#chatscroll',
      'Full Clips       : https://kcevents.uk/#socialfeed',
      'Full Voting      : https://kcevents.uk/#voting',
    ].join('\n'),
  });
  return embed;
}

function buildClipsListEmbed(list=[], page=0, nameMap) {
  const start = page * CLIPS.PAGE_SIZE;
  const slice = list.slice(start, start + CLIPS.PAGE_SIZE);

  const lines = slice.map((p, i) => {
    const d = p.data || {};
    const who = nameMap.get(p.ownerUid) || '(unknown)';
    const cap = (d.caption || '').trim() || '(no caption)';
    const link = clipLink(d);
    const meta = `👍 ${p.reacts} • 💬 ${p.comments}`;
    const idx = start + i + 1;
    return `**${idx}.** ${cap}${link ? ` — ${link}` : ''}\nUploader: **${who}** • ${meta}`;
  }).join('\n\n') || 'No clips found.';

  return new EmbedBuilder()
    .setTitle(`Top ${Math.min(list.length, CLIPS.PAGE_SIZE)} Clips`)
    .setDescription(lines)
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: 'KC Bot • /clips' });
}

function clipsListRows(listLen, page=0) {
  const rows = [];
  const start = page * CLIPS.PAGE_SIZE;
  const visible = Math.min(CLIPS.PAGE_SIZE, Math.max(0, listLen - start));

  const num = new ActionRowBuilder();
  for (let i = 0; i < visible; i++) {
    num.addComponents(
      new ButtonBuilder()
        .setCustomId(`c:o:${start+i}`)   // open idx
        .setLabel(String(start + i + 1))
        .setStyle(ButtonStyle.Secondary)
    );
  }
  if (num.components.length) rows.push(num);

  const maxPage = Math.max(0, Math.ceil(listLen/CLIPS.PAGE_SIZE)-1);
  rows.push(
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId(`c:p:${Math.max(page-1,0)}`).setLabel('◀ Page').setStyle(ButtonStyle.Secondary).setDisabled(page<=0),
      new ButtonBuilder().setCustomId(`c:p:${Math.min(page+1,maxPage)}`).setLabel('Page ▶').setStyle(ButtonStyle.Secondary).setDisabled(page>=maxPage),
      new ButtonBuilder().setCustomId('c:rf').setLabel('Refresh').setStyle(ButtonStyle.Primary),
    )
  );
  return rows;
}

function buildClipDetailEmbed(item, nameMap) {
  const d = item.data || {};
  const who = nameMap.get(item.ownerUid) || '(unknown)';
  const link = clipLink(d);
  const cap = (d.caption || '').trim() || '(no caption)';
  const { perEmoji, total } = reactCount(d.reactions || {});
  const reactsLine = CLIPS.REACTS.map(e => `${e} ${perEmoji[e]||0}`).join('  ');
  const comments = Object.keys(d.comments || {}).length;

  const e = new EmbedBuilder()
    .setTitle(cap.slice(0,256))
    .setURL(link || undefined)
    .setDescription([
      `Uploader: **${who}**`,
      `Reactions: ${reactsLine}  (total ${total})`,
      `Comments: **${comments}**`,
      link,
    ].filter(Boolean).join('\n'))
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: 'KC Bot • /clips' });

  const thumb = clipThumb(d);
  if (thumb) e.setImage(thumb); // shows a preview; Discord will auto-embed YT URLs when clicked

  return e;
}

function clipsDetailRows(interactionOrMessage, postPath) {
  const rows = [];
  const row1 = new ActionRowBuilder();
  const client = interactionOrMessage.client;
  
  // Use interaction.message if available (from button), otherwise interaction (from slash command reply)
  const message = interactionOrMessage.message || interactionOrMessage;

  for (const emo of POST_EMOJIS) {
    const sid = _cacheForMessage(
      client.reactCache,
      message, // Use the message object
      { postPath, emoji: emo }
    );
    row1.addComponents(
      new ButtonBuilder()
        .setCustomId(`clips:react:${sid}`)
        .setLabel(emo)
        .setStyle(ButtonStyle.Secondary)
    );
  }
  if (row1.components.length) rows.push(row1);

  const sidView = _cacheForMessage(
    client.reactorsCache,
    message, // Use the message object
    { postPath }
  );
  
  const sidComments = _cacheForMessage(
    client.commentsCache, 
    message, // Use the message object
    { postPath, page: 0 }
  );

  const commentSid = cacheModalTarget({ postPath });
  const row2 = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`clips:reactors:${sidView}`)
      .setLabel('👀 View reactors')
      .setStyle(ButtonStyle.Secondary),
    new ButtonBuilder()
      .setCustomId(`clips:comments:${sidComments}`)
      .setLabel('💬 View comments')
      .setStyle(ButtonStyle.Secondary),
    new ButtonBuilder()
      .setCustomId(`clips:comment:${commentSid}`)
      .setLabel('💬 Comment')
      .setStyle(ButtonStyle.Primary),
    new ButtonBuilder()
      .setCustomId('clips:back') // Changed from 'c:b' to be specific
      .setLabel('Back to list')
      .setStyle(ButtonStyle.Secondary),
  );
  rows.push(row2);

  return rows;
}

async function loadClipComments(postPath) {
    const snap = await rtdb.ref(`${postPath}/comments`).get();
    if (!snap.exists()) return [];
    const comments = [];
    snap.forEach(child => {
        const data = child.val();
        comments.push({
            key: child.key,
            uid: data.uid,
            user: data.user, // Will be replaced by nameMap lookup later
            text: data.text,
            time: data.time,
            repliesCount: data.replies ? Object.keys(data.replies).length : 0,
        });
    });
    return comments.sort((a, b) => b.time - a.time);
}

function buildClipCommentsEmbed(item, comments, page, nameMap) {
    const pageSize = 10;
    const start = page * pageSize;
    const pageComments = comments.slice(start, start + pageSize);

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

function commentsRows(sid, page, maxPage) {
    const row = new ActionRowBuilder();
    const prevTarget = Math.max(page - 1, 0);
    const nextTarget = Math.min(page + 1, maxPage);

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


// Votes -> scores
async function loadVoteScores() {
  const [cfgSnap, votesSnap] = await Promise.all([
    withTimeout(rtdb.ref('config/liveLeaderboardEnabled').get(), 6000, 'RTDB config'),
    withTimeout(rtdb.ref('votes').get(), 8000, 'RTDB votes'),
  ]);

  const live = cfgSnap.exists() ? cfgSnap.val() !== false : true;
  const votes = votesSnap.exists() ? votesSnap.val() : {};

  const off = {}, def = {};
  for (const key of Object.keys(votes)) {
    const v = votes[key] || {};
    const o = normalize(v.bestOffence || '');
    const d = normalize(v.bestDefence || '');
    if (o) off[o] = (off[o] || 0) + 1;
    if (d) def[d] = (def[d] || 0) + 1;
  }

  const usersSnap = await withTimeout(rtdb.ref('users').get(), 8000, 'RTDB users');
  const users = usersSnap.exists() ? usersSnap.val() : {};
  const normToDisplay = {};
  for (const uid of Object.keys(users)) {
    const name = users[uid]?.displayName || users[uid]?.email || '';
    const k = normalize(name);
    if (k && !normToDisplay[k]) normToDisplay[k] = name;
  }

  const sortPairs = obj =>
    Object.entries(obj)
      .map(([k, n]) => ({ name: normToDisplay[k] || k, count: n }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 10);

  return { live, offence: sortPairs(off), defence: sortPairs(def) };
}

function buildVoteEmbed(scores) {
  const offLines = scores.offence.map((x, i) => `**${i + 1}. ${x.name}** — \`${x.count}\``).join('\n') || '_No votes yet_';
  const defLines = scores.defence.map((x, i) => `**${i + 1}. ${x.name}** — \`${x.count}\``).join('\n') || '_No votes yet_';

  const e = new EmbedBuilder()
    .setTitle(`Live Voting Scores ${scores.live ? '' : '(offline)'}`)
    .addFields(
      { name: 'Best Offence', value: offLines, inline: false },
      { name: 'Best Defence', value: defLines, inline: false },
    )
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: 'KC Bot • /votingscores' });

  return e;
}

async function loadLeaderboardData() {
  const [usersSnap, badgesSnap] = await Promise.all([
    withTimeout(rtdb.ref('users').get(), 6000, 'RTDB users'),
    withTimeout(rtdb.ref('badges').get(), 6000, 'RTDB badges'),
  ]);

  const users  = (usersSnap && usersSnap.exists())  ? usersSnap.val()  : {};
  const badges = (badgesSnap && badgesSnap.exists()) ? badgesSnap.val() : {};

  return Object.entries(users).map(([uid, u]) => {
    const b = badges[uid] || {};
    return {
      name:   u.displayName || u.email || '(unknown)',
      colour: u.profileCustomization?.nameColor || null,
      overall: parseInt(b.overall  || 0),
      offence: parseInt(b.offence  || 0),
      defence: parseInt(b.defence  || 0),
    };
  });
}

function buildLbEmbed(rows, catIdx, page) {
  const cat = LB.CATS[catIdx];
  const start = page * LB.PAGE_SIZE;
  const slice = [...rows]
    .sort((a,b) => (b[cat.key]||0) - (a[cat.key]||0))
    .slice(start, start + LB.PAGE_SIZE);

  const lines = slice.map((r, i) => {
    const rank = start + i + 1;
    return `**${rank}.** ${r.name} — \`${r[cat.key] || 0}\``;
  });
  const embed = new EmbedBuilder()
    .setTitle(`Leaderboard — ${cat.label}`)
    .setDescription(lines.join('\n') || '_No data_')
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: 'KC Bot • /leaderboard' });
  return embed;
}

function lbRow(catIdx, page) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`lb:cat:${(catIdx+2)%3}:${page}`).setLabel('◀ Category').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`lb:page:${catIdx}:${Math.max(page-1,0)}`).setLabel('◀ Page').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`lb:page:${catIdx}:${page+1}`).setLabel('Page ▶').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`lb:cat:${(catIdx+1)%3}:${page}`).setLabel('Category ▶').setStyle(ButtonStyle.Secondary),
  );
}

async function getKCUidForDiscord(discordId) {
  try {
    const snap = await withTimeout(
      rtdb.ref(`discordLinks/${discordId}`).get(),
      8000, // increased timeout
      `RTDB discordLinks/${discordId}`
    );
    return snap.exists() ? (snap.val() || {}).uid || null : null;
  } catch {
    return null; // don't throw prior to replying
  }
}

function clampStr(val, max, fallback = '—') {
  if (val == null) return fallback;
  const s = String(val);
  if (s.length === 0) return fallback;
  return s.length > max ? s.slice(0, max) : s;
}

async function getKCProfile(uid) {
  const firestore = admin.firestore();

  // 1) RTDB reads (preferred)
  const [userSnapRT, badgeSnapRT, postsSnapRT] = await Promise.allSettled([
    withTimeout(rtdb.ref(`users/${uid}`).get(), 6000, `RTDB users/${uid}`),
    withTimeout(rtdb.ref(`badges/${uid}`).get(), 6000, `RTDB badges/${uid}`),
    withTimeout(rtdb.ref(`users/${uid}/posts`).get(), 6000, `RTDB users/${uid}/posts`),
  ]);

  const safeVal = s => (s && s.status === 'fulfilled' && s.value && s.value.exists()) ? s.value.val() : null;
  let user   = safeVal(userSnapRT)  || {};
  let badges = safeVal(badgeSnapRT) || {};
  let posts  = safeVal(postsSnapRT) || {};

  // 2) Firestore fallbacks (if RTDB missing)
  if (!user || Object.keys(user).length === 0) {
    try {
      const fsUser = await withTimeout(
        firestore.collection('users').doc(uid).get(),
        6000,
        `FS users/${uid}`
      );
      if (fsUser.exists) user = fsUser.data() || {};
    } catch (e) { console.warn('FS user read timeout/fail:', e.message); }
  }
  if (!posts || Object.keys(posts).length === 0) {
    try {
      const fsPosts = await withTimeout(
        firestore.collection('users').doc(uid).collection('posts').get(),
        6000,
        `FS users/${uid}/posts`
      );
      if (!fsPosts.empty) {
        posts = {};
        fsPosts.forEach(d => { posts[d.id] = d.data(); });
      }
    } catch (e) { console.warn('FS posts read timeout/fail:', e.message); }
  }
  // (Badges also sometimes appear in FS; keep RTDB as source of truth per site)

  // -------- Field normalisation (match your site) --------
  const about =
    user.about ??
    user.aboutMe ??
    user.bio ??
    'No "About Me" set.';

  const displayName =
    user.displayName ??
    user.name ??
    user.username ??
    'Anonymous User';

  const streak = Number.isFinite(user.loginStreak) ? String(user.loginStreak) : '—';

  // Profile customization colour (tint embed)
  const custom = user.profileCustomization || {};
  const nameColor = custom.nameColor || null;
  const gradientColor = custom.gradient ? firstHexFromGradient(custom.gradient) : null;

  // Posts visible if content unlocked (diamond/emerald codes or explicit content)
  const codesUnlocked = user.codesUnlocked || {};
  const postingAllowed = !!(codesUnlocked.content || codesUnlocked.diamond || codesUnlocked.emerald || user.postsUnlocked || user.canPost);

  // Build at most 3 post lines as:  • "Caption" — <link>
  let postLines = [];
  if (postingAllowed && posts) {
    const list = Object.entries(posts)
      .filter(([,p]) => p && !p.draft && (!p.publishAt || p.publishAt < Date.now())) // Filter out drafts/scheduled
      .sort((a, b) => (b[1]?.createdAt || 0) - (a[1]?.createdAt || 0))
      .slice(0, 3);

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
  }
  const postsField =
    !postingAllowed
      ? 'Posts locked. Unlock posting on your profile.'
      : (Object.keys(posts).length === 0 ? 'This user has no posts.' : (postLines.join('\n') || 'This user has no posts.'));

  // Badges summary – same three counted on site + verified/diamond/emerald
  const counts = {
    offence: parseInt(badges.offence ?? badges.bestOffence ?? 0) || 0,
    defence: parseInt(badges.defence ?? badges.bestDefence ?? 0) || 0,
    overall: parseInt(badges.overall  ?? badges.overallWins  ?? 0) || 0,
  };
  const isVerified = !!(user.emailVerified === true || (user.badges && user.badges.verified === true));
  const hasDiamond = !!codesUnlocked.diamond;
  const hasEmerald = !!codesUnlocked.emerald;

  // Convert to human lines (we’ll keep emojis inside Discord text)
  const e = EMOJI;
  const badgeLines = [];
  if (isVerified)                  badgeLines.push(`${e.verified ?? '✅'} Verified`);
  if (counts.offence > 0)          badgeLines.push(`${e.offence ?? '🏹'} Best Offence x${counts.offence}`);
  if (counts.defence > 0)          badgeLines.push(`${e.defence ?? '🛡️'} Best Defence x${counts.defence}`);
  if (counts.overall > 0)          badgeLines.push(`${e.overall  ?? '🌟'} Overall Winner x${counts.overall}`);
  if (hasDiamond)                  badgeLines.push(`${e.diamond ?? '💎'} Diamond User`);
  if (hasEmerald)                  badgeLines.push(`${e.emerald ?? '🟩'} Emerald User`);

  const customBadges = user.customBadges || {};
  for (const key of Object.keys(customBadges)) {
    const b = customBadges[key] || {};
    const piece = [b.icon ?? b.emoji, b.name ?? b.label].filter(Boolean).join(' ');
    if (piece) badgeLines.push(piece);
  }

  return {
    id: uid, // Pass the UID through
    displayName,
    about,
    streak,
    badgesText: badgeLines.length ? badgeLines.join('\n') : 'No badges yet.',
    postsText: postsField,
    postingUnlocked: postingAllowed, // Pass this through for the /post command check
    // embed colour preference: nameColor > gradient first colour > default
    embedColor: hexToInt(nameColor) || hexToInt(gradientColor) || null,
  };
}

function norm(s=''){ return s.toLowerCase().replace(/[^a-z0-9]/g,''); }

async function getVerifiedNameMap() {
  const snap = await withTimeout(rtdb.ref('users').get(), 8000, 'RTDB users for voting');
  const map = {}; // normalizedName -> original displayName/email
  if (snap.exists()) {
    const all = snap.val() || {};
    for (const uid of Object.keys(all)) {
      const u = all[uid] || {};
      if (u.emailVerified) {
        const name = u.displayName || u.email || '';
        const k = norm(name);
        if (k && !map[k]) map[k] = name;
      }
    }
  }
  return map;
}

async function userIsVerified(uid) {
  const s = await withTimeout(rtdb.ref(`users/${uid}/emailVerified`).get(), 6000, 'RTDB emailVerified');
  return !!s.val();
}

async function getExistingVotesBy(uid) {
  const q = await withTimeout(
    rtdb.ref('votes').orderByChild('uid').equalTo(uid).get(),
    8000, 'RTDB votes by uid'
  );
  const found = [];
  if (q.exists()) q.forEach(c => found.push({ key:c.key, ...(c.val()||{}) }));
  return found;
}

async function showVoteModal(interaction, defaults={}) {
  const modal = new ModalBuilder()
    .setCustomId('vote:modal')
    .setTitle('KC Events — Vote');

  const offIn = new TextInputBuilder()
    .setCustomId('voteOff')
    .setLabel('Best Offence (type player name)')
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setValue(defaults.off || '');

  const defIn = new TextInputBuilder()
    .setCustomId('voteDef')
    .setLabel('Best Defence (type player name)')
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setValue(defaults.def || '');

  const rateIn = new TextInputBuilder()
    .setCustomId('voteRate')
    .setLabel('Event rating 1–5')
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setValue(defaults.rating ? String(defaults.rating) : '');

  modal.addComponents(
    new ActionRowBuilder().addComponents(offIn),
    new ActionRowBuilder().addComponents(defIn),
    new ActionRowBuilder().addComponents(rateIn),
  );

  return interaction.showModal(modal);
}

async function loadCustomBadges(uid) {
  const s = await withTimeout(rtdb.ref(`users/${uid}/customBadges`).get(), 6000, `RTDB users/${uid}/customBadges`);
  const out = [];
  if (s.exists()) {
    s.forEach(c => {
      const b = c.val() || {};
      if (b.name) out.push(`${b.icon || ''} ${b.name}`.trim());
    });
  }
  return out.slice(0, 10); // clamp to fit embed field
}

// --- PATCH (Step 2/4): This function is now wrapped by the try/catch in the main handler
async function handleSetClipsChannel(interaction) {
    if (!interaction.inGuild()) {
      return safeReply(interaction, { content: 'Run this in a server.', ephemeral: true });
    }

    const picked = interaction.options.getChannel('channel', true);

    // Must be this guild and not a thread
    if (picked.guildId !== interaction.guildId) {
      return safeReply(interaction, { content: 'That channel isn’t in this server.', ephemeral: true });
    }
    // --- FIX (Item 3): Robust thread check ---
    if (typeof picked.isThread === 'function' && picked.isThread()) {
      return safeReply(interaction, { content: 'Pick a text channel, not a thread.', ephemeral: true });
    }
    // --- END FIX ---

    // Re-fetch via the global ChannelManager to avoid partials/null guild references
    const chan = await interaction.client.channels.fetch(picked.id).catch(() => null);
    if (!chan || !chan.isTextBased?.()) {
      return safeReply(interaction, { content: 'Pick a text or announcement channel I can post in.', ephemeral: true });
    }

    // Invoker must have Manage Server
    const invokerOk =
      interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild) ||
      interaction.member?.permissions?.has?.(PermissionFlagsBits.ManageGuild);
    if (!invokerOk) {
      return safeReply(interaction, { content: 'You need the **Manage Server** permission to set this.', ephemeral: true });
    }

    // Bot perms in that channel
    const me = interaction.guild.members.me ?? await interaction.guild.members.fetchMe().catch(() => null);
    if (!me) {
      return safeReply(interaction, { content: 'I couldn’t resolve my member record. Reinvite me or check my permissions.', ephemeral: true });
    }
    const botOk = chan.permissionsFor(me).has([
      PermissionFlagsBits.ViewChannel,
      PermissionFlagsBits.SendMessages,
      PermissionFlagsBits.EmbedLinks,
    ]);
    if (!botOk) {
      return safeReply(interaction, { content: 'I need **View Channel**, **Send Messages**, and **Embed Links** in that channel.', ephemeral: true });
    }

    // PATCH: Write to the correct RTDB path with the correct structure
    await rtdb.ref(`config/clipDestinations/${interaction.guildId}`).set({
      channelId: chan.id,
      updatedBy: interaction.user.id,
      updatedAt: admin.database.ServerValue.TIMESTAMP,
    });
    globalCache.clipDestinations.set(interaction.guildId, chan.id);

    return safeReply(interaction, { content: `✅ Clips will be posted in <#${chan.id}>.`, ephemeral: true });
}

// -----------------------------------------------------------------------------
// Clan Battle & Roles Helpers
//
// These helpers implement the commands for clan challenges, role assignments and
// event announcements. They mirror the behaviour on the KC website where clan
// owners can challenge other clans to battles, view incoming challenges, and
// assign custom roles to members. In addition, accepted clan battles will be
// broadcast automatically to configured channels in each guild.

/**
 * Return the owner UID for a given clan object. The KC database stores the
 * owner as `clan.owner`. If the field is missing or not a string, null is returned.
 *
 * @param {Object|null|undefined} clan The clan object from RTDB
 * @returns {string|null}
 */
function getOwnerUid(clan) {
  if (!clan || typeof clan !== 'object') return null;
  const owner = clan.owner;
  return typeof owner === 'string' ? owner : null;
}

/**
 * Handle the /seteventschannel command. Writes the selected channel to
 * `config/battleDestinations/<guildId>` in RTDB so that accepted clan battles
 * can be announced there. Mirrors the behaviour of /setclipschannel.
 *
 * Requires Manage Server permissions on the invoking member and ensures the bot
 * has View Channel, Send Messages and Embed Links in the chosen channel.
 */
async function handleSetEventsChannel(interaction) {
  if (!interaction.inGuild()) {
    return safeReply(interaction, { content: 'Run this in a server.', ephemeral: true });
  }

  const picked = interaction.options.getChannel('channel', true);
  if (picked.guildId !== interaction.guildId) {
    return safeReply(interaction, { content: 'That channel isn’t in this server.', ephemeral: true });
  }
  if (typeof picked.isThread === 'function' && picked.isThread()) {
    return safeReply(interaction, { content: 'Pick a text channel, not a thread.', ephemeral: true });
  }
  // Re-fetch channel to ensure full object
  const chan = await interaction.client.channels.fetch(picked.id).catch(() => null);
  if (!chan || !chan.isTextBased?.()) {
    return safeReply(interaction, { content: 'Pick a text or announcement channel I can post in.', ephemeral: true });
  }
  // Permissions check for invoker
  const invokerOk = interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild) ||
    interaction.member?.permissions?.has?.(PermissionFlagsBits.ManageGuild);
  if (!invokerOk) {
    return safeReply(interaction, { content: 'You need the **Manage Server** permission to set this.', ephemeral: true });
  }
  // Bot perms check in that channel
  const me = interaction.guild.members.me ?? await interaction.guild.members.fetchMe().catch(() => null);
  if (!me) {
    return safeReply(interaction, { content: 'I couldn’t resolve my member record. Reinvite me or check my permissions.', ephemeral: true });
  }
  const botOk = chan.permissionsFor(me).has([
    PermissionFlagsBits.ViewChannel,
    PermissionFlagsBits.SendMessages,
    PermissionFlagsBits.EmbedLinks,
  ]);
  if (!botOk) {
    return safeReply(interaction, { content: 'I need **View Channel**, **Send Messages**, and **Embed Links** in that channel.', ephemeral: true });
  }
  // Write to RTDB
  await rtdb.ref(`config/battleDestinations/${interaction.guildId}`).set({
    channelId: chan.id,
    updatedBy: interaction.user.id,
    updatedAt: admin.database.ServerValue.TIMESTAMP,
  });
  globalCache.battleDestinations.set(interaction.guildId, chan.id);
  return safeReply(interaction, { content: `✅ Clan battles will be announced in <#${chan.id}>.`, ephemeral: true });
}

/**
 * Handle the /getclanroles command. Determines the invoking user's clan and
 * creates or assigns a Discord role corresponding to that clan. Owners receive
 * a role suffixed with "Owner". Roles are created with the clan’s name and
 * icon fetched from the KC database. If the role already exists, it is simply
 * assigned to the user.
 */
async function handleGetClanRoles(interaction) {
  if (!interaction.inGuild()) {
    return safeReply(interaction, { content: 'Run this in a server.', ephemeral: true });
  }
  // Resolve the KC UID and find the user's clan
  const discordId = interaction.user.id;
  const uid = await getKCUidForDiscord(discordId);
  if (!uid) {
    return safeReply(interaction, { content: 'Link your KC account first with /link.', ephemeral: true });
  }
  // Fetch all clans to locate membership
  const clansSnap = await rtdb.ref('clans').get();
  const clansData = clansSnap.exists() ? clansSnap.val() || {} : {};
  let userClanId = null;
  let userClan = null;
  for (const [cid, clan] of Object.entries(clansData)) {
    if (clan.members && clan.members[uid]) {
      userClanId = cid;
      userClan = clan;
      break;
    }
  }
  if (!userClanId) {
    return safeReply(interaction, { content: 'You are not in a clan.', ephemeral: true });
  }
  const isOwner = getOwnerUid(userClan) === uid;
  const baseName = userClan.name || userClanId;
  const roleName = isOwner ? `${baseName} Owner` : baseName;
  // Check if role exists already
  const guild = interaction.guild;
  const existing = guild.roles.cache.find(r => r.name === roleName);
  let role;
  if (existing) {
    role = existing;
  } else {
    // Fetch icon image from clan data (if present and looks like a URL)
    let iconBuf = null;
    const iconUrl = userClan.icon;
    if (iconUrl && /^https?:\/\//i.test(iconUrl)) {
      try {
        iconBuf = await new Promise((resolve, reject) => {
          https.get(iconUrl, res => {
            const data = [];
            res.on('data', chunk => data.push(chunk));
            res.on('end', () => resolve(Buffer.concat(data)));
          }).on('error', reject);
        });
      } catch (e) {
        console.warn(`[getClanRoles] failed to fetch icon for ${userClanId}:`, e.message);
      }
    }
    // Create new role. The bot must have Manage Roles; colour left unset (null) so default is used
    const createData = { name: roleName };
    if (iconBuf) {
      createData.icon = iconBuf;
    }
    try {
      role = await guild.roles.create(createData);
    } catch (e) {
      console.error('[getClanRoles] failed to create role:', e);
      return safeReply(interaction, { content: 'Failed to create the role. Check my permissions and try again later.', ephemeral: true });
    }
  }
  // Assign the role to the member (requires Manage Roles)
  try {
    await interaction.member.roles.add(role);
  } catch (e) {
    console.error('[getClanRoles] failed to assign role:', e);
    return safeReply(interaction, { content: 'Failed to assign the role to you. Check my permissions and role hierarchy.', ephemeral: true });
  }
  return safeReply(interaction, { content: `✅ You have been given the **${roleName}** role.`, ephemeral: true });
}

/**
 * Handle the /incomingchallenges command. Shows a list of pending clan battle
 * challenges where the invoking user’s clan is either the challenger or target.
 * Only clan owners can use this command. The user will see an embed listing
 * each pending challenge with Accept and Decline buttons.
 */
async function handleIncomingChallenges(interaction) {
  if (!interaction.inGuild()) {
    return safeReply(interaction, { content: 'Run this in a server.', ephemeral: true });
  }
  const discordId = interaction.user.id;
  const uid = await getKCUidForDiscord(discordId);
  if (!uid) {
    return safeReply(interaction, { content: 'Link your KC account first with /link.', ephemeral: true });
  }
  // Fetch clans and determine the user’s clan
  const clansSnap = await rtdb.ref('clans').get();
  const clansData = clansSnap.exists() ? clansSnap.val() || {} : {};
  let clanId = null;
  let clan = null;
  for (const [cid, c] of Object.entries(clansData)) {
    if (c.members && c.members[uid]) {
      clanId = cid;
      clan = c;
      break;
    }
  }
  if (!clanId) {
    return safeReply(interaction, { content: 'You are not in a clan.', ephemeral: true });
  }
  if (getOwnerUid(clan) !== uid) {
    return safeReply(interaction, { content: 'You must be a Clan Owner to run this command!', ephemeral: true });
  }
  // Fetch all battles and filter pending involving this clan
  const battlesSnap = await rtdb.ref('battles').get();
  const allBattles = battlesSnap.exists() ? battlesSnap.val() || {} : {};
  const pendingList = [];
  for (const [bid, b] of Object.entries(allBattles)) {
    if (b.status === 'pending' && (b.challengerId === clanId || b.targetId === clanId)) {
      pendingList.push([bid, b]);
    }
  }
  if (pendingList.length === 0) {
    return safeReply(interaction, { content: 'There are no pending challenges for your clan.', ephemeral: true });
  }
  // Build embed
  const embed = new EmbedBuilder()
    .setTitle('Incoming Clan Challenges')
    .setDescription('Below are the pending clan battle challenges. Use the buttons to accept or decline.')
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: 'KC Bot • /incomingchallenges' });
  // Add a field for each challenge (limit to first 10 to avoid hitting embed limits)
  const nameMap = await getAllUserNames();
  pendingList.slice(0, 10).forEach(([bid, b], idx) => {
    const c1 = clansData[b.challengerId] || {};
    const c2 = clansData[b.targetId] || {};
    const d = b.scheduledTime ? new Date(b.scheduledTime) : null;
    const dateStr = d ? d.toLocaleDateString('en-GB', { timeZone: 'Europe/London' }) : 'N/A';
    const timeStr = d ? d.toLocaleTimeString('en-GB', { timeZone: 'Europe/London', hour: '2-digit', minute: '2-digit' }) : '';
    const title = `${c1.name || b.challengerId} vs ${c2.name || b.targetId}`;
    const valLines = [];
    if (b.server) valLines.push(`Server: ${b.server}`);
    if (b.scheduledTime) valLines.push(`Date: ${dateStr} ${timeStr}`);
    if (b.rules) valLines.push(`Rules: ${b.rules}`);
    embed.addFields({ name: `${idx + 1}. ${title}`, value: valLines.join('\n') || '\u200b' });
  });
  // Build rows of Accept/Decline buttons. Each challenge gets its own row to avoid hitting the limit of 5 buttons per row.
  const rows = [];
  const parentId = interaction.id;
  pendingList.slice(0, 10).forEach(([bid], idx) => {
    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`cc:accept:${parentId}:${bid}`)
        .setLabel('Accept')
        .setStyle(ButtonStyle.Success),
      new ButtonBuilder()
        .setCustomId(`cc:decline:${parentId}:${bid}`)
        .setLabel('Decline')
        .setStyle(ButtonStyle.Danger),
    );
    rows.push(row);
  });
  // Cache this state so we can handle button interactions. We also store the user’s clanId.
  interaction.client.challengeCache ??= new Map();
  interaction.client.challengeCache.set(parentId, { clanId, pendingList });
  // Reply with embed and buttons
  return safeReply(interaction, { embeds: [embed], components: rows, });
}

/**
 * Handle the /sendclanchallenge command. Only owners can send challenges. The
 * command takes a string identifying the target clan. If valid, a modal is
 * presented to capture server, date/time and rules. The modal custom ID
 * encodes the interaction ID and target clan ID so that submission can be
 * processed later. If the user is not an owner or if the target clan is
 * invalid, an appropriate error message is shown.
 */
async function handleSendClanChallenge(interaction) {
  // Confirm KC account and clan membership
  const discordId = interaction.user.id;
  const uid = await getKCUidForDiscord(discordId);
  if (!uid) {
    return safeReply(interaction, { content: 'Link your KC account first with /link.', ephemeral: true });
  }
  // Load clans
  const clansSnap = await rtdb.ref('clans').get();
  const clansData = clansSnap.exists() ? clansSnap.val() || {} : {};
  let myClanId = null;
  let myClan = null;
  for (const [cid, c] of Object.entries(clansData)) {
    if (c.members && c.members[uid]) {
      myClanId = cid;
      myClan = c;
      break;
    }
  }
  if (!myClanId) {
    return safeReply(interaction, { content: 'You are not in a clan.', ephemeral: true });
  }
  // Only owners can send challenges
  if (getOwnerUid(myClan) !== uid) {
    return safeReply(interaction, { content: 'You must be a Clan Owner to run this command!', ephemeral: true });
  }
  const targetQuery = (interaction.options.getString('clan') || '').trim().toLowerCase();
  if (!targetQuery) {
    return safeReply(interaction, { content: 'Please specify a target clan.', ephemeral: true });
  }
  // Find target clan by id or name (case-insensitive)
  let targetId = null;
  let targetClan = null;
  for (const [cid, c] of Object.entries(clansData)) {
    if (cid.toLowerCase() === targetQuery || (c.name && c.name.toLowerCase() === targetQuery)) {
      targetId = cid;
      targetClan = c;
      break;
    }
  }
  if (!targetId) {
    return safeReply(interaction, { content: 'Could not find that clan. Provide a valid clan name or ID.', ephemeral: true });
  }
  if (targetId === myClanId) {
    return safeReply(interaction, { content: 'You cannot challenge your own clan.', ephemeral: true });
  }
  // Check there isn’t already a pending challenge between the clans
  const battlesSnap = await rtdb.ref('battles').get();
  const battles = battlesSnap.exists() ? battlesSnap.val() || {} : {};
  for (const b of Object.values(battles)) {
    if (b.status === 'pending' && ((b.challengerId === myClanId && b.targetId === targetId) || (b.challengerId === targetId && b.targetId === myClanId))) {
      return safeReply(interaction, { content: 'A pending challenge already exists between these clans.', ephemeral: true });
    }
  }
  // Build modal
  const modal = new ModalBuilder()
    .setCustomId(`scc:${interaction.id}:${targetId}`)
    .setTitle('Send Clan Challenge');
  const serverInput = new TextInputBuilder()
    .setCustomId('scc_server')
    .setLabel('Server')
    .setRequired(true)
    .setPlaceholder('e.g. Europe West')
    .setStyle(TextInputStyle.Short);
  const dateInput = new TextInputBuilder()
    .setCustomId('scc_datetime')
    .setLabel('Date & Time (ISO)')
    .setRequired(true)
    .setPlaceholder('2025-08-21T10:00')
    .setStyle(TextInputStyle.Short);
  const rulesInput = new TextInputBuilder()
    .setCustomId('scc_rules')
    .setLabel('Rules')
    .setRequired(true)
    .setPlaceholder('List any battle rules here')
    .setStyle(TextInputStyle.Paragraph);
  modal.addComponents(
    new ActionRowBuilder().addComponents(serverInput),
    new ActionRowBuilder().addComponents(dateInput),
    new ActionRowBuilder().addComponents(rulesInput),
  );
  // Show the modal to the user. We do not need to defer in the slash handler when showing a modal.
  return interaction.showModal(modal);
}

/**
 * Broadcast an accepted clan battle to all configured guild channels. This
 * function is called when a battle is added or updated. Only battles with
 * `status === 'accepted'` will be announced. Each guild’s announcement is
 * tracked under `battles/<battleId>/postedToDiscord/<guildId>` so that we
 * don’t post duplicates. If a battle was accepted more than a configurable
 * number of milliseconds ago, it won’t be broadcast. Use the
 * BATTLE_BROADCAST_MAX_AGE_MS env var or default to 30 minutes.
 */
async function maybeBroadcastBattle(battleId, battle) {
  try {
    if (!battle || battle.status !== 'accepted') return;
    // Skip if the battle is too old. Use createdAt as proxy; if missing, broadcast anyway.
    const maxAge = parseInt(process.env.BATTLE_BROADCAST_MAX_AGE_MS || '1800000', 10);
    const created = battle.createdAt || Date.now();
    if (Date.now() - created > maxAge) return;
    // Load clan and user data once
    const [clansSnap, usersSnap] = await Promise.all([
      rtdb.ref('clans').get(),
      rtdb.ref('users').get(),
    ]);
    const clansData = clansSnap.exists() ? clansSnap.val() || {} : {};
    const usersData = usersSnap.exists() ? usersSnap.val() || {} : {};
    // Iterate over configured guilds
    for (const [guildId, channelId] of globalCache.battleDestinations.entries()) {
      const flagRef = rtdb.ref(`battles/${battleId}/postedToDiscord/${guildId}`);
      try {
        const tx = await flagRef.transaction(cur => {
          if (cur) return; // already posted
          return { pending: true, by: client.user.id, at: Date.now() };
        });
        if (!tx.committed) continue;
        // Fetch channel and check permission
        const channel = await client.channels.fetch(channelId).catch(() => null);
        if (!channel || !channel.isTextBased?.()) {
          await flagRef.remove();
          continue;
        }
        const me = channel.guild?.members.me || await channel.guild?.members.fetchMe().catch(() => null);
        if (!me) {
          await flagRef.remove();
          continue;
        }
        const ok = channel.permissionsFor(me).has([
          PermissionFlagsBits.ViewChannel,
          PermissionFlagsBits.SendMessages,
          PermissionFlagsBits.EmbedLinks,
        ]);
        if (!ok) {
          await flagRef.remove();
          continue;
        }
        // Build embed (without extended description by default)
        const embed = buildBattleDetailEmbed(battleId, battle, clansData, usersData, false);
        // Buttons: join + info. For broadcast messages we default to Join; the button will change on interaction.
        const joinBtn = new ButtonBuilder()
          .setCustomId(`battle:join:${battleId}`)
          .setLabel('Join')
          .setStyle(ButtonStyle.Success);
        const infoBtn = new ButtonBuilder()
          .setCustomId(`battle:info:${battleId}:show`)
          .setLabel('Info')
          .setStyle(ButtonStyle.Secondary);
        const row = new ActionRowBuilder().addComponents(joinBtn, infoBtn);
        // Mention clan roles if they exist in this guild
        let content = '';
        try {
          const c1 = clansData[battle.challengerId] || {};
          const c2 = clansData[battle.targetId] || {};
          const mentions = [];
          if (c1.name) {
            const r1 = channel.guild.roles.cache.find(r => r.name === c1.name);
            if (r1) mentions.push(`<@&${r1.id}>`);
          }
          if (c2.name) {
            const r2 = channel.guild.roles.cache.find(r => r.name === c2.name);
            if (r2) mentions.push(`<@&${r2.id}>`);
          }
          content = mentions.join(' ');
        } catch (_) {
          content = '';
        }
        const msg = await channel.send({ content, embeds: [embed], components: [row] });
        await flagRef.set({
          messageId: msg.id,
          channelId: channel.id,
          by: client.user.id,
          at: admin.database.ServerValue.TIMESTAMP,
        });
        console.log(`[battle broadcast] posted battle ${battleId} to guild ${guildId}`);
      } catch (e) {
        console.error(`[battle broadcast] failed posting to ${guildId}:`, e);
        await flagRef.remove();
      }
    }
  } catch (e) {
    console.error('[battle broadcast] error in maybeBroadcastBattle:', e);
  }
}

// ---------- Discord Client ----------
const client = new Client({
  intents: [GatewayIntentBits.Guilds],
  partials: [Partials.Channel],
});

// Cache for /help pages navigation. Map key = parentId (unique per invocation).
// Each entry: { pages: Array<page>, index: number, userId: string }
client.helpCache = new Map();

// --- Short-id caches for /clips actions ---// Map key = "<messageId>|<shortId>"
client.reactCache = new Map();
client.reactorsCache = new Map();
client.commentsCache = new Map();
client.modalCache = new Map();

function _makeShortId(len = 8) {
  const abc = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let s = ''; for (let i = 0; i < len; i++) s += abc[Math.floor(Math.random()*abc.length)];
  return s;
}
function cacheModalTarget(payload, ttlMs = 10 * 60 * 1000) {
  const sid = _makeShortId(10);
  client.modalCache.set(sid, payload);
  setTimeout(() => client.modalCache.delete(sid), ttlMs).unref?.();
  return sid;
}
function readModalTarget(sid) {
  return client.modalCache.get(sid) || null;
}
function _cacheForMessage(map, message, payload, ttlMs = 10 * 60 * 1000) {
  const short = _makeShortId();
  const key = `${message.id}|${short}`;
  map.set(key, payload);
  setTimeout(() => map.delete(key), ttlMs).unref?.();
  return short;
}
function _readFromCache(map, message, shortId) {
  // PATCH: Ensure message is valid before accessing .id
  const msgId = message?.id;
  if (!msgId) return null;
  return map.get(`${msgId}|${shortId}`) || null;
}

// ---------- Slash Commands (definitions) ----------
const linkCmd = new SlashCommandBuilder()
  .setName('link')
  .setDescription('Link your Discord to your KC Events account');

const badgesCmd = new SlashCommandBuilder()
  .setName('badges')
  .setDescription('Show a KC Events profile')
  .addUserOption(opt =>
    opt.setName('user')
      .setDescription('Show someone else')
      .setRequired(false)
  );

const whoamiCmd = new SlashCommandBuilder()
  .setName('whoami')
  .setDescription('Show your Discord ID and resolved KC UID');

const dumpCmd = new SlashCommandBuilder()
  .setName('dumpme')
  .setDescription('Debug: dump raw keys for your mapped KC UID');

const lbCmd = new SlashCommandBuilder()
  .setName('leaderboard')
  .setDescription('Show the live KC Events leaderboard');

const clipsCmd = new SlashCommandBuilder()
  .setName('clips')
  .setDescription('Top 5 most popular clips')
  .addStringOption(o =>
    o.setName('platform')
     .setDescription('Filter by platform')
     .addChoices({ name:'All', value:'all' }, { name:'YouTube', value:'youtube' }, { name:'TikTok', value:'tiktok' })
     .setRequired(false)
  );

const latestFiveCmd = new SlashCommandBuilder()
  .setName('latestfive')
  .setDescription('Post the 5 most recently uploaded clips here')
  .addStringOption(o =>
    o.setName('platform')
     .setDescription('Filter by platform')
     .addChoices(
       { name: 'All', value: 'all' },
       { name: 'YouTube', value: 'youtube' },
       { name: 'TikTok', value: 'tiktok' },
     )
     .setRequired(false)
  );

const messagesCmd = new SlashCommandBuilder()
  .setName('messages')
  .setDescription('Show the latest 10 messageboard posts');

const votingCmd = new SlashCommandBuilder()
  .setName('votingscores')
  .setDescription('Show current live voting scores (Offence/Defence)');

const avatarCmd = new SlashCommandBuilder()
  .setName('syncavatar')
  .setDescription('Use your Discord avatar on KC (Emerald users)')
  .addStringOption(o =>
    o.setName('action')
     .setDescription('Choose what to do')
     .addChoices(
       { name: 'Set (use Discord avatar)', value: 'set' },
       { name: 'Revert (remove override)', value: 'revert' },
     )
     .setRequired(true)
  );

const postCmd = new SlashCommandBuilder()
  .setName('post')
  .setDescription('Create a YouTube or TikTok post on your KC profile')
  .addStringOption(o =>
    o.setName('link')
     .setDescription('YouTube or TikTok link')
     .setRequired(true))
  .addStringOption(o =>
    o.setName('caption')
     .setDescription('Caption (max 140 chars)')
     .setRequired(true))
  .addBooleanOption(o =>
    o.setName('draft')
     .setDescription('Save as draft (default: false)')
     .setRequired(false))
  .addStringOption(o =>
    o.setName('schedule_at')
     .setDescription('Schedule publish time ISO (e.g. 2025-08-21T10:00)')
     .setRequired(false));

const postMessageCmd = new SlashCommandBuilder()
  .setName('postmessage')
  .setDescription('Post a message to the message board')
  .addStringOption(o =>
    o.setName('text')
      .setDescription('The message to post')
      .setRequired(true)
  );

const helpCmd = new SlashCommandBuilder()
  .setName('help')
  .setDescription('Links to the full KC features');

const voteCmd = new SlashCommandBuilder()
  .setName('vote')
  .setDescription('Vote Best Offence, Best Defence and rate the event');

const compareCmd = new SlashCommandBuilder()
  .setName('compare')
  .setDescription('Compare your KC badges with another player')
  .addUserOption(o =>
    o.setName('user')
     .setDescription('The other Discord user')
     .setRequired(true)
  );

const setClipsChannelCmd = new SlashCommandBuilder()
  .setName('setclipschannel')
  .setDescription('Choose the channel where new KC clips will be posted.')
  .addChannelOption(opt =>
    opt.setName('channel')
      .setDescription('Text or Announcement channel in this server')
      .addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement)
      .setRequired(true)
  );

// Clan listing command
const clansCmd = new SlashCommandBuilder()
  .setName('clans')
  .setDescription('Browse KC clans and view details');

// New: Clan Battles command
// This command will allow users to view upcoming clan battles, filter between all battles,
// battles involving their own clan, and past battles. Users who are members of either
// participating clan can sign up to join an upcoming battle. Selecting a battle reveals
// additional details such as server, date/time, rules and a list of participants. A join
// button is shown if the current user is eligible to participate. This ties into the
// battles data stored under `battles` in Firebase RTDB.
const clanBattlesCmd = new SlashCommandBuilder()
  .setName('clanbattles')
  .setDescription('View clan battles and sign up if your clan is participating');

// Command: Send Clan Challenge
// Allows clan owners to challenge another clan to a battle. Owners will provide the target clan name or ID and then fill out
// server, date/time and rules via a modal. Only owners can use this command.
const sendClanChallengeCmd = new SlashCommandBuilder()
  .setName('sendclanchallenge')
  .setDescription('Challenge another clan to a battle (owner only)')
  .addStringOption(opt =>
    opt.setName('clan')
      .setDescription('Name or ID of the target clan')
      .setRequired(true)
  );

// Command: Incoming Challenges
// Allows clan owners to view pending challenges for their clan and accept or decline them.
const incomingChallengesCmd = new SlashCommandBuilder()
  .setName('incomingchallenges')
  .setDescription('View and respond to pending clan battle challenges (owner only)');

// Command: Get Clan Roles
// Assigns the clan role to the invoking user. If the role doesn’t exist, it will be created with the clan’s name and icon.
const getClanRolesCmd = new SlashCommandBuilder()
  .setName('getclanroles')
  .setDescription('Assign yourself your clan role (creates it if missing)');

// Command: Set Events Channel
// Configures which channel new accepted clan battles will be announced in this server. Requires Manage Guild.
const setEventsChannelCmd = new SlashCommandBuilder()
  .setName('seteventschannel')
  .setDescription('Choose the channel where new clan battles will be announced.')
  .addChannelOption(opt =>
    opt.setName('channel')
      .setDescription('Text or Announcement channel in this server')
      .addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement)
      .setRequired(true)
  );

// Command: Battledome
// View Battledome servers and who is currently playing
const battledomeCmd = new SlashCommandBuilder()
  .setName('battledome')
  .setDescription('View Battledome servers and who is currently playing');

// Command: Set Battledome Update Channel (NEW)
const setBattledomeChannelCmd = new SlashCommandBuilder()
  .setName('setbattledomechannel')
  .setDescription('Choose the channel for Battledome join/leave updates')
  .addChannelOption(opt =>
    opt.setName('channel')
      .setDescription('Text or Announcement channel')
      .addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement)
      .setRequired(true)
  );

// Command: Notify BD (NEW)
const notifyBdCmd = new SlashCommandBuilder()
  .setName('notifybd')
  .setDescription('Manage Battledome notifications')
  .addStringOption(o =>
    o.setName('region')
     .setDescription('Region to subscribe to')
     .addChoices(
       { name: 'West Coast', value: 'west' },
       { name: 'East Coast (NY)', value: 'east' },
       { name: 'EU', value: 'eu' }
     )
     .setRequired(false)
  )
  .addIntegerOption(o => 
    o.setName('threshold')
     .setDescription('Only ping if player count reaches this number (optional)')
     .setMinValue(1)
     .setMaxValue(200)
     .setRequired(false)
  )
  .addStringOption(o => 
    o.setName('action')
     .setDescription('Manage subscription')
     .addChoices(
        { name: 'Subscribe (default)', value: 'sub' },
        { name: 'Unsubscribe', value: 'unsub' },
        { name: 'Turn Off All', value: 'clear' }
     )
     .setRequired(false)
  );

// Command: Battledome Leaderboard (NEW)
const battledomeLbCmd = new SlashCommandBuilder()
  .setName('battledomelb')
  .setDescription('Show live leaderboard for a Battledome region')
  .addStringOption(o =>
    o.setName('region')
     .setDescription('Select region')
     .addChoices(
       { name: 'West Coast', value: 'west' },
       { name: 'East Coast (NY)', value: 'east' },
       { name: 'EU', value: 'eu' }
     )
     .setRequired(true)
  );

// Register (include it in commands array)
const commandsJson = [
  linkCmd, badgesCmd, whoamiCmd, dumpCmd, lbCmd, clipsCmd, messagesCmd, votingCmd,
  avatarCmd, postCmd, postMessageCmd, helpCmd, voteCmd, compareCmd, setClipsChannelCmd,
  latestFiveCmd,
  clansCmd, clanBattlesCmd, sendClanChallengeCmd, incomingChallengesCmd, getClanRolesCmd, setEventsChannelCmd,
  battledomeCmd, setBattledomeChannelCmd, notifyBdCmd, battledomeLbCmd
].map(c => c.toJSON());


// ---------- Register commands on startup ----------
async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(process.env.DISCORD_BOT_TOKEN);
  const clientId = process.env.DISCORD_CLIENT_ID;
  const guildId = process.env.DISCORD_GUILD_ID;

  try {
    if (guildId) {
      console.log('Registering guild commands…');
      await rest.put(Routes.applicationGuildCommands(clientId, guildId), { body: commandsJson });
      console.log('Guild commands registered ✅');
    } else {
      console.log('Registering global commands…');
      await rest.put(Routes.applicationCommands(clientId), { body: commandsJson });
      console.log('Global commands registered ✅ (may take a few minutes to appear)');
    }
  } catch (err) {
    console.error('Failed to register commands:', err);
  }
}

// ---------- Interaction handling ----------
let clientReady = false;
const MAX_AGE_MS = 15000; // be generous; we’ll still try to ack
client.on('interactionCreate', async (interaction) => {
  if (!clientReady) {
    try {
      // Use safeReply for consistency, though reply is fine here
      await safeReply(interaction, { content: 'Starting up, try again in a second.', ephemeral: true });
    } catch {}
    return;
  }
  const age = Date.now() - interaction.createdTimestamp;
  if (age > MAX_AGE_MS) {
    console.warn(`[old interaction ~${age}ms] attempting to acknowledge anyway`);
  }
  
  const seen = globalThis.__seen ??= new Set();
  if (seen.has(interaction.id)) {
    console.warn(`[INT] duplicate seen: ${interaction.id}`);
    return;
  }
  seen.add(interaction.id);
  setTimeout(() => seen.delete(interaction.id), 60_000);

  console.log(`[INT] ${interaction.isChatInputCommand() ? interaction.commandName : interaction.customId} from ${interaction.user?.tag} age=${Date.now()-interaction.createdTimestamp}ms`);

  // --- Slash Commands ---
  if (interaction.isChatInputCommand()) {
    const { commandName } = interaction;
    const ephemeral = isEphemeralCommand(commandName);

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
            const discordAvatar = target.displayAvatarURL({ extension: 'png', size: 128 });
            const embed = new EmbedBuilder()
                .setTitle(title)
                .setThumbnail(discordAvatar)
                .setDescription(description)
                .addFields(
                    { name: 'Badges', value: badgesVal, inline: false },
                    { name: 'Streak', value: streakVal, inline: true  },
                    { name: 'Posts',  value: postsVal,  inline: false },
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
          return safeReply(interaction, { content: `✅ Battledome updates will post to <#${chan.id}>.`, ephemeral: true });
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

          if (action === 'clear') {
            await ref.remove();
            return safeReply(interaction, { content: '🔕 Unsubscribed from all Battledome alerts.', ephemeral: true });
          }

          if (!region) return safeReply(interaction, { content: 'Please specify a region.', ephemeral: true });

          if (action === 'unsub') {
             await ref.child(`regions/${region}`).remove();
             return safeReply(interaction, { content: `🔕 Unsubscribed from **${region}** alerts.`, ephemeral: true });
          } else {
             // Subscribe
             const updateData = {
                 enabled: true,
                 // State is managed by the poller; do not reset it here to avoid double-pings
             };
             
             // Only set threshold if provided; otherwise preserve existing (or undefined)
             if (threshold) {
                 updateData.threshold = threshold;
             }
             // If user wants to clear threshold, they can unsubscribe/resubscribe or we could add a clear option later.
             // Per instructions, we do not overwrite with null if missing.

             await ref.child(`regions/${region}`).update(updateData);
             await ref.child('updatedAt').set(admin.database.ServerValue.TIMESTAMP);
             
             const msg = threshold
                ? `🔔 Subscribed to **${region}**! You will be notified when player count reaches **${threshold}+**.`
                : `🔔 Subscribed to **${region}** join alerts! (All named joins)`;
             
             return safeReply(interaction, { content: msg, ephemeral: true });
          }
        }
        // Handle /battledomelb
        else if (commandName === 'battledomelb') {
          await safeDefer(interaction);
          const region = interaction.options.getString('region');
          const serverConfig = BD_SERVERS[region];
          if (!serverConfig) return safeReply(interaction, { content: 'Unknown region.', ephemeral: true });

          let info;
          try {
            info = await fetchBdInfo(serverConfig.url);
          } catch (e) {
            return safeReply(interaction, { content: `Failed to fetch leaderboard: ${e.message}`, ephemeral: true });
          }

          if (!info || !info.players || info.players.length === 0) {
             return safeReply(interaction, { content: `No players currently on **${serverConfig.name}**.`, ephemeral: true });
          }

          const top15 = info.players.slice(0, 15);
          const lines = top15.map((p, i) => {
             const idle = p.inactive && p.inactive > 60 ? ` *(idle ${p.inactive}s)*` : '';
             return `**${i+1}. ${p.name}** — ${p.score}${idle}`;
          }).join('\n');

          const embed = new EmbedBuilder()
            .setTitle(`Battledome Leaderboard — ${serverConfig.name}`)
            .setDescription(lines)
            .setFooter({ text: `Live snapshot • ${new Date().toLocaleTimeString('en-GB')}` })
            .setColor(DEFAULT_EMBED_COLOR);
          
          return safeReply(interaction, { embeds: [embed] });
        }

    } catch (err) {
        console.error(`[${commandName}]`, err);
        await safeReply(interaction, { 
            content: '❌ Sorry — something went wrong while processing your request.', 
            ephemeral: true 
        });
    }
    // --- END PATCH (Step 2/4) ---
  } 
  // --- Button Handlers ---
  else if (interaction.isButton()) {
    const id = interaction.customId;
    // Handle multi‑page /help navigation buttons. Custom IDs are of the form
    // help:<prev|next>:<parentId>. Only the original invoker may use these controls.
    if (id && id.startsWith('help:')) {
      const parts = id.split(':');
      const dir = parts[1];
      const parentId = parts[2];
      const cache = interaction.client.helpCache?.get(parentId);
      if (!cache) {
        return safeReply(interaction, { content: 'This help message has expired. Run `/help` again.', ephemeral: true });
      }
      if (interaction.user.id !== cache.userId) {
        return safeReply(interaction, { content: 'Only the person who ran this command can use these controls.', ephemeral: true });
      }
      // Compute new page index
      let newIndex = cache.index;
      if (dir === 'next') newIndex += 1;
      else if (dir === 'prev') newIndex -= 1;
      newIndex = Math.max(0, Math.min(cache.pages.length - 1, newIndex));
      cache.index = newIndex;
      const embedHelp = buildHelpEmbed(cache.pages[newIndex]);
      const prevDisabled = newIndex === 0;
      const nextDisabled = newIndex === cache.pages.length - 1;
      const prevBtn2 = new ButtonBuilder()
        .setCustomId(`help:prev:${parentId}`)
        .setLabel('◀')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(prevDisabled);
      const nextBtn2 = new ButtonBuilder()
        .setCustomId(`help:next:${parentId}`)
        .setLabel('▶')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(nextDisabled);
      const rowHelp = new ActionRowBuilder().addComponents(prevBtn2, nextBtn2);
      await safeDefer(interaction, { intent: 'update' });
      // When updating an existing message, omit the `ephemeral` flag; the interaction
      // is already replied to.
      return safeReply(interaction, { embeds: [embedHelp], components: [rowHelp] });
    }
    try {
      // For clan battle controls (cb: prefix)
      if (id.startsWith('cb:')) {
        const parts = id.split(':');
        // cb:<action>:<parentId>:<param>
        const action = parts[1];
        const parentId = parts[2];
        const param = parts[3];
        // Restrict interactions to the user who ran the command
        const invokerId = interaction.message?.interaction?.user?.id;
        if (invokerId && invokerId !== interaction.user.id) {
          return safeReply(interaction, { content: 'Only the person who ran this command can use these controls.', ephemeral: true });
        }
        // Fetch cache
        const cache = interaction.client.battleCache?.get(parentId);
        if (!cache) {
          return safeReply(interaction, { content: 'This battle list has expired. Run `/clanbattles` again.', ephemeral: true });
        }
        // Handle actions
        if (action === 'filter') {
          const newFilter = param;
          if (!['all','my','past'].includes(newFilter)) {
            return safeReply(interaction, { content: 'Invalid filter.', ephemeral: true });
          }
          cache.filter = newFilter;
          const list = cache.lists[newFilter] || [];
          // Build embed and rows
          const embed = buildBattlesListEmbed(list, newFilter, cache.clansData, cache.usersData);
          const rows = [];
          const max = Math.min(list.length, BATTLES_PAGE_SIZE);
          for (let i = 0; i < max; i += 5) {
            const row = new ActionRowBuilder();
            for (let j = i; j < Math.min(i + 5, max); j++) {
              row.addComponents(
                new ButtonBuilder()
                  .setCustomId(`cb:detail:${parentId}:${j}`)
                  .setLabel(String(j + 1))
                  .setStyle(ButtonStyle.Secondary)
              );
            }
            rows.push(row);
          }
          const filterRow = new ActionRowBuilder();
          ['all', 'my', 'past'].forEach(ft => {
            const label = ft === 'all' ? 'All' : (ft === 'my' ? 'My Clan' : 'Past');
            const style = ft === newFilter ? ButtonStyle.Primary : ButtonStyle.Secondary;
            filterRow.addComponents(
              new ButtonBuilder()
                .setCustomId(`cb:filter:${parentId}:${ft}`)
                .setLabel(label)
                .setStyle(style)
            );
          });
          rows.push(filterRow);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: rows });
        }
        else if (action === 'detail') {
          const idx = parseInt(param, 10);
          const filterType = cache.filter || 'all';
          const list = cache.lists[filterType] || [];
          if (isNaN(idx) || idx < 0 || idx >= list.length) {
            return safeReply(interaction, { content: 'Invalid selection.', ephemeral: true });
          }
          const [battleId, battle] = list[idx];
          // Build detail embed
          // Build detail embed (default view does not show extended description)
          const embed = buildBattleDetailEmbed(battleId, battle, cache.clansData, cache.usersData, /* includeDesc */ false);
          // Determine join eligibility
          let joinBtn;
          const canJoin = (
            battle.status !== 'finished' &&
            cache.uid && cache.userClanId &&
            (battle.challengerId === cache.userClanId || battle.targetId === cache.userClanId) &&
            !(battle.participants && battle.participants[cache.uid])
          );
          if (canJoin) {
            joinBtn = new ButtonBuilder()
              .setCustomId(`cb:join:${parentId}:${battleId}`)
              .setLabel('Join')
              .setStyle(ButtonStyle.Success);
          } else {
            // If already joined, show Leave button; otherwise disabled Join
            const joined = battle.participants && battle.participants[cache.uid];
            if (joined) {
              joinBtn = new ButtonBuilder()
                .setCustomId(`cb:leave:${parentId}:${battleId}`)
                .setLabel('Leave')
                .setStyle(ButtonStyle.Danger);
            } else {
              joinBtn = new ButtonBuilder()
                .setCustomId('cb:join:disabled')
                .setLabel('Join')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(true);
            }
          }
          // Info button to toggle extended description
          const infoBtn = new ButtonBuilder()
            .setCustomId(`cb:info:${parentId}:${battleId}:show`)
            .setLabel('Info')
            .setStyle(ButtonStyle.Secondary);
          const backBtn = new ButtonBuilder()
            .setCustomId(`cb:list:${parentId}`)
            .setLabel('Back')
            .setStyle(ButtonStyle.Secondary);
          const row = new ActionRowBuilder().addComponents(joinBtn, infoBtn, backBtn);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: [row] });
        }
        else if (action === 'list') {
          const filterType = cache.filter || 'all';
          const list = cache.lists[filterType] || [];
          const embed = buildBattlesListEmbed(list, filterType, cache.clansData, cache.usersData);
          const rows = [];
          const max = Math.min(list.length, BATTLES_PAGE_SIZE);
          for (let i = 0; i < max; i += 5) {
            const row = new ActionRowBuilder();
            for (let j = i; j < Math.min(i + 5, max); j++) {
              row.addComponents(
                new ButtonBuilder()
                  .setCustomId(`cb:detail:${parentId}:${j}`)
                  .setLabel(String(j + 1))
                  .setStyle(ButtonStyle.Secondary)
              );
            }
            rows.push(row);
          }
          const filterRow = new ActionRowBuilder();
          ['all', 'my', 'past'].forEach(ft => {
            const label = ft === 'all' ? 'All' : (ft === 'my' ? 'My Clan' : 'Past');
            const style = ft === filterType ? ButtonStyle.Primary : ButtonStyle.Secondary;
            filterRow.addComponents(
              new ButtonBuilder()
                .setCustomId(`cb:filter:${parentId}:${ft}`)
                .setLabel(label)
                .setStyle(style)
            );
          });
          rows.push(filterRow);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: rows });
        }
        else if (action === 'join') {
          const battleId = param;
          // Ensure user is eligible
          if (!cache.uid) {
            return safeReply(interaction, { content: 'Link your KC account with /link first to join battles.', ephemeral: true });
          }
          if (!cache.userClanId) {
            return safeReply(interaction, { content: 'You must be in a clan to join this battle.', ephemeral: true });
          }
          // Find battle in cache lists
          let battleRef = null;
          for (const k of Object.keys(cache.lists)) {
            const arr = cache.lists[k];
            for (let i = 0; i < arr.length; i++) {
              if (arr[i][0] === battleId) {
                battleRef = arr[i][1];
                break;
              }
            }
            if (battleRef) break;
          }
          if (!battleRef) {
            return safeReply(interaction, { content: 'Battle not found. It may have expired.', ephemeral: true });
          }
          // Check eligibility again based on battle data
          const inClan = (battleRef.challengerId === cache.userClanId || battleRef.targetId === cache.userClanId);
          const alreadyJoined = battleRef.participants && battleRef.participants[cache.uid];
          if (!inClan) {
            return safeReply(interaction, { content: 'You are not a member of either participating clan.', ephemeral: true });
          }
          if (battleRef.status === 'finished') {
            return safeReply(interaction, { content: 'This battle has already finished.', ephemeral: true });
          }
          if (alreadyJoined) {
            return safeReply(interaction, { content: 'You have already joined this battle.', ephemeral: true });
          }
          // Perform join in database
          try {
            await withTimeout(
              rtdb.ref(`battles/${battleId}/participants/${cache.uid}`).set(cache.userClanId),
              8000,
              `join battle ${battleId}`
            );
          } catch (err) {
            console.error('[join battle]', err);
            return safeReply(interaction, { content: 'Failed to join the battle. Please try again later.', ephemeral: true });
          }
          // Update local cache
          for (const k of Object.keys(cache.lists)) {
            const arr = cache.lists[k];
            for (let i = 0; i < arr.length; i++) {
              if (arr[i][0] === battleId) {
                arr[i][1].participants = arr[i][1].participants || {};
                arr[i][1].participants[cache.uid] = cache.userClanId;
              }
            }
          }
          // Build updated detail view (default view without description)
          const embed = buildBattleDetailEmbed(battleId, battleRef, cache.clansData, cache.usersData, /* includeDesc */ false);
          // Show a Leave button now that the user has joined
          const leaveBtn = new ButtonBuilder()
            .setCustomId(`cb:leave:${parentId}:${battleId}`)
            .setLabel('Leave')
            .setStyle(ButtonStyle.Danger);
          // Info button remains available to toggle description
          const infoBtn2 = new ButtonBuilder()
            .setCustomId(`cb:info:${parentId}:${battleId}:show`)
            .setLabel('Info')
            .setStyle(ButtonStyle.Secondary);
          const backBtn2 = new ButtonBuilder()
            .setCustomId(`cb:list:${parentId}`)
            .setLabel('Back')
            .setStyle(ButtonStyle.Secondary);
          const row = new ActionRowBuilder().addComponents(leaveBtn, infoBtn2, backBtn2);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: [row] });
        }
        else if (action === 'leave') {
          const battleId = param;
          // Ensure user is linked and in a clan
          if (!cache.uid) {
            return safeReply(interaction, { content: 'Link your KC account with /link first to leave battles.', ephemeral: true });
          }
          if (!cache.userClanId) {
            return safeReply(interaction, { content: 'You must be in a clan to leave this battle.', ephemeral: true });
          }
          // Find battle reference in cache
          let battleRef = null;
          for (const k of Object.keys(cache.lists)) {
            const arr = cache.lists[k];
            for (let i = 0; i < arr.length; i++) {
              if (arr[i][0] === battleId) {
                battleRef = arr[i][1];
                break;
              }
            }
            if (battleRef) break;
          }
          if (!battleRef) {
            return safeReply(interaction, { content: 'Battle not found. It may have expired.', ephemeral: true });
          }
          const joined = battleRef.participants && battleRef.participants[cache.uid];
          if (!joined) {
            return safeReply(interaction, { content: 'You are not currently joined in this battle.', ephemeral: true });
          }
          // Remove from DB
          try {
            await withTimeout(
              rtdb.ref(`battles/${battleId}/participants/${cache.uid}`).remove(),
              8000,
              `leave battle ${battleId}`
            );
          } catch (err) {
            console.error('[leave battle]', err);
            return safeReply(interaction, { content: 'Failed to leave the battle. Please try again later.', ephemeral: true });
          }
          // Update local cache
          for (const k of Object.keys(cache.lists)) {
            const arr = cache.lists[k];
            for (let i = 0; i < arr.length; i++) {
              if (arr[i][0] === battleId) {
                if (arr[i][1].participants) {
                  delete arr[i][1].participants[cache.uid];
                }
              }
            }
          }
          // Build updated detail view
          const embed = buildBattleDetailEmbed(battleId, battleRef, cache.clansData, cache.usersData, /* includeDesc */ false);
          // Determine new join/leave button
          let btn;
          const canJoinAgain = (
            battleRef.status !== 'finished' &&
            cache.uid && cache.userClanId &&
            (battleRef.challengerId === cache.userClanId || battleRef.targetId === cache.userClanId) &&
            !(battleRef.participants && battleRef.participants[cache.uid])
          );
          if (canJoinAgain) {
            btn = new ButtonBuilder()
              .setCustomId(`cb:join:${parentId}:${battleId}`)
              .setLabel('Join')
              .setStyle(ButtonStyle.Success);
          } else {
            btn = new ButtonBuilder()
              .setCustomId('cb:join:disabled')
              .setLabel('Join')
              .setStyle(ButtonStyle.Secondary)
              .setDisabled(true);
          }
          const infoBtn2 = new ButtonBuilder()
            .setCustomId(`cb:info:${parentId}:${battleId}:show`)
            .setLabel('Info')
            .setStyle(ButtonStyle.Secondary);
          const backBtn2 = new ButtonBuilder()
            .setCustomId(`cb:list:${parentId}`)
            .setLabel('Back')
            .setStyle(ButtonStyle.Secondary);
          const row2 = new ActionRowBuilder().addComponents(btn, infoBtn2, backBtn2);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: [row2] });
        }
        else if (action === 'info') {
          // Toggle additional description view for a battle
          // Expected customId format: cb:info:<parentId>:<battleId>:<mode>
          const battleId = param;
          const mode = parts[4] || 'show';
          // Ensure we still have cache and battle reference
          const battleLists = cache.lists;
          let battleRef = null;
          for (const k of Object.keys(battleLists)) {
            for (const entry of battleLists[k]) {
              if (entry[0] === battleId) {
                battleRef = entry[1];
                break;
              }
            }
            if (battleRef) break;
          }
          if (!battleRef) {
            return safeReply(interaction, { content: 'Battle not found. It may have expired.', ephemeral: true });
          }
          // Determine whether to show extended description
          const includeDesc = mode === 'show';
          // Build embed with or without description
          const embed = buildBattleDetailEmbed(battleId, battleRef, cache.clansData, cache.usersData, includeDesc);
          // Determine join eligibility (unchanged)
          let joinBtn;
          const canJoin = (
            battleRef.status !== 'finished' &&
            cache.uid && cache.userClanId &&
            (battleRef.challengerId === cache.userClanId || battleRef.targetId === cache.userClanId) &&
            !(battleRef.participants && battleRef.participants[cache.uid])
          );
          if (canJoin) {
            joinBtn = new ButtonBuilder()
              .setCustomId(`cb:join:${parentId}:${battleId}`)
              .setLabel('Join')
              .setStyle(ButtonStyle.Success);
          } else {
            // Already joined or not eligible
            const joined = battleRef.participants && battleRef.participants[cache.uid];
            const label = joined ? 'Joined' : 'Join';
            joinBtn = new ButtonBuilder()
              .setCustomId('cb:join:disabled')
              .setLabel(label)
              .setStyle(ButtonStyle.Secondary)
              .setDisabled(true);
          }
          // Toggle Info button
          const nextMode = includeDesc ? 'hide' : 'show';
          const infoLabel = includeDesc ? 'Hide Info' : 'Info';
          const infoBtn = new ButtonBuilder()
            .setCustomId(`cb:info:${parentId}:${battleId}:${nextMode}`)
            .setLabel(infoLabel)
            .setStyle(ButtonStyle.Secondary);
          // Back button to list
          const backBtn3 = new ButtonBuilder()
            .setCustomId(`cb:list:${parentId}`)
            .setLabel('Back')
            .setStyle(ButtonStyle.Secondary);
          const row = new ActionRowBuilder().addComponents(joinBtn, infoBtn, backBtn3);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: [row] });
        }
        // Unknown cb action falls through
      }
      // Handle clan challenge accept/decline buttons (cc: prefix)
      else if (id.startsWith('cc:')) {
        const parts = id.split(':');
        // cc:<action>:<parentId>:<battleId>
        const action = parts[1];
        const parentId = parts[2];
        const battleId = parts[3];
        // Restrict to the user who invoked the command
        const invokerId = interaction.message?.interaction?.user?.id;
        if (invokerId && invokerId !== interaction.user.id) {
          return safeReply(interaction, { content: 'Only the person who ran this command can use these controls.', ephemeral: true });
        }
        // Retrieve the cached list
        const cache = interaction.client.challengeCache?.get(parentId);
        if (!cache) {
          return safeReply(interaction, { content: 'This challenge list has expired. Run `/incomingchallenges` again.', ephemeral: true });
        }
        // Ensure this battle is still pending
        const idx = cache.pendingList.findIndex(([bid]) => bid === battleId);
        if (idx < 0) {
          return safeReply(interaction, { content: 'Challenge not found. It may have been updated.', ephemeral: true });
        }
        const [bid, battle] = cache.pendingList[idx];
        // Confirm the user is still the owner of their clan
        let uid;
        try {
          uid = await getKCUidForDiscord(interaction.user.id);
        } catch (_) { uid = null; }
        if (!uid) {
          return safeReply(interaction, { content: 'Link your KC account first with /link.', ephemeral: true });
        }
        // Reload clan to check owner
        const clanSnap = await rtdb.ref(`clans/${cache.clanId}`).get();
        const clan = clanSnap.exists() ? clanSnap.val() || {} : {};
        if (getOwnerUid(clan) !== uid) {
          return safeReply(interaction, { content: 'You must be a Clan Owner to run this command!', ephemeral: true });
        }
        // Update battle status
        try {
          if (action === 'accept') {
            await rtdb.ref(`battles/${battleId}`).update({ status: 'accepted', acceptedBy: uid, acceptedAt: admin.database.ServerValue.TIMESTAMP });
          } else if (action === 'decline') {
            await rtdb.ref(`battles/${battleId}`).update({ status: 'declined', declinedBy: uid, declinedAt: admin.database.ServerValue.TIMESTAMP });
          } else {
            return safeReply(interaction, { content: 'Unknown action.', ephemeral: true });
          }
        } catch (e) {
          console.error('[cc action] failed to update battle:', e);
          return safeReply(interaction, { content: 'Failed to update the challenge. Try again later.', ephemeral: true });
        }
        // Remove from cache list
        cache.pendingList.splice(idx, 1);
        // If no more pending, clear message
        if (cache.pendingList.length === 0) {
          interaction.client.challengeCache.delete(parentId);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { content: 'All challenges processed.', embeds: [], components: [] });
        }
        // Otherwise, rebuild embed and rows
        const clansSnap = await rtdb.ref('clans').get();
        const clansData = clansSnap.exists() ? clansSnap.val() || {} : {};
        const embed = new EmbedBuilder()
          .setTitle('Incoming Clan Challenges')
          .setDescription('Below are the pending clan battle challenges. Use the buttons to accept or decline.')
          .setColor(DEFAULT_EMBED_COLOR)
          .setFooter({ text: 'KC Bot • /incomingchallenges' });
        cache.pendingList.slice(0, 10).forEach(([bid2, b2], idx2) => {
          const c1 = clansData[b2.challengerId] || {};
          const c2 = clansData[b2.targetId] || {};
          const d = b2.scheduledTime ? new Date(b2.scheduledTime) : null;
          const dateStr = d ? d.toLocaleDateString('en-GB', { timeZone: 'Europe/London' }) : 'N/A';
          const timeStr = d ? d.toLocaleTimeString('en-GB', { timeZone: 'Europe/London', hour: '2-digit', minute: '2-digit' }) : '';
          const title = `${c1.name || b2.challengerId} vs ${c2.name || b2.targetId}`;
          const lines = [];
          if (b2.server) lines.push(`Server: ${b2.server}`);
          if (b2.scheduledTime) lines.push(`Date: ${dateStr} ${timeStr}`);
          if (b2.rules) lines.push(`Rules: ${b2.rules}`);
          embed.addFields({ name: `${idx2 + 1}. ${title}`, value: lines.join('\n') || '\u200b' });
        });
        const rows = [];
        cache.pendingList.slice(0, 10).forEach(([bid2], idx2) => {
          const row = new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId(`cc:accept:${parentId}:${bid2}`).setLabel('Accept').setStyle(ButtonStyle.Success),
            new ButtonBuilder().setCustomId(`cc:decline:${parentId}:${bid2}`).setLabel('Decline').setStyle(ButtonStyle.Danger),
          );
          rows.push(row);
        });
        // Update the message
        await safeDefer(interaction, { intent: 'update' });
        return safeReply(interaction, { embeds: [embed], components: rows });
      }

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
  }
  // --- Select Menu Handlers ---
  else if (interaction.isStringSelectMenu()) {
    const [prefix, parentInteractionId] = interaction.customId.split(':');
  
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
          const region = s.region || BD_NAME_OVERRIDES[s.name] || "west";
          const url = BD_SERVERS[region]?.url || s.url;

          if (!url) {
            return safeReply(interaction, { content: `No URL configured for region: ${region}`, ephemeral: true });
          }
      
          let info;
          try {
            info = await fetchBdInfo(url);
          } catch (e) {
            return safeReply(interaction, {
              content: `Couldn’t load server info for **${s.name || 'Unknown'}**.\nURL: ${url}\nError: ${e.message}`,
              ephemeral: true
            });
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
      
          const embed = new EmbedBuilder()
            .setTitle(info?.name ? `Battledome — ${info.name}` : `Battledome — ${s.name || 'Server'}`)
            .addFields(
              { name: 'Online now', value: String(info?.onlinenow ?? '—'), inline: true },
              { name: 'In dome now', value: String(info?.indomenow ?? '—'), inline: true },
              { name: 'Players this hour', value: String(info?.thishour ?? '—'), inline: true },
              { name: `Players (${players.length})`, value: lines.length ? lines.join('\n') : '_No players listed._' }
            )
            .setColor(DEFAULT_EMBED_COLOR)
            .setFooter({ text: 'KC Bot • /battledome' });
      
          return safeReply(interaction, { embeds: [embed], components: interaction.message.components });
      } catch (err) {
        console.error(`[select:${prefix}]`, err);
        await safeReply(interaction, { 
            content: '❌ Sorry, something went wrong.', 
            ephemeral: true, embeds: [], components: [] 
        });
      }
    }
  }
});


// ---------- Interaction handling ----------
let clientReady = false;
const MAX_AGE_MS = 15000; // be generous; we’ll still try to ack
client.on('interactionCreate', async (interaction) => {
  if (!clientReady) {
    try {
      // Use safeReply for consistency, though reply is fine here
      await safeReply(interaction, { content: 'Starting up, try again in a second.', ephemeral: true });
    } catch {}
    return;
  }
  const age = Date.now() - interaction.createdTimestamp;
  if (age > MAX_AGE_MS) {
    console.warn(`[old interaction ~${age}ms] attempting to acknowledge anyway`);
  }
  
  const seen = globalThis.__seen ??= new Set();
  if (seen.has(interaction.id)) {
    console.warn(`[INT] duplicate seen: ${interaction.id}`);
    return;
  }
  seen.add(interaction.id);
  setTimeout(() => seen.delete(interaction.id), 60_000);

  console.log(`[INT] ${interaction.isChatInputCommand() ? interaction.commandName : interaction.customId} from ${interaction.user?.tag} age=${Date.now()-interaction.createdTimestamp}ms`);

  // --- Slash Commands ---
  if (interaction.isChatInputCommand()) {
    const { commandName } = interaction;
    const ephemeral = isEphemeralCommand(commandName);

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
            const discordAvatar = target.displayAvatarURL({ extension: 'png', size: 128 });
            const embed = new EmbedBuilder()
                .setTitle(title)
                .setThumbnail(discordAvatar)
                .setDescription(description)
                .addFields(
                    { name: 'Badges', value: badgesVal, inline: false },
                    { name: 'Streak', value: streakVal, inline: true  },
                    { name: 'Posts',  value: postsVal,  inline: false },
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
          return safeReply(interaction, { content: `✅ Battledome updates will post to <#${chan.id}>.`, ephemeral: true });
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

          if (action === 'clear') {
            await ref.remove();
            return safeReply(interaction, { content: '🔕 Unsubscribed from all Battledome alerts.', ephemeral: true });
          }

          if (!region) return safeReply(interaction, { content: 'Please specify a region.', ephemeral: true });

          if (action === 'unsub') {
             await ref.child(`regions/${region}`).remove();
             return safeReply(interaction, { content: `🔕 Unsubscribed from **${region}** alerts.`, ephemeral: true });
          } else {
             // Subscribe
             const updateData = {
                 enabled: true,
                 // State is managed by the poller; do not reset it here to avoid double-pings
             };
             
             // Only set threshold if provided; otherwise preserve existing (or undefined)
             if (threshold) {
                 updateData.threshold = threshold;
             }
             // If user wants to clear threshold, they can unsubscribe/resubscribe or we could add a clear option later.
             // Per instructions, we do not overwrite with null if missing.

             await ref.child(`regions/${region}`).update(updateData);
             await ref.child('updatedAt').set(admin.database.ServerValue.TIMESTAMP);
             
             const msg = threshold
                ? `🔔 Subscribed to **${region}**! You will be notified when player count reaches **${threshold}+**.`
                : `🔔 Subscribed to **${region}** join alerts! (All named joins)`;
             
             return safeReply(interaction, { content: msg, ephemeral: true });
          }
        }
        // Handle /battledomelb
        else if (commandName === 'battledomelb') {
          await safeDefer(interaction);
          const region = interaction.options.getString('region');
          const serverConfig = BD_SERVERS[region];
          if (!serverConfig) return safeReply(interaction, { content: 'Unknown region.', ephemeral: true });

          let info;
          try {
            info = await fetchBdInfo(serverConfig.url);
          } catch (e) {
            return safeReply(interaction, { content: `Failed to fetch leaderboard: ${e.message}`, ephemeral: true });
          }

          if (!info || !info.players || info.players.length === 0) {
             return safeReply(interaction, { content: `No players currently on **${serverConfig.name}**.`, ephemeral: true });
          }

          const top15 = info.players.slice(0, 15);
          const lines = top15.map((p, i) => {
             const idle = p.inactive && p.inactive > 60 ? ` *(idle ${p.inactive}s)*` : '';
             return `**${i+1}. ${p.name}** — ${p.score}${idle}`;
          }).join('\n');

          const embed = new EmbedBuilder()
            .setTitle(`Battledome Leaderboard — ${serverConfig.name}`)
            .setDescription(lines)
            .setFooter({ text: `Live snapshot • ${new Date().toLocaleTimeString('en-GB')}` })
            .setColor(DEFAULT_EMBED_COLOR);
          
          return safeReply(interaction, { embeds: [embed] });
        }

    } catch (err) {
        console.error(`[${commandName}]`, err);
        await safeReply(interaction, { 
            content: '❌ Sorry — something went wrong while processing your request.', 
            ephemeral: true 
        });
    }
    // --- END PATCH (Step 2/4) ---
  } 
  // --- Button Handlers ---
  else if (interaction.isButton()) {
    const id = interaction.customId;
    // Handle multi‑page /help navigation buttons. Custom IDs are of the form
    // help:<prev|next>:<parentId>. Only the original invoker may use these controls.
    if (id && id.startsWith('help:')) {
      const parts = id.split(':');
      const dir = parts[1];
      const parentId = parts[2];
      const cache = interaction.client.helpCache?.get(parentId);
      if (!cache) {
        return safeReply(interaction, { content: 'This help message has expired. Run `/help` again.', ephemeral: true });
      }
      if (interaction.user.id !== cache.userId) {
        return safeReply(interaction, { content: 'Only the person who ran this command can use these controls.', ephemeral: true });
      }
      // Compute new page index
      let newIndex = cache.index;
      if (dir === 'next') newIndex += 1;
      else if (dir === 'prev') newIndex -= 1;
      newIndex = Math.max(0, Math.min(cache.pages.length - 1, newIndex));
      cache.index = newIndex;
      const embedHelp = buildHelpEmbed(cache.pages[newIndex]);
      const prevDisabled = newIndex === 0;
      const nextDisabled = newIndex === cache.pages.length - 1;
      const prevBtn2 = new ButtonBuilder()
        .setCustomId(`help:prev:${parentId}`)
        .setLabel('◀')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(prevDisabled);
      const nextBtn2 = new ButtonBuilder()
        .setCustomId(`help:next:${parentId}`)
        .setLabel('▶')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(nextDisabled);
      const rowHelp = new ActionRowBuilder().addComponents(prevBtn2, nextBtn2);
      await safeDefer(interaction, { intent: 'update' });
      // When updating an existing message, omit the `ephemeral` flag; the interaction
      // is already replied to.
      return safeReply(interaction, { embeds: [embedHelp], components: [rowHelp] });
    }
    try {
      // For clan battle controls (cb: prefix)
      if (id.startsWith('cb:')) {
        const parts = id.split(':');
        // cb:<action>:<parentId>:<param>
        const action = parts[1];
        const parentId = parts[2];
        const param = parts[3];
        // Restrict interactions to the user who ran the command
        const invokerId = interaction.message?.interaction?.user?.id;
        if (invokerId && invokerId !== interaction.user.id) {
          return safeReply(interaction, { content: 'Only the person who ran this command can use these controls.', ephemeral: true });
        }
        // Fetch cache
        const cache = interaction.client.battleCache?.get(parentId);
        if (!cache) {
          return safeReply(interaction, { content: 'This battle list has expired. Run `/clanbattles` again.', ephemeral: true });
        }
        // Handle actions
        if (action === 'filter') {
          const newFilter = param;
          if (!['all','my','past'].includes(newFilter)) {
            return safeReply(interaction, { content: 'Invalid filter.', ephemeral: true });
          }
          cache.filter = newFilter;
          const list = cache.lists[newFilter] || [];
          // Build embed and rows
          const embed = buildBattlesListEmbed(list, newFilter, cache.clansData, cache.usersData);
          const rows = [];
          const max = Math.min(list.length, BATTLES_PAGE_SIZE);
          for (let i = 0; i < max; i += 5) {
            const row = new ActionRowBuilder();
            for (let j = i; j < Math.min(i + 5, max); j++) {
              row.addComponents(
                new ButtonBuilder()
                  .setCustomId(`cb:detail:${parentId}:${j}`)
                  .setLabel(String(j + 1))
                  .setStyle(ButtonStyle.Secondary)
              );
            }
            rows.push(row);
          }
          const filterRow = new ActionRowBuilder();
          ['all', 'my', 'past'].forEach(ft => {
            const label = ft === 'all' ? 'All' : (ft === 'my' ? 'My Clan' : 'Past');
            const style = ft === newFilter ? ButtonStyle.Primary : ButtonStyle.Secondary;
            filterRow.addComponents(
              new ButtonBuilder()
                .setCustomId(`cb:filter:${parentId}:${ft}`)
                .setLabel(label)
                .setStyle(style)
            );
          });
          rows.push(filterRow);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: rows });
        }
        else if (action === 'detail') {
          const idx = parseInt(param, 10);
          const filterType = cache.filter || 'all';
          const list = cache.lists[filterType] || [];
          if (isNaN(idx) || idx < 0 || idx >= list.length) {
            return safeReply(interaction, { content: 'Invalid selection.', ephemeral: true });
          }
          const [battleId, battle] = list[idx];
          // Build detail embed
          // Build detail embed (default view does not show extended description)
          const embed = buildBattleDetailEmbed(battleId, battle, cache.clansData, cache.usersData, /* includeDesc */ false);
          // Determine join eligibility
          let joinBtn;
          const canJoin = (
            battle.status !== 'finished' &&
            cache.uid && cache.userClanId &&
            (battle.challengerId === cache.userClanId || battle.targetId === cache.userClanId) &&
            !(battle.participants && battle.participants[cache.uid])
          );
          if (canJoin) {
            joinBtn = new ButtonBuilder()
              .setCustomId(`cb:join:${parentId}:${battleId}`)
              .setLabel('Join')
              .setStyle(ButtonStyle.Success);
          } else {
            // If already joined, show Leave button; otherwise disabled Join
            const joined = battle.participants && battle.participants[cache.uid];
            if (joined) {
              joinBtn = new ButtonBuilder()
                .setCustomId(`cb:leave:${parentId}:${battleId}`)
                .setLabel('Leave')
                .setStyle(ButtonStyle.Danger);
            } else {
              joinBtn = new ButtonBuilder()
                .setCustomId('cb:join:disabled')
                .setLabel('Join')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(true);
            }
          }
          // Info button to toggle extended description
          const infoBtn = new ButtonBuilder()
            .setCustomId(`cb:info:${parentId}:${battleId}:show`)
            .setLabel('Info')
            .setStyle(ButtonStyle.Secondary);
          const backBtn = new ButtonBuilder()
            .setCustomId(`cb:list:${parentId}`)
            .setLabel('Back')
            .setStyle(ButtonStyle.Secondary);
          const row = new ActionRowBuilder().addComponents(joinBtn, infoBtn, backBtn);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: [row] });
        }
        else if (action === 'list') {
          const filterType = cache.filter || 'all';
          const list = cache.lists[filterType] || [];
          const embed = buildBattlesListEmbed(list, filterType, cache.clansData, cache.usersData);
          const rows = [];
          const max = Math.min(list.length, BATTLES_PAGE_SIZE);
          for (let i = 0; i < max; i += 5) {
            const row = new ActionRowBuilder();
            for (let j = i; j < Math.min(i + 5, max); j++) {
              row.addComponents(
                new ButtonBuilder()
                  .setCustomId(`cb:detail:${parentId}:${j}`)
                  .setLabel(String(j + 1))
                  .setStyle(ButtonStyle.Secondary)
              );
            }
            rows.push(row);
          }
          const filterRow = new ActionRowBuilder();
          ['all', 'my', 'past'].forEach(ft => {
            const label = ft === 'all' ? 'All' : (ft === 'my' ? 'My Clan' : 'Past');
            const style = ft === filterType ? ButtonStyle.Primary : ButtonStyle.Secondary;
            filterRow.addComponents(
              new ButtonBuilder()
                .setCustomId(`cb:filter:${parentId}:${ft}`)
                .setLabel(label)
                .setStyle(style)
            );
          });
          rows.push(filterRow);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: rows });
        }
        else if (action === 'join') {
          const battleId = param;
          // Ensure user is eligible
          if (!cache.uid) {
            return safeReply(interaction, { content: 'Link your KC account with /link first to join battles.', ephemeral: true });
          }
          if (!cache.userClanId) {
            return safeReply(interaction, { content: 'You must be in a clan to join this battle.', ephemeral: true });
          }
          // Find battle in cache lists
          let battleRef = null;
          for (const k of Object.keys(cache.lists)) {
            const arr = cache.lists[k];
            for (let i = 0; i < arr.length; i++) {
              if (arr[i][0] === battleId) {
                battleRef = arr[i][1];
                break;
              }
            }
            if (battleRef) break;
          }
          if (!battleRef) {
            return safeReply(interaction, { content: 'Battle not found. It may have expired.', ephemeral: true });
          }
          // Check eligibility again based on battle data
          const inClan = (battleRef.challengerId === cache.userClanId || battleRef.targetId === cache.userClanId);
          const alreadyJoined = battleRef.participants && battleRef.participants[cache.uid];
          if (!inClan) {
            return safeReply(interaction, { content: 'You are not a member of either participating clan.', ephemeral: true });
          }
          if (battleRef.status === 'finished') {
            return safeReply(interaction, { content: 'This battle has already finished.', ephemeral: true });
          }
          if (alreadyJoined) {
            return safeReply(interaction, { content: 'You have already joined this battle.', ephemeral: true });
          }
          // Perform join in database
          try {
            await withTimeout(
              rtdb.ref(`battles/${battleId}/participants/${cache.uid}`).set(cache.userClanId),
              8000,
              `join battle ${battleId}`
            );
          } catch (err) {
            console.error('[join battle]', err);
            return safeReply(interaction, { content: 'Failed to join the battle. Please try again later.', ephemeral: true });
          }
          // Update local cache
          for (const k of Object.keys(cache.lists)) {
            const arr = cache.lists[k];
            for (let i = 0; i < arr.length; i++) {
              if (arr[i][0] === battleId) {
                arr[i][1].participants = arr[i][1].participants || {};
                arr[i][1].participants[cache.uid] = cache.userClanId;
              }
            }
          }
          // Build updated detail view (default view without description)
          const embed = buildBattleDetailEmbed(battleId, battleRef, cache.clansData, cache.usersData, /* includeDesc */ false);
          // Show a Leave button now that the user has joined
          const leaveBtn = new ButtonBuilder()
            .setCustomId(`cb:leave:${parentId}:${battleId}`)
            .setLabel('Leave')
            .setStyle(ButtonStyle.Danger);
          // Info button remains available to toggle description
          const infoBtn2 = new ButtonBuilder()
            .setCustomId(`cb:info:${parentId}:${battleId}:show`)
            .setLabel('Info')
            .setStyle(ButtonStyle.Secondary);
          const backBtn2 = new ButtonBuilder()
            .setCustomId(`cb:list:${parentId}`)
            .setLabel('Back')
            .setStyle(ButtonStyle.Secondary);
          const row = new ActionRowBuilder().addComponents(leaveBtn, infoBtn2, backBtn2);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: [row] });
        }
        else if (action === 'leave') {
          const battleId = param;
          // Ensure user is linked and in a clan
          if (!cache.uid) {
            return safeReply(interaction, { content: 'Link your KC account with /link first to leave battles.', ephemeral: true });
          }
          if (!cache.userClanId) {
            return safeReply(interaction, { content: 'You must be in a clan to leave this battle.', ephemeral: true });
          }
          // Find battle reference in cache
          let battleRef = null;
          for (const k of Object.keys(cache.lists)) {
            const arr = cache.lists[k];
            for (let i = 0; i < arr.length; i++) {
              if (arr[i][0] === battleId) {
                battleRef = arr[i][1];
                break;
              }
            }
            if (battleRef) break;
          }
          if (!battleRef) {
            return safeReply(interaction, { content: 'Battle not found. It may have expired.', ephemeral: true });
          }
          const joined = battleRef.participants && battleRef.participants[cache.uid];
          if (!joined) {
            return safeReply(interaction, { content: 'You are not currently joined in this battle.', ephemeral: true });
          }
          // Remove from DB
          try {
            await withTimeout(
              rtdb.ref(`battles/${battleId}/participants/${cache.uid}`).remove(),
              8000,
              `leave battle ${battleId}`
            );
          } catch (err) {
            console.error('[leave battle]', err);
            return safeReply(interaction, { content: 'Failed to leave the battle. Please try again later.', ephemeral: true });
          }
          // Update local cache
          for (const k of Object.keys(cache.lists)) {
            const arr = cache.lists[k];
            for (let i = 0; i < arr.length; i++) {
              if (arr[i][0] === battleId) {
                if (arr[i][1].participants) {
                  delete arr[i][1].participants[cache.uid];
                }
              }
            }
          }
          // Build updated detail view
          const embed = buildBattleDetailEmbed(battleId, battleRef, cache.clansData, cache.usersData, /* includeDesc */ false);
          // Determine new join/leave button
          let btn;
          const canJoinAgain = (
            battleRef.status !== 'finished' &&
            cache.uid && cache.userClanId &&
            (battleRef.challengerId === cache.userClanId || battleRef.targetId === cache.userClanId) &&
            !(battleRef.participants && battleRef.participants[cache.uid])
          );
          if (canJoinAgain) {
            btn = new ButtonBuilder()
              .setCustomId(`cb:join:${parentId}:${battleId}`)
              .setLabel('Join')
              .setStyle(ButtonStyle.Success);
          } else {
            btn = new ButtonBuilder()
              .setCustomId('cb:join:disabled')
              .setLabel('Join')
              .setStyle(ButtonStyle.Secondary)
              .setDisabled(true);
          }
          const infoBtn2 = new ButtonBuilder()
            .setCustomId(`cb:info:${parentId}:${battleId}:show`)
            .setLabel('Info')
            .setStyle(ButtonStyle.Secondary);
          const backBtn2 = new ButtonBuilder()
            .setCustomId(`cb:list:${parentId}`)
            .setLabel('Back')
            .setStyle(ButtonStyle.Secondary);
          const row2 = new ActionRowBuilder().addComponents(btn, infoBtn2, backBtn2);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: [row2] });
        }
        else if (action === 'info') {
          // Toggle additional description view for a battle
          // Expected customId format: cb:info:<parentId>:<battleId>:<mode>
          const battleId = param;
          const mode = parts[4] || 'show';
          // Ensure we still have cache and battle reference
          const battleLists = cache.lists;
          let battleRef = null;
          for (const k of Object.keys(battleLists)) {
            for (const entry of battleLists[k]) {
              if (entry[0] === battleId) {
                battleRef = entry[1];
                break;
              }
            }
            if (battleRef) break;
          }
          if (!battleRef) {
            return safeReply(interaction, { content: 'Battle not found. It may have expired.', ephemeral: true });
          }
          // Determine whether to show extended description
          const includeDesc = mode === 'show';
          // Build embed with or without description
          const embed = buildBattleDetailEmbed(battleId, battleRef, cache.clansData, cache.usersData, includeDesc);
          // Determine join eligibility (unchanged)
          let joinBtn;
          const canJoin = (
            battleRef.status !== 'finished' &&
            cache.uid && cache.userClanId &&
            (battleRef.challengerId === cache.userClanId || battleRef.targetId === cache.userClanId) &&
            !(battleRef.participants && battleRef.participants[cache.uid])
          );
          if (canJoin) {
            joinBtn = new ButtonBuilder()
              .setCustomId(`cb:join:${parentId}:${battleId}`)
              .setLabel('Join')
              .setStyle(ButtonStyle.Success);
          } else {
            // Already joined or not eligible
            const joined = battleRef.participants && battleRef.participants[cache.uid];
            const label = joined ? 'Joined' : 'Join';
            joinBtn = new ButtonBuilder()
              .setCustomId('cb:join:disabled')
              .setLabel(label)
              .setStyle(ButtonStyle.Secondary)
              .setDisabled(true);
          }
          // Toggle Info button
          const nextMode = includeDesc ? 'hide' : 'show';
          const infoLabel = includeDesc ? 'Hide Info' : 'Info';
          const infoBtn = new ButtonBuilder()
            .setCustomId(`cb:info:${parentId}:${battleId}:${nextMode}`)
            .setLabel(infoLabel)
            .setStyle(ButtonStyle.Secondary);
          // Back button to list
          const backBtn3 = new ButtonBuilder()
            .setCustomId(`cb:list:${parentId}`)
            .setLabel('Back')
            .setStyle(ButtonStyle.Secondary);
          const row = new ActionRowBuilder().addComponents(joinBtn, infoBtn, backBtn3);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { embeds: [embed], components: [row] });
        }
        // Unknown cb action falls through
      }
      // Handle clan challenge accept/decline buttons (cc: prefix)
      else if (id.startsWith('cc:')) {
        const parts = id.split(':');
        // cc:<action>:<parentId>:<battleId>
        const action = parts[1];
        const parentId = parts[2];
        const battleId = parts[3];
        // Restrict to the user who invoked the command
        const invokerId = interaction.message?.interaction?.user?.id;
        if (invokerId && invokerId !== interaction.user.id) {
          return safeReply(interaction, { content: 'Only the person who ran this command can use these controls.', ephemeral: true });
        }
        // Retrieve the cached list
        const cache = interaction.client.challengeCache?.get(parentId);
        if (!cache) {
          return safeReply(interaction, { content: 'This challenge list has expired. Run `/incomingchallenges` again.', ephemeral: true });
        }
        // Ensure this battle is still pending
        const idx = cache.pendingList.findIndex(([bid]) => bid === battleId);
        if (idx < 0) {
          return safeReply(interaction, { content: 'Challenge not found. It may have been updated.', ephemeral: true });
        }
        const [bid, battle] = cache.pendingList[idx];
        // Confirm the user is still the owner of their clan
        let uid;
        try {
          uid = await getKCUidForDiscord(interaction.user.id);
        } catch (_) { uid = null; }
        if (!uid) {
          return safeReply(interaction, { content: 'Link your KC account first with /link.', ephemeral: true });
        }
        // Reload clan to check owner
        const clanSnap = await rtdb.ref(`clans/${cache.clanId}`).get();
        const clan = clanSnap.exists() ? clanSnap.val() || {} : {};
        if (getOwnerUid(clan) !== uid) {
          return safeReply(interaction, { content: 'You must be a Clan Owner to run this command!', ephemeral: true });
        }
        // Update battle status
        try {
          if (action === 'accept') {
            await rtdb.ref(`battles/${battleId}`).update({ status: 'accepted', acceptedBy: uid, acceptedAt: admin.database.ServerValue.TIMESTAMP });
          } else if (action === 'decline') {
            await rtdb.ref(`battles/${battleId}`).update({ status: 'declined', declinedBy: uid, declinedAt: admin.database.ServerValue.TIMESTAMP });
          } else {
            return safeReply(interaction, { content: 'Unknown action.', ephemeral: true });
          }
        } catch (e) {
          console.error('[cc action] failed to update battle:', e);
          return safeReply(interaction, { content: 'Failed to update the challenge. Try again later.', ephemeral: true });
        }
        // Remove from cache list
        cache.pendingList.splice(idx, 1);
        // If no more pending, clear message
        if (cache.pendingList.length === 0) {
          interaction.client.challengeCache.delete(parentId);
          await safeDefer(interaction, { intent: 'update' });
          return safeReply(interaction, { content: 'All challenges processed.', embeds: [], components: [] });
        }
        // Otherwise, rebuild embed and rows
        const clansSnap = await rtdb.ref('clans').get();
        const clansData = clansSnap.exists() ? clansSnap.val() || {} : {};
        const embed = new EmbedBuilder()
          .setTitle('Incoming Clan Challenges')
          .setDescription('Below are the pending clan battle challenges. Use the buttons to accept or decline.')
          .setColor(DEFAULT_EMBED_COLOR)
          .setFooter({ text: 'KC Bot • /incomingchallenges' });
        cache.pendingList.slice(0, 10).forEach(([bid2, b2], idx2) => {
          const c1 = clansData[b2.challengerId] || {};
          const c2 = clansData[b2.targetId] || {};
          const d = b2.scheduledTime ? new Date(b2.scheduledTime) : null;
          const dateStr = d ? d.toLocaleDateString('en-GB', { timeZone: 'Europe/London' }) : 'N/A';
          const timeStr = d ? d.toLocaleTimeString('en-GB', { timeZone: 'Europe/London', hour: '2-digit', minute: '2-digit' }) : '';
          const title = `${c1.name || b2.challengerId} vs ${c2.name || b2.targetId}`;
          const lines = [];
          if (b2.server) lines.push(`Server: ${b2.server}`);
          if (b2.scheduledTime) lines.push(`Date: ${dateStr} ${timeStr}`);
          if (b2.rules) lines.push(`Rules: ${b2.rules}`);
          embed.addFields({ name: `${idx2 + 1}. ${title}`, value: lines.join('\n') || '\u200b' });
        });
        const rows = [];
        cache.pendingList.slice(0, 10).forEach(([bid2], idx2) => {
          const row = new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId(`cc:accept:${parentId}:${bid2}`).setLabel('Accept').setStyle(ButtonStyle.Success),
            new ButtonBuilder().setCustomId(`cc:decline:${parentId}:${bid2}`).setLabel('Decline').setStyle(ButtonStyle.Danger),
          );
          rows.push(row);
        });
        // Update the message
        await safeDefer(interaction, { intent: 'update' });
        return safeReply(interaction, { embeds: [embed], components: rows });
      }

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
  }
});

async function maybeBroadcast(uid, postId, data) {
    if (!data || data.draft === true) return;
    if (data.publishAt && Date.now() < data.publishAt) return;
    if (!['youtube', 'tiktok'].includes(data.type)) return;

    // Check creation time. Don't broadcast old posts.
    const maxAge = parseInt(process.env.CLIP_BROADCAST_MAX_AGE_MS || '1800000', 10); // 30 mins
    const postAge = Date.now() - (data.createdAt || 0);
    if (!data.createdAt || postAge > maxAge) {
      // console.log(`[broadcast] skipping old post ${postId}, age=${postAge}ms`);
      return;
    }

    for (const guildId of globalCache.clipDestinations.keys()) {
        const channelId = globalCache.clipDestinations.get(guildId);
        if (!channelId) continue;

        const flagRef = rtdb.ref(`users/${uid}/posts/${postId}/postedToDiscord/${guildId}`);
        try {
            const tx = await flagRef.transaction(cur => {
                if (cur) return; // Abort if already posted or pending
                return { pending: true, by: client.user.id, at: Date.now() };
            });

            if (!tx.committed) {
                console.log(`[broadcast] clip ${postId} already posted to guild ${guildId}`);
                continue;
            }

            const item = { ownerUid: uid, postId, data };
            const nameMap = await getAllUserNames();
            
            const embed = buildClipDetailEmbed(item, nameMap);
            
            const channel = await client.channels.fetch(channelId).catch(() => null);
            if (!channel || !channel.isTextBased?.()) { await flagRef.remove(); continue; }
            
            const me = channel.guild?.members.me || await channel.guild?.members.fetchMe().catch(() => null);
            if (!me) { await flagRef.remove(); continue; }

            const ok = channel.permissionsFor(me).has([PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.EmbedLinks]);
            if (!ok) { await flagRef.remove(); continue; }

            const msg = await channel.send({ embeds: [embed] });
            const postPath = `users/${uid}/posts/${postId}`;
            const rows = clipsDetailRows(msg, postPath); // Pass the new message
            await msg.edit({ components: rows });

            await flagRef.set({
                messageId: msg.id,
                channelId: channel.id,
                by: client.user.id,
                at: admin.database.ServerValue.TIMESTAMP,
            });
            console.log(`[broadcast] successfully posted clip ${postId} to ${guildId}/${channel.id}`);

        } catch (e) {
            console.error(`[broadcast] failed to post clip ${postId} to guild ${guildId}:`, e);
            await flagRef.remove(); // Rollback
        }
    }
}


const listenedUids = new Set();
function attachPostsListener(uid) {
    if (!uid || listenedUids.has(uid)) return;
    listenedUids.add(uid);
    const ref = rtdb.ref(`users/${uid}/posts`);
    
    // Listen for *new* posts
    ref.orderByChild('createdAt').startAt(Date.now() - 5000).on('child_added', s => {
        // Check age again, 'child_added' can fire for old items on startup
        const data = s.val();
        const postAge = Date.now() - (data?.createdAt || 0);
        if (postAge < (parseInt(process.env.CLIP_BROADCAST_MAX_AGE_MS || '1800000', 10) + 10000)) { // 30m + 10s buffer
            maybeBroadcast(uid, s.key, data).catch(e => console.error(`[broadcast] unhandled error in maybeBroadcast for ${uid}/${s.key}`, e));
        }
    }, err => console.error(`[broadcast] listener error for ${uid}:`, err));
    
    // Listen for *updates* (e.g., draft -> published)
    ref.on('child_changed', s => {
        maybeBroadcast(uid, s.key, s.val()).catch(e => console.error(`[broadcast] unhandled error in maybeBroadcast (update) for ${uid}/${s.key}`, e));
    });

    console.log(`[broadcast] attached listener for user ${uid}`);
}

async function checkRegion(regionKey) {
  const config = BD_SERVERS[regionKey];
  if (!config) return;
  
  let info;
  try {
    info = await fetchBdInfo(config.url);
  } catch (e) {
    return;
  }
  if (!info) return;

  const state = bdState[regionKey];
  const currentNames = new Set(info.players.map(p => p.name));
  const currentOnline = info.onlinenow;

  // First run: just init state
  if (state.lastCheck === 0) {
    state.lastNames = currentNames;
    state.lastOnline = currentOnline;
    state.lastCheck = Date.now();
    return;
  }

  // Diff
  const joins = [];
  const leaves = [];
  
  for (const name of currentNames) {
    if (!state.lastNames.has(name)) joins.push(name);
  }
  for (const name of state.lastNames) {
    if (!currentNames.has(name)) leaves.push(name);
  }

  // Unnamed calc
  // Effective count of named players
  const namedCountPrev = state.lastNames.size;
  const namedCountCurr = currentNames.size;
  
  const unnamedPrev = Math.max(0, state.lastOnline - namedCountPrev);
  const unnamedCurr = Math.max(0, currentOnline - namedCountCurr);
  
  let unnamedDiff = unnamedCurr - unnamedPrev; 
  
  // Update state immediately
  state.lastNames = currentNames;
  state.lastOnline = currentOnline;
  state.lastCheck = Date.now();

  if (joins.length === 0 && leaves.length === 0 && unnamedDiff === 0) return;

  // Broadcast
  await broadcastBdUpdate(regionKey, { joins, leaves, unnamedDiff, info });
}

async function broadcastBdUpdate(regionKey, { joins, leaves, unnamedDiff, info }) {
  const serverName = BD_SERVERS[regionKey]?.name || regionKey;
  const onlineNow = info.onlinenow || 0;
  
  // Prepare embed content
  const fields = [];
  if (joins.length > 0) {
    fields.push({ name: '✅ Joined', value: joins.join(', ').slice(0, 1024) });
  }
  if (leaves.length > 0) {
    fields.push({ name: '❌ Left', value: leaves.join(', ').slice(0, 1024) });
  }
  if (unnamedDiff !== 0) {
    const verb = unnamedDiff > 0 ? 'Joined' : 'Left';
    const count = Math.abs(unnamedDiff);
    const emoji = unnamedDiff > 0 ? '✅' : '❌';
    fields.push({ name: `${emoji} ${verb} (Unnamed)`, value: `(${count > 0 ? '+' : ''}${unnamedDiff}) unnamed player${count > 1 ? 's' : ''}` });
  }

  const embed = new EmbedBuilder()
    .setTitle(`Battledome Update — ${serverName}`)
    .setDescription(`**Online:** ${info.onlinenow}  |  **In Dome:** ${info.indomenow ?? '?'}  |  **This Hour:** ${info.thishour ?? '?'}`)
    .addFields(fields)
    .setColor(joins.length > 0 || unnamedDiff > 0 ? 0x2ecc71 : 0xe74c3c) // Green for joins, Red for leaves
    .setFooter({ text: `Live snapshot • ${new Date().toLocaleTimeString('en-GB')}` });

  // Broadcast to all configured guilds
  for (const [guildId, channelId] of globalCache.bdDestinations.entries()) {
    try {
      const channel = await client.channels.fetch(channelId).catch(() => null);
      if (!channel || !channel.isTextBased?.()) continue;

      // Construct content with mentions
      let content = '';
      
      // Optimization: Only check subscribers if there's a potential trigger
      // Triggers happen on: 
      // 1. Named joins (legacy)
      // 2. Count change (threshold crossing up OR down for re-arm)
      const shouldCheckSubs = joins.length > 0 || unnamedDiff !== 0;

      if (shouldCheckSubs) {
        // Fetch subscribers for this guild/region
        const snap = await rtdb.ref(`bdNotify/${guildId}`).get();
        if (snap.exists()) {
            const triggeredUserIds = [];
            const stateUpdates = {}; // path -> value

            snap.forEach(child => {
            const userId = child.key;
            const val = child.val();
            const sub = val.regions?.[regionKey];
            
            if (!sub || !sub.enabled) return;

            const threshold = sub.threshold; // number or null
            const prevState = sub.state || 'below';
            
            // Logic: Check threshold crossing
            if (typeof threshold === 'number') {
                if (onlineNow >= threshold) {
                    // Currently Above
                    if (prevState !== 'above') {
                    // Crossing UP
                    // Only trigger if we have positive movement (joins or unnamed increase)
                    const isIncrease = (joins.length > 0) || (unnamedDiff > 0);
                    if (isIncrease) {
                        triggeredUserIds.push(userId);
                        stateUpdates[`bdNotify/${guildId}/${userId}/regions/${regionKey}/state`] = 'above';
                    }
                    }
                } else {
                    // Currently Below
                    if (prevState !== 'below') {
                    // Crossing DOWN (Re-arm)
                    stateUpdates[`bdNotify/${guildId}/${userId}/regions/${regionKey}/state`] = 'below';
                    }
                }
            } else {
                // No threshold: legacy join pings (only named joins)
                if (joins.length > 0) {
                    triggeredUserIds.push(userId);
                }
            }
            });

            // Apply state updates
            if (Object.keys(stateUpdates).length > 0) {
            // We do this asynchronously without awaiting to not block the broadcast loop too much
            rtdb.ref().update(stateUpdates).catch(e => console.error('State update failed', e));
            }

            if (triggeredUserIds.length > 0) {
            content = triggeredUserIds.map(id => `<@${id}>`).join(' ');
            }
        }
      }

      await channel.send({ content, embeds: [embed] });
    } catch (e) {
      console.error(`[BD Broadcast] Failed to send to guild ${guildId}:`, e.message);
    }
  }
}

// Polling Loop
async function pollBattledome() {
  while (true) {
    try {
      // Staggered checks
      await checkRegion('west');
      await new Promise(r => setTimeout(r, 5000));
      
      await checkRegion('east');
      await new Promise(r => setTimeout(r, 5000));
      
      await checkRegion('eu');
      await new Promise(r => setTimeout(r, 5000));

    } catch (e) {
      console.error('[BD Poller] Loop error:', e);
      await new Promise(r => setTimeout(r, 10000));
    }
  }
}

// ---------- Startup ----------
client.once('ready', async () => {
  console.log(`Logged in as ${client.user.tag}`);
  clientReady = true;
  
  // Load clip destinations
  try {
    const snap = await rtdb.ref('config/clipDestinations').get();
    globalCache.clipDestinations.clear();
    if (snap.exists()) {
      const all = snap.val() || {};
      for (const [guildId, cfg] of Object.entries(all)) {
        if (cfg?.channelId) globalCache.clipDestinations.set(guildId, cfg.channelId);
      }
    }
    console.log(`[broadcast] loaded initial clip destinations: ${globalCache.clipDestinations.size}`);

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
  } catch (e) {
    console.error('[broadcast] failed to load clip destinations', e);
  }

  // Load Battledome destinations (NEW)
  try {
    const snap = await rtdb.ref('config/bdDestinations').get();
    globalCache.bdDestinations.clear();
    if (snap.exists()) {
      const all = snap.val() || {};
      for (const [guildId, cfg] of Object.entries(all)) {
        if (cfg?.channelId) globalCache.bdDestinations.set(guildId, cfg.channelId);
      }
    }
    console.log(`[BD] loaded initial destinations: ${globalCache.bdDestinations.size}`);

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
  } catch (e) {
    console.error('[BD] failed to load destinations', e);
  }

  // Attach listeners for all existing and new users to broadcast clips
  try {
    const usersSnap = await rtdb.ref('users').get();
    if (usersSnap.exists()) {
        Object.keys(usersSnap.val()).forEach(attachPostsListener);
    }
    rtdb.ref('users').on('child_added', s => attachPostsListener(s.key));
  } catch (e) {
      console.error('[broadcast] failed to attach initial user listeners', e);
  }

  // Load clan battle/event destination channels and attach live updates
  try {
    const snap2 = await rtdb.ref('config/battleDestinations').get();
    globalCache.battleDestinations.clear();
    if (snap2.exists()) {
      const all = snap2.val() || {};
      for (const [guildId, cfg] of Object.entries(all)) {
        if (cfg?.channelId) globalCache.battleDestinations.set(guildId, cfg.channelId);
      }
    }
    console.log(`[battle broadcast] loaded initial battle destinations: ${globalCache.battleDestinations.size}`);
    // Listen for updates to battle destinations
    rtdb.ref('config/battleDestinations').on('child_added', s => {
      const v = s.val() || {};
      if (v.channelId) globalCache.battleDestinations.set(s.key, v.channelId);
    });
    rtdb.ref('config/battleDestinations').on('child_changed', s => {
      const v = s.val() || {};
      if (v.channelId) globalCache.battleDestinations.set(s.key, v.channelId);
      else globalCache.battleDestinations.delete(s.key);
    });
    rtdb.ref('config/battleDestinations').on('child_removed', s => {
      globalCache.battleDestinations.delete(s.key);
    });
  } catch (e) {
    console.error('[battle broadcast] failed to load battle destinations', e);
  }
  // Attach listener for clan battles to broadcast accepted battles
  try {
    const ref = rtdb.ref('battles');
    // Listen for new battles
    ref.orderByChild('createdAt').startAt(Date.now() - 5000).on('child_added', s => {
      const battle = s.val();
      maybeBroadcastBattle(s.key, battle).catch(e => console.error('[battle broadcast] add error', e));
    }, err => console.error('[battle broadcast] listener error (add)', err));
    // Listen for updates (e.g., pending -> accepted)
    ref.on('child_changed', s => {
      const battle = s.val();
      maybeBroadcastBattle(s.key, battle).catch(e => console.error('[battle broadcast] change error', e));
    }, err => console.error('[battle broadcast] listener error (change)', err));
  } catch (e) {
    console.error('[battle broadcast] failed to attach battle listeners', e);
  }
});

// Redundant listeners (already added at top)
// process.on('unhandledRejection', e => console.error('unhandledRejection', e));
// process.on('uncaughtException', e => console.error('uncaughtException', e));

(async () => {
  try {
    console.log('Registering slash commands…');
    await registerCommands();
    console.log('Slash command registration complete.');
  } catch (e) {
    console.error('registerCommands FAILED:', e?.rawError || e);
  }

  // Claim lock before client login
  // Start bot only after we own the lock
  async function startWhenLocked() {
    while (true) {
      try {
        const got = await claimBotLock();
        if (got) break;
        console.log('Standby — lock held by another instance. Retrying in 20s…');
      } catch (e) {
        console.warn('Lock claim error:', e.message);
      }
      await new Promise(r => setTimeout(r, 20_000));
    }
    console.log('Lock acquired — logging into Discord…');
    await client.login(process.env.DISCORD_BOT_TOKEN);
    
    // Start Battledome Poller only after acquiring lock and login
    pollBattledome().catch(e => console.error('BD Poller failed to start:', e));

    setInterval(() => renewBotLock().catch(() => {}), 30_000).unref();
  }

  startWhenLocked().catch(e => {
    console.error('Failed to start bot:', e);
    process.exit(1);
  });
})();
