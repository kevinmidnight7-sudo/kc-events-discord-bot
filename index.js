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

console.log('ENV sanity', {
  hasToken: !!process.env.DISCORD_BOT_TOKEN,
  hasClient: !!process.env.DISCORD_CLIENT_ID,
  hasGuild: !!process.env.DISCORD_GUILD_ID,          // optional
  hasDbUrl: !!process.env.FB_DATABASE_URL,
  hasSAJson: !!process.env.FB_SERVICE_ACCOUNT_JSON,
  hasSAPath: !!process.env.FB_SERVICE_ACCOUNT_PATH,
});


const {
  Client,
  GatewayIntentBits,
  Partials,
  SlashCommandBuilder,
  Routes,
  REST,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
} = require('discord.js');

const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

const EPHEMERAL_FLAG = 64; // Discord message flag for ephemeral

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

const DEFAULT_EMBED_COLOR = 0x2b2d31;

// Simple in-memory cache for frequently accessed, non-critical data
const globalCache = {
  userNames: { data: {}, fetchedAt: 0 },
};

const isEphemeralCommand = (name) => new Set([
  'whoami', 'dumpme', 'syncavatar', 'post', 'postmessage', 'link'
]).has(name);

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

// ---------- Helpers ----------
function isPermsError(err) { return err?.code === 50013; }

function isExpiredToken(err) {
  return (
    err?.code === 10062 || // Unknown interaction
    err?.code === 50027 || // Invalid Webhook Token
    err?.status === 401    // Unauthorized (same effect)
  );
}

function encPath(p){ return String(p).replace(/\//g, '|'); }
function decPath(s){ return String(s).replace(/\|/g, '/'); }

async function safeEdit(interaction, data, fallbackText = 'Reply expired. Please run the command again.') {
  try {
    if (interaction.deferred || interaction.replied) {
      return await interaction.editReply(data);
    } else {
      return await interaction.reply(data.flags ? data : { ...data, flags: EPHEMERAL_FLAG });
    }
  } catch (err) {
    if (isExpiredToken(err) || isPermsError(err)) {
      try {
        return await interaction.followUp(data.flags ? data : { ...data, flags: EPHEMERAL_FLAG });
      } catch {
        if (interaction.channel && fallbackText) {
          try { await interaction.channel.send(fallbackText); } catch {}
        }
      }
      return null;
    }
    throw err;
  }
}


// --- Promise timeout wrapper so we never hang indefinitely
function withTimeout(promise, ms, label = 'operation') {
  let t;
  const timeout = new Promise((_, rej) => {
    t = setTimeout(() => rej(new Error(`Timeout after ${ms}ms: ${label}`)), ms);
  });
  return Promise.race([
    promise.finally(() => clearTimeout(t)),
    timeout,
  ]);
}

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

async function postingUnlocked(uid) {
  // same idea as your site: emerald/diamond/content OR explicit flags
  try {
    const snap = await withTimeout(rtdb.ref(`users/${uid}`).get(), 4000, `RTDB users/${uid}`);
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
    const s = await withTimeout(rtdb.ref('config/postsDisabled').get(), 2000, 'RTDB config/postsDisabled');
    return !!s.val();
  } catch { return false; }
}

// ----- Shared helpers for new commands -----
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
  if (now - globalCache.userNames.fetchedAt < CACHE_DURATION) {
    return globalCache.userNames.data;
  }

  const snap = await withTimeout(rtdb.ref('users').get(), 4000, 'RTDB users');
  const out = {};
  if (snap.exists()) {
    const all = snap.val() || {};
    for (const uid of Object.keys(all)) {
      const u = all[uid] || {};
      out[uid] = u.displayName || u.email || '(unknown)';
    }
  }
  globalCache.userNames.data = out;
  globalCache.userNames.fetchedAt = now;
  return out;
}

// Gather all posts across users; return an array of {ownerUid, postId, data, score, reacts, comments}
async function fetchAllPosts({ platform = 'all' } = {}) {
  const usersSnap = await withTimeout(rtdb.ref('users').get(), 5000, 'RTDB users');
  const users = usersSnap.exists() ? usersSnap.val() : {};
  const results = [];
  const started = Date.now();
  let totalPostsSeen = 0;

  const tasks = Object.keys(users).map(async uid => {
    if (Date.now() - started > 4500 || totalPostsSeen > 500) return;
    const postsSnap = await withTimeout(rtdb.ref(`users/${uid}/posts`).get(), 4000, `RTDB users/${uid}/posts`);
    if (!postsSnap.exists()) return;

    postsSnap.forEach(p => {
      if (Date.now() - started > 4500 || totalPostsSeen > 500) return;
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

function snapshotToArray(snap) {
  const arr = [];
  if (snap?.exists()) {
    snap.forEach(c => arr.push({ key: c.key, ...(c.val() || {}) }));
    arr.sort((a, b) => (b.time || 0) - (a.time || 0));
  }
  return arr;
}

// Latest N messageboard messages (defensive: over-fetch then trim)
async function fetchLatestMessages(limit = 10) {
  const OVERFETCH = Math.max(limit * 3, 30); // grab more, sort, then trim
  try {
    const snap = await withTimeout(
      rtdb.ref('messages').orderByChild('time').limitToLast(OVERFETCH).get(),
      5000,
      'RTDB messages recent'
    );
    const arr = snapshotToArray(snap).slice(0, limit).map(m => ({
      ...m,
      path: `messages/${m.key}`,
    }));
    return arr;
  } catch (e) {
    if (String(e?.message || '').includes('Index not defined')) {
      console.warn('[fetchLatestMessages] Index missing, using unordered fallback.');
      const snap = await withTimeout(
        rtdb.ref('messages').limitToLast(OVERFETCH).get(),
        5000,
        'RTDB messages fallback'
      );
      // sort by time desc if present, else by key desc, then trim
      const arr = snapshotToArray(snap).slice(0, limit).map(m => ({
        ...m,
        path: `messages/${m.key}`,
      }));
      return arr;
    }
    throw e;
  }
}

// Build an embed showing a page of 10 messages (title, text, reply count)
function buildMessagesEmbed(list, userNames = {}) {
  const desc = list
    .map((m, i) => {
      const who = m.user || userNames[m.uid] || m.username || m.displayName || '(unknown)';
      const text = m.text?.length > 100 ? m.text.slice(0, 100) + '…' : m.text;
      const when = m.time ? new Date(m.time).toLocaleString() : '';
      return `**${i + 1}. ${who}** — _${when}_\n— ${text || '_no text_'}\nReplies: **${m.replies ? Object.keys(m.replies).length : 0}**`;
    })
    .join('\n\n');

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

function buildMessageDetailEmbed(msg, nameMap = {}) {
  const who = msg.user || nameMap[msg.uid] || msg.username || msg.displayName || '(unknown)';
  const likes = msg.likes || (msg.likedBy ? Object.keys(msg.likedBy).length : 0) || 0;
  const replies = msg.replies ? Object.keys(msg.replies).length : 0;

  const e = new EmbedBuilder()
    .setTitle(`${who}`)
    .setDescription((msg.text || '_no text_').toString().slice(0, 4096))
    .addFields(
      { name: 'Likes', value: String(likes), inline: true },
      { name: 'Replies', value: String(replies), inline: true },
    )
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: `Posted ${fmtTime(msg.time)} • KC Bot • /messages` });

  return e;
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
  const snap = await withTimeout(rtdb.ref(path).get(), 4000, `RTDB ${path}`);
  return snap.exists() ? { key: path.split('/').slice(-1)[0], path, ...(snap.val()||{}) } : null;
}

async function loadReplies(path) {
  const snap = await withTimeout(rtdb.ref(`${path}/replies`).get(), 4000, `RTDB ${path}/replies`);
  const list = [];
  if (snap.exists()) {
    snap.forEach(c => list.push({ key: c.key, path: `${path}/replies/${c.key}`, ...(c.val()||{}) }));
    list.sort((a,b)=>(b.time||0)-(a.time||0));
  }
  return list;
}

function buildThreadEmbed(parent, children, page=0, pageSize=10, nameMap={}) {
  const start = page*pageSize;
  const slice = children.slice(start, start+pageSize);
  const lines = slice.map((r,i)=>{
    const who = r.user || nameMap[r.uid] || r.username || r.displayName || '(unknown)';
    const txt = (r.text||'').toString().slice(0,120) || '(no text)';
    return `**${i+1}. ${who}** — ${txt}`;
  }).join('\n\n') || '_No replies yet_';

  return new EmbedBuilder()
    .setTitle(`Thread — ${nameMap[parent.uid]||parent.user||'(unknown)'}`)
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
  if (numRow.components.length) rows.push(numRow); // <- only add if non-empty

  rows.push(
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId(`msg:thread:${encPath(parentPath)}:${Math.max(page-1,0)}`).setLabel('◀ Page').setStyle(ButtonStyle.Secondary).setDisabled(page<=0),
      new ButtonBuilder().setCustomId(`msg:thread:${encPath(parentPath)}:${Math.min(page+1,maxPage)}`).setLabel('Page ▶').setStyle(ButtonStyle.Secondary).setDisabled(page>=maxPage),
      new ButtonBuilder().setCustomId(`msg:openPath:${encPath(parentPath)}`).setLabel('Back to message').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('msg:back').setLabel('Back to list').setStyle(ButtonStyle.Secondary),
    )
  );
  return rows;
}


// Votes -> scores
async function loadVoteScores() {
  const [cfgSnap, votesSnap] = await Promise.all([
    withTimeout(rtdb.ref('config/liveLeaderboardEnabled').get(), 3000, 'RTDB config'),
    withTimeout(rtdb.ref('votes').get(), 5000, 'RTDB votes'),
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

  const usersSnap = await withTimeout(rtdb.ref('users').get(), 4000, 'RTDB users');
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
    withTimeout(rtdb.ref('users').get(), 3000, 'RTDB users'),
    withTimeout(rtdb.ref('badges').get(), 3000, 'RTDB badges'),
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
    const snap = await withTimeout(
    rtdb.ref(`discordLinks/${discordId}`).get(),
    3000,
    `RTDB discordLinks/${discordId}`
  );
  if (!snap.exists()) return null;
  const { uid } = snap.val() || {};
  return uid || null;
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
    withTimeout(rtdb.ref(`users/${uid}`).get(), 3000, `RTDB users/${uid}`),
    withTimeout(rtdb.ref(`badges/${uid}`).get(), 3000, `RTDB badges/${uid}`),
    withTimeout(rtdb.ref(`users/${uid}/posts`).get(), 3000, `RTDB users/${uid}/posts`),
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
        3000,
        `FS users/${uid}`
      );
      if (fsUser.exists) user = fsUser.data() || {};
    } catch (e) { console.warn('FS user read timeout/fail:', e.message); }
  }
  if (!posts || Object.keys(posts).length === 0) {
    try {
      const fsPosts = await withTimeout(
        firestore.collection('users').doc(uid).collection('posts').get(),
        3000,
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
    const piece = [b.emoji, b.label].filter(Boolean).join(' ');
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


// ---------- Discord Client ----------
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
  ],
  partials: [Partials.Channel],
});

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

// Register (include it in commands array)
const commandsJson = [
  linkCmd, badgesCmd, whoamiCmd, dumpCmd, lbCmd, clipsCmd, messagesCmd, votingCmd,
  avatarCmd, postCmd, postMessageCmd
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
client.on('interactionCreate', async (interaction) => {
  // Log first (non-blocking)
  try {
    const tag = interaction.user?.tag ?? 'unknown';
    const name = interaction.isChatInputCommand() ? interaction.commandName : interaction.customId;
    console.log(`[INT] ${name} from ${tag} (${interaction.user?.id}) at ${new Date().toISOString()}`);
  } catch (_) {}

  // Acknowledge chat input commands fast (3-second rule)
  if (interaction.isChatInputCommand()) {
    // Decide if this command should be ephemeral
    const eph = new Set(['whoami','dumpme','syncavatar','post','postmessage','badges','messages','votingscores','leaderboard']).has(interaction.commandName);

    // Special case: /link — reply immediately (no defer), then return.
    if (interaction.commandName === 'link') {
      try {
        const start = process.env.AUTH_BRIDGE_START_URL;
        const url = `${start}?state=${encodeURIComponent(interaction.user.id)}`;
        await interaction.reply({ content: `Click to link your account: ${url}`, flags: EPHEMERAL_FLAG });
      } catch (err) {
        console.warn('[link reply failed]', err?.rawError || err);
      }
      return;
    }

    try {
      await interaction.deferReply(eph ? { ephemeral: true } : {});
      console.log(`[ACK] /${interaction.commandName} deferred`);
    } catch (err) {
      console.warn('[deferReply failed]', err?.rawError || err);
      return; // if we didn’t ack, stop to avoid “did not respond”
    }
  }

  if (interaction.isButton()) {
    // Button handlers can remain largely the same, but we'll use safeEdit for consistency
    if (interaction.customId.startsWith('lb:')) {
      try {
        await interaction.deferUpdate();
        const [, , catStr, pageStr] = interaction.customId.split(':');
        const catIdx = Math.max(0, Math.min(2, parseInt(catStr,10) || 0));
        const page   = Math.max(0, parseInt(pageStr,10) || 0);

        let rows = interaction.client.lbCache?.get(interaction.message.interaction?.id || '');
        if (!Array.isArray(rows)) {
          console.log('[LB] cache miss, reloading data');
          rows = await loadLeaderboardData();
        }

        const embed = buildLbEmbed(rows, catIdx, page);
        await safeEdit(interaction, { embeds: [embed], components: [lbRow(catIdx, page)] });
      } catch (e) {
        console.error('lb button error:', e);
        await safeEdit(interaction, { content: 'Sorry — leaderboard couldn’t refresh right now.', components: [] });
      }
    }
    else if (interaction.customId.startsWith('msg:')) {
      const invokerId = interaction.message.interaction?.user?.id;
      if (invokerId && invokerId !== interaction.user.id) {
        return interaction.reply({ content: 'Only the person who ran this command can use these controls.', flags: EPHEMERAL_FLAG });
      }
    
      try {
        const key = interaction.message.interaction?.id || '';
        interaction.client.msgCache ??= new Map();
        let state = interaction.client.msgCache.get(key);
        if (!state) {
          const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
          state = { list, nameMap };
          interaction.client.msgCache.set(key, state);
        }
    
        const [ns, action, a, b] = interaction.customId.split(':'); // msg:<action>:...
    
        // ✅ Modal path: DO NOT defer
        if (action === 'reply') {
          const path = decPath(a);
          const modal = new ModalBuilder()
            .setCustomId(`msg:replyModal:${encPath(path)}`)
            .setTitle('Reply to message');
    
          const input = new TextInputBuilder()
            .setCustomId('replyText')
            .setLabel('Your reply')
            .setStyle(TextInputStyle.Paragraph)
            .setMaxLength(500);
    
          modal.addComponents(new ActionRowBuilder().addComponents(input));
          return await interaction.showModal(modal);
        }
    
        // ✅ Everything else can be safely deferred
        await interaction.deferUpdate();
    
        if (action === 'view' || action === 'openIdx') {
          const idx = Math.max(0, Math.min(parseInt(a||'0',10), (state.list.length||1)-1));
          const msg = state.list[idx];
          const fresh = await loadNode(msg.path);
          const hasReplies = !!(fresh?.replies && Object.keys(fresh.replies).length);
          const embed = buildMessageDetailEmbed({ ...msg, ...fresh }, state.nameMap);
          return await safeEdit(interaction, { embeds: [embed], components: messageDetailRows(idx, state.list, msg.path, hasReplies) });
        }
        if (action === 'back') {
          const embed = buildMessagesEmbed(state.list, state.nameMap);
          return await safeEdit(interaction, { embeds: [embed], components: messageIndexRows(state.list.length || 0) });
        }
        if (action === 'refresh') {
          const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
          state.list = list;
          state.nameMap = nameMap;
          const embed = buildMessagesEmbed(list, nameMap);
          return await safeEdit(interaction, { embeds: [embed], components: messageIndexRows(list.length || 0) });
        }
        if (action === 'refreshOne') {
          const idx = Math.max(0, Math.min(parseInt(a||'0',10), (state.list.length||1)-1));
          const msg = state.list[idx];
          const fresh = await loadNode(msg.path);
          const hasReplies = !!(fresh?.replies && Object.keys(fresh.replies).length);
          const embed = buildMessageDetailEmbed({ ...msg, ...fresh }, state.nameMap);
          return await safeEdit(interaction, { embeds: [embed], components: messageDetailRows(idx, state.list, msg.path, hasReplies) });
        }
        if (action === 'openPath') {
          const path = decPath(a);
          const idx = state.list.findIndex(m=>m.path===path);
          const base = idx>=0 ? state.list[idx] : await loadNode(path);
          const fresh = await loadNode(path);
          const hasReplies = !!(fresh?.replies && Object.keys(fresh.replies).length);
          const embed = buildMessageDetailEmbed({ ...(base||{}), ...(fresh||{}) }, state.nameMap);
          return await safeEdit(interaction, { embeds: [embed], components: messageDetailRows(Math.max(0,idx), state.list, path, hasReplies) });
        }
        if (action === 'thread') {
          const path = decPath(a);
          const page = parseInt(b||'0',10) || 0;
          const parent = await loadNode(path);
          const children = await loadReplies(path);
          const embed = buildThreadEmbed(parent, children, page, 10, state.nameMap);
          return await safeEdit(interaction, { embeds: [embed], components: threadRows(path, children, page, 10) });
        }
        if (action === 'openChild') {
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
          return await safeEdit(interaction, { embeds: [embed], components: rows });
        }
        if (action === 'like') {
          const discordId = interaction.user.id;
          const uid = await getKCUidForDiscord(discordId);
          if (!uid) return await safeEdit(interaction, { content: 'Link your KC account first with /link.' });
    
          const path = decPath(a);
          const likedSnap = await withTimeout(rtdb.ref(`${path}/likedBy/${uid}`).get(), 3000, `RTDB ${path}/likedBy`);
          const wasLiked = likedSnap.exists();
          await rtdb.ref(`${path}/likedBy/${uid}`).transaction(cur => cur ? null : true);
          await rtdb.ref(`${path}/likes`).transaction(cur => (cur||0) + (wasLiked ? -1 : 1));
    
          const node = await loadNode(path);
          const embed = buildMessageDetailEmbed(node, state?.nameMap || {});
          const i = state.list.findIndex(m=>m.path===path);
          if (i>=0) state.list[i] = { ...state.list[i], ...node };
          return await safeEdit(interaction, { embeds: [embed] }); // components persist
        }
      } catch (e) {
        console.error('msg button error:', e);
        await safeEdit(interaction, { content: 'Sorry — could not update messages right now.', components: [] });
      }
    }
    else if (interaction.customId === 'votes:refresh') {
      try {
        await interaction.deferUpdate();
        const scores = await loadVoteScores();
        const embed = buildVoteEmbed(scores);
        const row = new ActionRowBuilder().addComponents(
          new ButtonBuilder().setCustomId('votes:refresh').setLabel('Refresh').setStyle(ButtonStyle.Primary)
        );
        await safeEdit(interaction, { embeds: [embed], components: [row] });
      } catch (e) {
        console.error('votes refresh error:', e);
        await safeEdit(interaction, { content: 'Sorry — voting scores couldn’t refresh right now.', components: [] });
      }
    }
    return;
  }

  if (interaction.isModalSubmit() && interaction.customId.startsWith('msg:replyModal:')) {
    try {
      await interaction.deferReply({ ephemeral: true });
      const path = decPath(interaction.customId.split(':')[2]);
      const text = interaction.fields.getTextInputValue('replyText').trim();
      if (!text) return interaction.editReply('Reply can’t be empty.');
  
      const discordId = interaction.user.id;
      const uid = await getKCUidForDiscord(discordId);
      if (!uid) return interaction.editReply('You must link your KC account first with /link.');
  
      // Resolve a display name (same source your site uses)
      let nameMap = await getAllUserNames();
      const userName = nameMap[uid] || interaction.user.username;
  
      const reply = {
        user: userName,
        uid,
        text,
        time: admin.database.ServerValue.TIMESTAMP,
      };
  
      await withTimeout(rtdb.ref(`${path}/replies`).push(reply), 4000, `RTDB ${path}/replies.push`);
  
      await interaction.editReply('✅ Reply posted!');
    } catch (e) {
      console.error('reply modal error:', e);
      if (!interaction.replied) await interaction.reply({ content: 'Failed to post reply.', flags: EPHEMERAL_FLAG });
    }
  }

  if (!interaction.isChatInputCommand()) return;

  // Centralized error handler for all slash commands
  try {
    const { commandName } = interaction;
    
    if (commandName === 'whoami') {
      const kcUid = await getKCUidForDiscord(interaction.user.id) || 'not linked';
      await interaction.editReply({ content: `Discord ID: \`${interaction.user.id}\`\nKC UID: \`${kcUid}\`` });
    }
    else if (commandName === 'dumpme') {
        const discordId = interaction.user.id;
        const uid = await getKCUidForDiscord(discordId);
        if (!uid) {
          return await interaction.editReply({ content: 'Not linked. Run `/link` first.' });
        }

        const [userRT, badgesRT, postsRT] = await Promise.all([
          withTimeout(rtdb.ref(`users/${uid}`).get(), 4000, `RTDB users/${uid}`),
          withTimeout(rtdb.ref(`badges/${uid}`).get(), 4000, `RTDB badges/${uid}`),
          withTimeout(rtdb.ref(`users/${uid}/posts`).get(), 4000, `RTDB users/${uid}/posts`),
        ]);

        const firestore = admin.firestore();
        const [userFS, postsFS] = await Promise.all([
          withTimeout(firestore.collection('users').doc(uid).get(), 4000, `FS users/${uid}`),
          withTimeout(firestore.collection('users').doc(uid).collection('posts').get(), 4000, `FS users/${uid}/posts`),
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
          rtdb: {
            user: brief(userRT),
            badges: brief(badgesRT),
            postsCount: postsRT.exists() ? Object.keys(postsRT.val() || {}).length : 0,
          },
          firestore: {
            userExists: userFS.exists,
            userKeys: userFS.exists ? Object.keys(userFS.data() || {}) : [],
            postsCount: postsFS.size,
          }
        };
        await interaction.editReply({ content: '```json\n' + JSON.stringify(payload, null, 2).slice(0, 1900) + '\n```' });
    }
    else if (commandName === 'leaderboard') {
        const rows = await loadLeaderboardData();
        const catIdx = 0, page = 0;
        const embed = buildLbEmbed(rows, catIdx, page);
        interaction.client.lbCache ??= new Map(); 
        interaction.client.lbCache.set(interaction.id, rows);
        await interaction.editReply({ embeds: [embed], components: [lbRow(catIdx, page)] });
    }
    else if (commandName === 'clips') {
        const platform = (interaction.options.getString('platform') || 'all').toLowerCase();
        const all = await fetchAllPosts({ platform });
        if (!all.length) {
          return await interaction.editReply({ content: 'No clips found.' });
        }

        all.sort((a, b) => b.score - a.score);
        const top = all.slice(0, 5);
        const nameMap = await getAllUserNames();

        const lines = top.map((p, i) => {
          const d = p.data || {};
          const who = nameMap[p.ownerUid] || '(unknown)';
          const cap = (d.caption || '').trim().slice(0, 120) || '(no caption)';
          let link = '';
          if (d.type === 'youtube' && d.ytId) link = `https://youtu.be/${d.ytId}`;
          else if (d.type === 'tiktok' && d.videoId) link = `https://www.tiktok.com/embed/v2/${d.videoId}`;
          const meta = `👍 ${p.reacts} • 💬 ${p.comments}`;
          return `**${i + 1}.** ${cap}${link ? ` — ${link}` : ''}\nUploader: **${who}** • ${meta}`;
        });

        const embed = new EmbedBuilder()
          .setTitle(`Top ${Math.min(top.length, 5)} ${platform === 'all' ? 'Clips' : platform.charAt(0).toUpperCase()+platform.slice(1)}`)
          .setDescription(lines.join('\n\n'))
          .setColor(DEFAULT_EMBED_COLOR)
          .setFooter({ text: 'KC Bot • /clips' });

        await interaction.editReply({ embeds: [embed] });
    }
    else if (commandName === 'messages') {
        const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
        interaction.client.msgCache ??= new Map();
        interaction.client.msgCache.set(interaction.id, { list, nameMap });
        // Show the list first (numbers 1..10 + Refresh) as you do today:
        const embed = buildMessagesEmbed(list, nameMap);
        await interaction.editReply({ embeds: [embed], components: messageIndexRows(list.length || 0) });
    }
    else if (commandName === 'votingscores') {
        const scores = await loadVoteScores();
        const embed = buildVoteEmbed(scores);
        const row = new ActionRowBuilder().addComponents(
          new ButtonBuilder().setCustomId('votes:refresh').setLabel('Refresh').setStyle(ButtonStyle.Primary)
        );
        await interaction.editReply({ embeds: [embed], components: [row] });
    }
    else if (commandName === 'badges') {
        const target = interaction.options.getUser('user') || interaction.user;
        const discordId = target.id;
        const kcUid = await getKCUidForDiscord(discordId);

        if (!kcUid) {
          return await interaction.editReply({
            content: target.id === interaction.user.id
              ? 'I can’t find your KC account. Use `/link` to connect it first.'
              : `I can’t find a KC account linked to **${target.tag}**.`
          });
        }
        
        const profile = await withTimeout(getKCProfile(kcUid), 5000, `getKCProfile(${kcUid})`);
        if (!profile) {
          return await interaction.editReply({ content: 'No profile data found.' });
        }

        const title       = clampStr(`${profile.displayName} — KC Profile`, 256, 'KC Profile');
        const description = clampStr(profile.about, 4096);
        const badgesVal   = clampStr(profile.badgesText, 1024);
        const streakVal   = clampStr(profile.streak, 1024, '—');
        const postsVal    = clampStr(profile.postsText, 1024);
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

        await interaction.editReply({ embeds: [embed], components: [row] });
    }
    else if (commandName === 'syncavatar') {
        const discordId = interaction.user.id;
        const uid = await getKCUidForDiscord(discordId);
        if (!uid) {
          return await interaction.editReply({ content: 'You are not linked. Use `/link` first.' });
        }

        const allowed = await hasEmerald(uid);
        if (!allowed) {
          return await interaction.editReply({ content: 'This feature requires Emerald/profile customisation.' });
        }

        const action = interaction.options.getString('action');
        if (action === 'set') {
          const url = interaction.user.displayAvatarURL({ extension: 'png', size: 256 });
          await setKCAvatar(uid, url);
          await interaction.editReply({ content: '✅ Your KC profile picture has been updated to your Discord avatar.' });
        } else if (action === 'revert') {
          await clearKCAvatar(uid);
          await interaction.editReply({ content: '✅ Avatar override removed. Your KC profile will use the default/site picture again.' });
        } else {
          await interaction.editReply({ content: 'Unknown action.' });
        }
    }
    else if (commandName === 'post') {
        const discordId = interaction.user.id;
        const uid = await getKCUidForDiscord(discordId);
        if (!uid) {
          return await interaction.editReply({ content: 'You are not linked. Use `/link` first.' });
        }
    
        if (await postsDisabledGlobally()) {
          return await interaction.editReply({ content: '🚫 Posting is currently disabled by admins.' });
        }
    
        const allowed = await postingUnlocked(uid);
        if (!allowed) {
          return await interaction.editReply({ content: '❌ You don’t have posting unlocked. (Emerald/Diamond or Content access required.)' });
        }
    
        const link = interaction.options.getString('link') || '';
        const caption = (interaction.options.getString('caption') || '').slice(0, 140);
        const draft = !!interaction.options.getBoolean('draft');
        const scheduleAtIso = interaction.options.getString('schedule_at') || '';
        const parsed = parseVideoLink(link);
    
        if (!parsed) {
          return await interaction.editReply({ content: 'Invalid link. Please provide a YouTube or TikTok link.' });
        }
    
        const publishAt = scheduleAtIso ? Date.parse(scheduleAtIso) : null;
        const postData = {
          ...parsed,
          caption,
          createdAt: admin.database.ServerValue.TIMESTAMP,
          createdBy: uid,
          draft: !!draft,
          publishAt: Number.isFinite(publishAt) ? publishAt : null,
        };
    
        const ref = rtdb.ref(`users/${uid}/posts`).push();
        const postId = ref.key;
        await withTimeout(ref.set(postData), 4000, `write post ${postId}`);
    
        await interaction.editReply({ content: '✅ Post saved. Sending summary…' });
        await interaction.followUp({
          content: [
            '✅ **Post created!**',
            `• **Type:** ${postData.type}`,
            `• **Caption:** ${caption || '(none)'}`,
            publishAt ? `• **Scheduled:** ${new Date(publishAt).toLocaleString()}` : (draft ? '• **Saved as draft**' : '• **Published immediately**')
          ].join('\n'),
        });
    }
    else if (commandName === 'postmessage') {
      const discordId = interaction.user.id;
      const uid = await getKCUidForDiscord(discordId);
      if (!uid) {
        return await interaction.editReply({ content: 'You must link your KC account first with /link.' });
      }

      const nameMap = await getAllUserNames();
      const userName = nameMap[uid] || interaction.user.username;
      const text = interaction.options.getString('text');

      const message = {
        text,
        uid,
        user: userName,
        time: admin.database.ServerValue.TIMESTAMP,
      };

      await rtdb.ref('messages').push(message);
      await interaction.editReply({ content: '✅ Message posted!' });
    }

  } catch (err) {
    console.error(`Error handling command ${interaction.commandName}:`, err);
    const msg = 'Sorry, something went wrong.';
    try {
      if (interaction.replied || interaction.deferred) {
        await interaction.editReply({ content: msg, embeds: [], components: [] });
      } else {
        await interaction.reply({ content: msg, flags: EPHEMERAL_FLAG });
      }
    } catch (e) {
      console.error('Failed to send error reply:', e);
    }
  }
});


// ---------- Startup ----------
client.once('ready', () => {
  console.log(`Logged in as ${client.user.tag}`);
});

process.on('unhandledRejection', e => console.error('unhandledRejection', e));
process.on('uncaughtException', e => console.error('uncaughtException', e));

(async () => {
  try {
    console.log('Registering slash commands…');
    await registerCommands();
    console.log('Slash command registration complete.');
  } catch (e) {
    console.error('registerCommands FAILED:', e?.rawError || e);
  }

  try {
    console.log('Logging into Discord…');
    await client.login(process.env.DISCORD_BOT_TOKEN);
    console.log('Login promise resolved.');
  } catch (e) {
    console.error('client.login FAILED:', e?.rawError || e);
    process.exit(1); // don’t keep a “healthy” web server without the bot
  }
})();
