// index.js
// Discord.js v14 + Firebase Admin (Realtime Database & Firestore)
// Commands: /link (DMs auth link), /badges (public profile embed), /whoami (debug), /dumpme (debug)

// Make dotenv optional (used locally, ignored on Render if not installed)
try { require('dotenv').config(); } catch (_) {}


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
} = require('discord.js');

const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

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
function isUnknownInteraction(err) {
  return err?.code === 10062 || err?.rawError?.code === 10062;
}

function isPermsError(err) { return err?.code === 50013; }

// New helper to catch all expired token errors
function isExpiredToken(err) {
  return (
    err?.code === 10062 || // Unknown interaction
    err?.code === 50027 || // Invalid Webhook Token
    err?.status === 401    // Unauthorized (same effect)
  );
}

async function safeDefer(interaction, { ephemeral = false } = {}) {
  const age = Date.now() - interaction.createdTimestamp;
  if (age > 2900) return false;
  if (interaction.deferred || interaction.replied) return true;
  try {
    await interaction.deferReply({ ephemeral });
    return true;
  } catch (err) {
    // Retry as ephemeral (often succeeds even if channel send perms are missing)
    try {
      await interaction.deferReply({ ephemeral: true });
      return true;
    } catch (e2) {
      console.warn(`[safeDefer] All defer attempts failed for interaction ${interaction.id}`);
      return false;
    }
  }
}

async function safeEdit(interaction, data, fallbackText = 'Reply expired. Please run the command again.') {
  try {
    if (interaction.deferred || interaction.replied) {
      return await interaction.editReply(data);
    } else {
      return await interaction.reply({ ...data, ephemeral: data.ephemeral || false });
    }
  } catch (err) {
    if (isExpiredToken(err) || isPermsError(err)) {
      try {
        // send a follow-up, always ephemeral to avoid spamming
        return await interaction.followUp({ ...data, ephemeral: true });
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

function watchdog(ms, onTimeout) {
  let done = false;
  const t = setTimeout(() => { if (!done) onTimeout(); }, ms);
  return () => { done = true; clearTimeout(t); };
}

// Always produce a user-visible message even if interaction token expired
async function finalRespond(interaction, data, fallbackText = null) {
  try {
    if (interaction.deferred || interaction.replied) {
      return await interaction.editReply(data);
    }
    // brand new reply
    return await interaction.reply({ ...data, ephemeral: data.ephemeral || false });
  } catch (err) {
    // If editReply failed because the token is invalid, perms are missing, etc.
    if (isExpiredToken(err) || isPermsError(err)) {
      console.warn(`[finalRespond] editReply failed for ${interaction.id}, attempting followup.`);
      try {
        // followUp can be used to send a new message after the token expires.
        return await interaction.followUp({ ...data, ephemeral: true });
      } catch (followUpErr) {
        console.error('[finalRespond] FollowUp also failed:', followUpErr);
        // As a last resort, send a message to the channel if we have a fallback.
        if (interaction.channel && fallbackText) {
          try { await interaction.channel.send(fallbackText); } catch (_) {}
        }
      }
      return null;
    }
    // Re-throw other, unexpected errors
    throw err;
  }
}

function progressKick(interaction, ms = 2900, text = 'Still working…') {
  const cancel = watchdog(ms, () => {
    // always ephemeral so we don’t spam channels if something stalls
    finalRespond(interaction, { content: text, ephemeral: true }, text);
  });
  return cancel;
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
    const arr = snapshotToArray(snap).slice(0, limit);
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
      const arr = snapshotToArray(snap).slice(0, limit);
      return arr;
    }
    throw e;
  }
}

// Build an embed showing a page of 10 messages (title, text, reply count)
function buildMessagesEmbed(list, userNames = {}) {
  const desc = list
    .map((m, i) => {
      const who = userNames[m.uid] || m.user || '(unknown)';
      const text = m.text?.length > 100 ? m.text.slice(0, 100) + '…' : m.text;
      return `**${i + 1}. ${who}**\n— ${text || '_no text_'}\nReplies: **${m.replies ? Object.keys(m.replies).length : 0}**`;
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

function buildRepliesEmbed(message, userNames = {}) {
  const who = userNames[message.uid] || message.user || '(unknown)';
  const title = `Replies — ${who}: ${(message.text || '').toString().slice(0, 60)}`;
  const replies = Object.entries(message.replies || {});
  const lines = replies.map(([key, r]) => {
    const name = userNames[r.uid] || r.user || '(unknown)';
    return `• **${name}**: ${(r.text || '').toString().slice(0, 180)}`;
  });

  return new EmbedBuilder()
    .setTitle(title)
    .setDescription(lines.join('\n') || '_No replies yet_')
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: 'KC Bot • /messages' });
}

function repliesNavRow(idx) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('msg:back').setLabel('Back').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`msg:refreshOne:${idx}`).setLabel('Refresh').setStyle(ButtonStyle.Primary)
  );
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

// Register (include it in commands array)
const commandsJson = [
  linkCmd, badgesCmd, whoamiCmd, dumpCmd, lbCmd, clipsCmd, messagesCmd, votingCmd,
  avatarCmd, postCmd
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
  try {
    const tag = interaction.user?.tag ?? 'unknown';
    const name = interaction.isChatInputCommand() ? interaction.commandName : interaction.customId;
    console.log(`[INT] ${name} from ${tag} (${interaction.user?.id}) at ${new Date().toISOString()}`);
  } catch (_) {}

  if (interaction.isButton() && interaction.customId.startsWith('lb:')) {
    try {
      await interaction.deferUpdate();
      const cancel = progressKick(interaction, 2000, 'Refreshing…');
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
      cancel();
    } catch (e) {
      console.error('lb button error:', e);
      await safeEdit(interaction, { content: 'Sorry — leaderboard couldn’t refresh right now.', components: [] });
    }
    return;
  }

  if (interaction.isButton() && interaction.customId.startsWith('msg:')) {
    try {
      await interaction.deferUpdate();
      const cancel = progressKick(interaction, 2000, 'Refreshing…');
      const parts = interaction.customId.split(':'); // msg:view:idx | msg:back | msg:refresh | msg:refreshOne:idx
      const action = parts[1];

      // Find cached data from the originating slash interaction
      const key = interaction.message.interaction?.id || '';
      interaction.client.msgCache ??= new Map();
      let cache = interaction.client.msgCache.get(key);

      // If cache missing, reload latest
      if (!cache) {
        const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
        cache = { list, nameMap };
        interaction.client.msgCache.set(key, cache);
      }

      if (action === 'view') {
        const idx = Math.max(0, Math.min(parseInt(parts[2] || '0', 10), (cache.list.length || 1) - 1));
        const msg = cache.list[idx];
        const embed = buildRepliesEmbed(msg, cache.nameMap);
        await safeEdit(interaction, { embeds: [embed], components: [repliesNavRow(idx)] });
      } else if (action === 'back') {
        const embed = buildMessagesEmbed(cache.list, cache.nameMap);
        await safeEdit(interaction, { embeds: [embed], components: messageIndexRows(cache.list.length || 0) });
      } else if (action === 'refresh') {
        const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
        interaction.client.msgCache.set(key, { list, nameMap });
        const embed = buildMessagesEmbed(list, nameMap);
        await safeEdit(interaction, { embeds: [embed], components: messageIndexRows(list.length || 0) });
      } else if (action === 'refreshOne') {
        const idx = parseInt(parts[2] || '0', 10);
        const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
        interaction.client.msgCache.set(key, { list, nameMap });
        const msg = list[idx] || list[0];
        const embed = buildRepliesEmbed(msg, nameMap);
        await safeEdit(interaction, { embeds: [embed], components: [repliesNavRow(Math.max(0, idx))] });
      }
      cancel();
    } catch (e) {
      console.error('msg button error:', e);
      await safeEdit(interaction, { content: 'Sorry — messages couldn’t refresh right now.', components: [] });
    }
    return;
  }

  if (interaction.isButton() && interaction.customId === 'votes:refresh') {
    try {
      await interaction.deferUpdate();
      const cancel = progressKick(interaction, 2000, 'Refreshing…');
      const scores = await loadVoteScores();
      const embed = buildVoteEmbed(scores);
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId('votes:refresh').setLabel('Refresh').setStyle(ButtonStyle.Primary)
      );
      await safeEdit(interaction, { embeds: [embed], components: [row] });
      cancel();
    } catch (e) {
      console.error('votes refresh error:', e);
      await safeEdit(interaction, { content: 'Sorry — voting scores couldn’t refresh right now.', components: [] });
    }
    return;
  }

  if (!interaction.isChatInputCommand()) return;

  const handleDeferFailure = async () => {
    console.warn(`[DEFER FAIL] ${interaction.commandName} in #${interaction.channel?.id} — missing perms or token expired`);
    try {
        await interaction.reply({ content: 'Sorry, that timed out. Please run the command again.', ephemeral: true });
    } catch {
        if (interaction.channel) { try { await interaction.channel.send('Sorry, that timed out. Please run the command again.'); } catch {} }
    }
  };

  if (interaction.commandName === 'link') {
    const start = process.env.AUTH_BRIDGE_START_URL;
    const url = `${start}?state=${encodeURIComponent(interaction.user.id)}`;
    return interaction.reply({ content: `Click to link your account: ${url}`, ephemeral: true });
  }

  if (interaction.commandName === 'whoami') {
    const ok = await safeDefer(interaction, { ephemeral: true });
    if (!ok) return handleDeferFailure();
    const cancelProgress = progressKick(interaction, 4000);

    try {
      console.log('[INT] whoami -> getKCUidForDiscord');
      const kcUid = await getKCUidForDiscord(interaction.user.id) || 'not linked';
      await finalRespond(
        interaction,
        { content: `Discord ID: \`${interaction.user.id}\`\nKC UID: \`${kcUid}\``, ephemeral: true },
        'The whoami reply expired. Please try again.'
      );
    } catch (e) {
      console.error('whoami error:', e);
      await finalRespond(interaction, { content: 'Something went wrong.', ephemeral: true }, 'whoami failed, please try again.');
    } finally {
      cancelProgress();
    }
    return;
  }

  if (interaction.commandName === 'dumpme') {
    const ok = await safeDefer(interaction, { ephemeral: true });
    if (!ok) return handleDeferFailure();
    const cancelProgress = progressKick(interaction, 4000);

    try {
      const discordId = interaction.user.id;
      const uid = await getKCUidForDiscord(discordId);
      if (!uid) {
        await safeEdit(interaction, { content: 'Not linked. Run `/link` first.', ephemeral: true });
        return;
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

      await safeEdit(interaction, { content: '```json\n' + JSON.stringify(payload, null, 2).slice(0, 1900) + '\n```', ephemeral: true });
    } catch (e) {
      console.error('dumpme error:', e);
      await safeEdit(interaction, { content: 'Error reading data (see logs).', ephemeral: true });
    } finally {
      cancelProgress();
    }
    return;
  }

  if (interaction.commandName === 'leaderboard') {
    const ok = await safeDefer(interaction); // public
    if (!ok) return handleDeferFailure();
    const cancelProgress = progressKick(interaction);

    try {
      console.log('[INT] leaderboard -> loadLeaderboardData');
      const rows = await loadLeaderboardData();
      const catIdx = 0, page = 0;
      const embed = buildLbEmbed(rows, catIdx, page);
      await safeEdit(interaction, { embeds: [embed], components: [lbRow(catIdx, page)] });
      interaction.client.lbCache ??= new Map(); 
      interaction.client.lbCache.set(interaction.id, rows);
    } catch (e) {
        console.error('leaderboard error:', e);
        await finalRespond(interaction, { content: 'Something went wrong.' }, 'leaderboard failed, please try again.');
    } finally {
        cancelProgress();
    }
    return;
  }

  if (interaction.commandName === 'clips') {
    const ok = await safeDefer(interaction); // public
    if (!ok) return handleDeferFailure();
    const cancelProgress = progressKick(interaction);

    try {
      const platform = (interaction.options.getString('platform') || 'all').toLowerCase();
      const all = await fetchAllPosts({ platform });
      if (!all.length) {
        await safeEdit(interaction, { content: 'No clips found.' });
        return;
      }

      // Sort by popularity score and take top 5
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

      await safeEdit(interaction, { embeds: [embed] }, 'The reply expired—try `/clips` again.');
    } catch (e) {
      console.error('clips error:', e);
      await finalRespond(interaction, { content: 'Something went wrong.' }, 'clips failed, please try again.');
    } finally {
      cancelProgress();
    }
    return;
  }

  if (interaction.commandName === 'messages') {
    const ok = await safeDefer(interaction); // public
    if (!ok) return handleDeferFailure();
    const cancelProgress = progressKick(interaction);

    try {
      const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
      const embed = buildMessagesEmbed(list, nameMap);

      // Cache for the buttons handler
      interaction.client.msgCache ??= new Map();
      interaction.client.msgCache.set(interaction.id, { list, nameMap });

      await safeEdit(interaction, { embeds: [embed], components: messageIndexRows(list.length || 0) });
    } catch (e) {
      console.error('messages error:', e);
      await finalRespond(interaction, { content: 'Something went wrong.' }, 'messages failed, please try again.');
    } finally {
      cancelProgress();
    }
    return;
  }

  if (interaction.commandName === 'votingscores') {
    const ok = await safeDefer(interaction); // public
    if (!ok) return handleDeferFailure();
    const cancelProgress = progressKick(interaction);

    try {
      const scores = await loadVoteScores();
      const embed = buildVoteEmbed(scores);
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId('votes:refresh').setLabel('Refresh').setStyle(ButtonStyle.Primary)
      );
      await safeEdit(interaction, { embeds: [embed], components: [row] });
    } catch (e) {
      console.error('votingscores error:', e);
      await finalRespond(interaction, { content: 'Something went wrong.' }, 'votingscores failed, please try again.');
    } finally {
      cancelProgress();
    }
    return;
  }

  if (interaction.commandName === 'badges') {
    const ok = await safeDefer(interaction); // public
    if (!ok) return handleDeferFailure();
    
    const cancelProgress = progressKick(interaction);

    try {
      // target: provided user or self
      const target = interaction.options.getUser('user') || interaction.user;
      const discordId = target.id;
      
      console.log('[INT] badges -> getKCUidForDiscord');
      const kcUid = await getKCUidForDiscord(discordId);
      if (!kcUid) {
        await finalRespond(interaction, {
          content: target.id === interaction.user.id
            ? 'I can’t find your KC account. Use `/link` to connect it first.'
            : `I can’t find a KC account linked to **${target.tag}**.`
        }, 'Could not find a linked KC account.');
        return;
      }
      
      console.log('[INT] badges -> getKCProfile');
      const profile = await withTimeout(getKCProfile(kcUid), 5000, `getKCProfile(${kcUid})`);

      if (!profile) {
        await finalRespond(interaction, { content: 'No profile data found.' }, 'No profile data found.');
        return;
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

      await finalRespond(
        interaction,
        { embeds: [embed], components: [row] },
        'Your `/badges` reply expired — please run it again.'
      );
    } catch (err) {
      console.error('badges command error:', err);
      console.error('raw error body:', err?.rawError);
      console.error('raw errors tree:', err?.rawError?.errors);
      await finalRespond(
        interaction,
        { content: 'Something went wrong (see logs).' },
        'Something went wrong running `/badges`. Please try again.'
      );
    } finally {
      cancelProgress();
    }
    return;
  }

  if (interaction.commandName === 'syncavatar') {
    const ok = await safeDefer(interaction, { ephemeral: true });
    if (!ok) return handleDeferFailure();

    try {
      // 1) Must be linked
      const discordId = interaction.user.id;
      const uid = await getKCUidForDiscord(discordId);
      if (!uid) {
        await safeEdit(interaction, { content: 'You are not linked. Use `/link` first.' });
        return;
      }

      // 2) Must have Emerald (or higher) customisation
      const allowed = await hasEmerald(uid);
      if (!allowed) {
        await safeEdit(interaction, { content: 'This feature requires Emerald/profile customisation.' });
        return;
      }

      // 3) Action
      const action = interaction.options.getString('action');

      if (action === 'set') {
        // Prefer PNG and a sensible size; 256 is plenty for your site
        const url = interaction.user.displayAvatarURL({ extension: 'png', size: 256 });
        await setKCAvatar(uid, url);
        await safeEdit(interaction, { content: '✅ Your KC profile picture has been updated to your Discord avatar.' });
        return;
      }

      if (action === 'revert') {
        await clearKCAvatar(uid);
        await safeEdit(interaction, { content: '✅ Avatar override removed. Your KC profile will use the default/site picture again.' });
        return;
      }

      await safeEdit(interaction, { content: 'Unknown action.' });
    } catch (e) {
      console.error('syncavatar error:', e);
      await safeEdit(interaction, { content: 'Something went wrong updating your avatar.' });
    }
    return;
  }
  
  if (interaction.commandName === 'post') {
    const ok = await safeDefer(interaction, { ephemeral: true });
    if (!ok) return handleDeferFailure();
  
    try {
      // Must be linked
      const discordId = interaction.user.id;
      const uid = await getKCUidForDiscord(discordId);
      if (!uid) {
        await safeEdit(interaction, { content: 'You are not linked. Use `/link` first.' });
        return;
      }
  
      // Global stop switch
      if (await postsDisabledGlobally()) {
        await safeEdit(interaction, { content: '🚫 Posting is currently disabled by admins.' });
        return;
      }
  
      // Must have posting unlocked (emerald/diamond/content etc.)
      const allowed = await postingUnlocked(uid);
      if (!allowed) {
        await safeEdit(interaction, { content: '❌ You don’t have posting unlocked. (Emerald/Diamond or Content access required.)' });
        return;
      }
  
      // Inputs
      const link = interaction.options.getString('link') || '';
      const caption = (interaction.options.getString('caption') || '').slice(0, 140);
      const draft = !!interaction.options.getBoolean('draft');
      const scheduleAtIso = interaction.options.getString('schedule_at') || '';
      const parsed = parseVideoLink(link);
  
      if (!parsed) {
        await safeEdit(interaction, { content: 'Invalid link. Please provide a YouTube or TikTok link.' });
        return;
      }
  
      // Build post payload (mirror site)
      const publishAt = scheduleAtIso ? Date.parse(scheduleAtIso) : null;
  
      const postData = {
        ...parsed,
        caption,
        createdAt: admin.database.ServerValue.TIMESTAMP,
        createdBy: uid,
        draft: !!draft,
        publishAt: Number.isFinite(publishAt) ? publishAt : null,
      };
  
      // Create a new post id and write
      const ref = rtdb.ref(`users/${uid}/posts`).push();
      const postId = ref.key;
      await withTimeout(ref.set(postData), 4000, `write post ${postId}`);
  
      // Public confirmation (non-ephemeral) with a small summary
      await safeEdit(interaction, { content: '✅ Post saved. Sending summary…', ephemeral: true });
      await interaction.followUp({
        content: [
          '✅ **Post created!**',
          `• **Type:** ${postData.type}`,
          `• **Caption:** ${caption || '(none)'}`,
          publishAt ? `• **Scheduled:** ${new Date(publishAt).toLocaleString()}` : (draft ? '• **Saved as draft**' : '• **Published immediately**')
        ].join('\n'),
        ephemeral: false
      });
    } catch (e) {
      console.error('post command error:', e);
      await finalRespond(interaction, { content: 'Something went wrong creating the post.' }, 'Post failed, please try again.');
    }
    return;
  }
});

// ---------- Startup ----------
client.once('ready', () => {
  console.log(`Logged in as ${client.user.tag}`);
});

process.on('unhandledRejection', (err) => {
  if (isExpiredToken(err)) {
    console.warn('Ignored expired token error (unhandledRejection).');
    return;
  }
  console.error('unhandledRejection:', err);
});

client.on('error', (err) => {
  if (isExpiredToken(err)) {
    console.warn('Client error: Ignored expired token error.');
    return;
  }
  console.error('Client error:', err);
});

(async () => {
  await registerCommands();
  await client.login(process.env.DISCORD_BOT_TOKEN);
})();
