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

// --- KC badge icons (same as site) ---
const KC_BADGE_ICONS = {
  offence: 'https://kevinmidnight7-sudo.github.io/messageboardkc/red.png',
  defence: 'https://kevinmidnight7-sudo.github.io/messageboardkc/blue.png',
  overall: 'https://kevinmidnight7-sudo.github.io/messageboardkc/kcevents.png',
  verified: 'https://kevinmidnight7-sudo.github.io/messageboardkc/verified.png',
  diamond: 'https://kevinmidnight7-sudo.github.io/messageboardkc/diamond2.png',
  emerald: 'https://cdn.discordapp.com/emojis/1381097372930543776.webp?size=128'
};

const EMOJI = {
  offence:  '<:kc_offence:123456789012345678>',
  defence:  '<:kc_defence:123456789012345679>',
  overall:  '<:kc_overall:123456789012345680>',
  verified: '<:kc_verified:123456789012345681>',
  diamond:  '<:kc_diamond:123456789012345682>',
  emerald:  '<:kc_emerald:123456789012345683>',
};

const LB = {
  CATS: [
    { key: 'overall', label: 'Overall Winner' },
    { key: 'offence', label: 'Best Offence' },
    { key: 'defence', label: 'Best Defence' },
  ],
  PAGE_SIZE: 10,
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

// New helper to catch all expired token errors
function isExpiredToken(err) {
  return (
    err?.code === 10062 || // Unknown interaction
    err?.code === 50027 || // Invalid Webhook Token
    err?.status === 401    // Unauthorized (same effect)
  );
}

async function safeDefer(interaction, { ephemeral = false } = {}) {
  // Already acknowledged? nothing to do.
  if (interaction.deferred || interaction.replied) return true;

  // Discord requires an ACK within ~3s. Aim to defer ASAP (<2.8s guard).
  const ageMs = Date.now() - interaction.createdTimestamp;
  if (ageMs > 2800) return false;

  try {
    // use flags instead of deprecated "ephemeral" option
    const opts = ephemeral ? { flags: 64 } : {};
    await interaction.deferReply(opts);
    return true;
  } catch (err) {
    // "Unknown interaction" or other token errors mean it already expired – caller will fallback.
    if (isExpiredToken(err) || err?.rawError?.code === 10062) return false;
    throw err;
  }
}

async function safeEdit(interaction, data) {
  try {
    if (interaction.deferred || interaction.replied) {
      return await interaction.editReply(data);
    } else {
      // still allowed to reply (not acknowledged yet)
      return await interaction.reply({ ...data, ephemeral: true });
    }
  } catch (err) {
    if (isExpiredToken(err)) return null;
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
    // brand new reply (use flags for ephemeral)
    const isEphemeral = data?.flags === 64 || data?.ephemeral === true;
    const payload = isEphemeral ? { ...data, flags: 64 } : data;
    return await interaction.reply(payload);
  } catch (err) {
    // If editReply failed because the token is invalid (likely >15 mins passed)
    if (isExpiredToken(err)) {
      console.warn(`[finalRespond] editReply failed for ${interaction.id}, attempting followup.`);
      try {
        // followUp can be used to send a new message after the token expires.
        // Make it ephemeral to avoid spamming the channel if the original was public.
        const isEphemeral = data?.flags === 64 || data?.ephemeral === true;
        const payload = isEphemeral ? data : { ...data, ephemeral: true };
        return await interaction.followUp(payload);
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
  const snap = await withTimeout(rtdb.ref('users').get(), 4000, 'RTDB users');
  const out = {};
  if (snap.exists()) {
    const all = snap.val() || {};
    for (const uid of Object.keys(all)) {
      const u = all[uid] || {};
      out[uid] = u.displayName || u.email || '(unknown)';
    }
  }
  return out;
}

// Gather all posts across users; return an array of {ownerUid, postId, data, score, reacts, comments}
async function fetchAllPosts({ platform = 'all' } = {}) {
  const usersSnap = await withTimeout(rtdb.ref('users').get(), 5000, 'RTDB users');
  const users = usersSnap.exists() ? usersSnap.val() : {};
  const results = [];

  const tasks = Object.keys(users).map(async uid => {
    const postsSnap = await withTimeout(rtdb.ref(`users/${uid}/posts`).get(), 4000, `RTDB users/${uid}/posts`);
    if (!postsSnap.exists()) return;

    postsSnap.forEach(p => {
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

// Latest N messageboard messages
async function fetchLatestMessages(limit = 10) {
  try {
    const snap = await withTimeout(
      rtdb.ref('messages').orderByChild('time').limitToLast(limit).get(),
      5000,
      'RTDB messages recent'
    );
    return snapshotToArray(snap);
  } catch (e) {
    // fallback if index missing
    if (String(e?.message || '').includes('Index not defined')) {
      console.warn('[fetchLatestMessages] Index missing, falling back to unordered query.');
      const snap = await withTimeout(
        rtdb.ref('messages').limitToLast(limit).get(),
        5000,
        'RTDB messages fallback'
      );
      return snapshotToArray(snap);
    }
    throw e;
  }
}

// Build an embed showing a page of 10 messages (title, text, reply count)
function buildMessagesEmbed(list, userNames = {}) {
  const lines = list.map((m, i) => {
    const who = userNames[m.uid] || m.user || '(unknown)';
    const txt = (m.text || '').toString().slice(0, 140);
    const replies = m.replies ? Object.keys(m.replies).length : 0;
    return `**${i + 1}. ${who}** — ${txt || '—'}\nReplies: **${replies}**`;
  });

  return new EmbedBuilder()
    .setTitle('Messageboard — latest 10')
    .setDescription(lines.join('\n\n') || '_No messages yet_');
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
    .setDescription(lines.join('\n') || '_No replies yet_');
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
    );

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
  const slice = rows
    .sort((a,b) => (b[cat.key]||0) - (a[cat.key]||0))
    .slice(start, start + LB.PAGE_SIZE);

  const lines = slice.map((r, i) => {
    const rank = start + i + 1;
    return `**${rank}.** ${r.name} — \`${r[cat.key] || 0}\``;
  });
  const embed = new EmbedBuilder()
    .setTitle(`Leaderboard — ${cat.label}`)
    .setDescription(lines.join('\n') || '_No data_');
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

  const bonus = user.bonus || 0;
  const streak = Number.isFinite(user.loginStreak) ? String(user.loginStreak) : '—';

  // Profile customization colour (tint embed)
  const custom = user.profileCustomization || {};
  const nameColor = custom.nameColor || null;
  const gradientColor = custom.gradient ? firstHexFromGradient(custom.gradient) : null;

  // Posts visible if content unlocked (diamond/emerald codes or explicit content)
  const codesUnlocked = user.codesUnlocked || {};
  const contentUnlocked = !!(codesUnlocked.content || codesUnlocked.diamond || codesUnlocked.emerald || user.postsUnlocked || user.canPost);

  // Build at most 3 post lines as:  • "Caption" — <link>
  let postLines = [];
  if (contentUnlocked && posts) {
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
    !contentUnlocked
      ? 'Posts locked.'
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
    displayName,
    about,
    bonus,
    streak,
    badgesText: badgeLines.length ? badgeLines.join('\n') : 'No badges yet.',
    postsText: postsField,
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

// Register (include it in commands array)
const commandsJson = [linkCmd, badgesCmd, whoamiCmd, dumpCmd, lbCmd, clipsCmd, messagesCmd, votingCmd].map(c => c.toJSON());


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
    if (!interaction.message?.editable && !interaction.deferred && !interaction.replied) {
      return finalRespond(interaction, { content: 'That leaderboard page is stale. Run `/leaderboard` again.' },
                          'Leaderboard interaction expired. Run `/leaderboard` again.');
    }
    const [, type, catStr, pageStr] = interaction.customId.split(':');
    const catIdx = Math.max(0, Math.min(2, parseInt(catStr,10) || 0));
    const page   = Math.max(0, parseInt(pageStr,10) || 0);

    let rows = interaction.client.lbCache?.get(interaction.message.interaction?.id || '');
    if (!Array.isArray(rows)) {
      console.log('[LB] cache miss, reloading data');
      rows = await loadLeaderboardData();
    }

    const embed = buildLbEmbed(rows, catIdx, page);
    await interaction.update({ embeds: [embed], components: [lbRow(catIdx, page)] });
    return;
  }

  if (interaction.isButton() && interaction.customId.startsWith('msg:')) {
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
      const idx = parseInt(parts[2] || '0', 10);
      const msg = cache.list[idx];
      const embed = buildRepliesEmbed(msg, cache.nameMap);
      return interaction.update({ embeds: [embed], components: [repliesNavRow(idx)] });
    }

    if (action === 'back') {
      const embed = buildMessagesEmbed(cache.list, cache.nameMap);
      return interaction.update({ embeds: [embed], components: messageIndexRows(cache.list.length || 0) });
    }

    if (action === 'refresh') {
      const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
      interaction.client.msgCache.set(key, { list, nameMap });
      const embed = buildMessagesEmbed(list, nameMap);
      return interaction.update({ embeds: [embed], components: messageIndexRows(list.length || 0) });
    }

    if (action === 'refreshOne') {
      const idx = parseInt(parts[2] || '0', 10);
      const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
      interaction.client.msgCache.set(key, { list, nameMap });
      const msg = list[idx] || list[0];
      const embed = buildRepliesEmbed(msg, nameMap);
      return interaction.update({ embeds: [embed], components: [repliesNavRow(Math.max(0, idx))] });
    }
  }

  if (interaction.isButton() && interaction.customId === 'votes:refresh') {
    try {
      const scores = await loadVoteScores();
      const embed = buildVoteEmbed(scores);
      return interaction.update({ embeds: [embed] });
    } catch (e) {
      console.error('votes refresh error:', e);
      return interaction.reply({ content: 'Refresh failed.', ephemeral: true });
    }
  }

  if (!interaction.isChatInputCommand()) return;

  if (interaction.commandName === 'link') {
    const start = process.env.AUTH_BRIDGE_START_URL;
    const url = `${start}?state=${encodeURIComponent(interaction.user.id)}`;
    return interaction.reply({ content: `Click to link your account:\n${url}`, flags: 64 });
  }

  if (interaction.commandName === 'whoami') {
    const ok = await safeDefer(interaction, { ephemeral: true });
    console.log(`[INT] ${interaction.commandName} deferred ok=${ok}`);
    if (!ok) {
      if (interaction.channel) await interaction.channel.send('Sorry, that timed out. Please run `/whoami` again.');
      return;
    }
    const done = watchdog(5000, () => {
      finalRespond(interaction,
        { content: 'Still working… one moment please.' },
        'Reply expired while working. Please try again.');
    });

    console.time(`whoami:${interaction.id}`);
    try {
      console.log('[INT] whoami -> getKCUidForDiscord');
      const kcUid = await getKCUidForDiscord(interaction.user.id) || 'not linked';
      await finalRespond(
        interaction,
        { content: `Discord ID: \`${interaction.user.id}\`\nKC UID: \`${kcUid}\`` },
        'The whoami reply expired. Please try again.'
      );
    } catch (e) {
      console.error('whoami error:', e);
      await finalRespond(interaction, { content: 'Something went wrong.' }, 'whoami failed, please try again.');
    } finally {
      done();
      console.timeEnd(`whoami:${interaction.id}`);
    }
    return;
  }

  if (interaction.commandName === 'dumpme') {
    const ok = await safeDefer(interaction, { ephemeral: true });
    console.log(`[INT] ${interaction.commandName} deferred ok=${ok}`);
    if (!ok) {
      if (interaction.channel) {
        await interaction.channel.send('Sorry, that timed out. Please run `/dumpme` again.');
      }
      return;
    }
    const discordId = interaction.user.id;
    const uid = await getKCUidForDiscord(discordId);
    if (!uid) return safeEdit(interaction, { content: 'Not linked. Run `/link` first.' });

    try {
      const [userRT, badgesRT, postsRT] = await Promise.all([
        rtdb.ref(`users/${uid}`).get(),
        rtdb.ref(`badges/${uid}`).get(),
        rtdb.ref(`users/${uid}/posts`).get(),
      ]);

      const firestore = admin.firestore();
      const [userFS, postsFS] = await Promise.all([
        firestore.collection('users').doc(uid).get(),
        firestore.collection('users').doc(uid).collection('posts').get(),
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

      await safeEdit(interaction, { content: '```json\n' + JSON.stringify(payload, null, 2).slice(0, 1900) + '\n```' });
    } catch (e) {
      console.error('dumpme error:', e);
      await safeEdit(interaction, { content: 'Error reading data (see logs).' });
    }
    return;
  }

  if (interaction.commandName === 'leaderboard') {
    const ok = await safeDefer(interaction); // public
    console.log(`[INT] ${interaction.commandName} deferred ok=${ok}`);
    if (!ok) return;

    const done = watchdog(5000, () => {
      finalRespond(interaction,
        { content: 'Still working… one moment please.' },
        'Reply expired while working. Please try again.');
    });

    try {
      console.log('[INT] leaderboard -> loadLeaderboardData');
      const rows = await loadLeaderboardData();
      const catIdx = 0, page = 0;
      const embed = buildLbEmbed(rows, catIdx, page);
      await safeEdit(interaction, { embeds: [embed], components: [lbRow(catIdx, page)] })
        .then(msg => { 
            /* store rows in memory for button handler */ 
            interaction.client.lbCache ??= new Map(); 
            interaction.client.lbCache.set(interaction.id, rows); 
        });
    } catch (e) {
        console.error('leaderboard error:', e);
        await finalRespond(interaction, { content: 'Something went wrong.' }, 'leaderboard failed, please try again.');
    } finally {
        done();
    }
    return;
  }

  if (interaction.commandName === 'clips') {
    const ok = await safeDefer(interaction); // public
    if (!ok) return;

    const platform = (interaction.options.getString('platform') || 'all').toLowerCase();
    try {
      const all = await fetchAllPosts({ platform });
      if (!all.length) {
        return finalRespond(interaction, { content: 'No clips found.' }, 'No clips found.');
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
        .setDescription(lines.join('\n\n'));

      return finalRespond(interaction, { embeds: [embed] }, 'The reply expired—try `/clips` again.');
    } catch (e) {
      console.error('clips error:', e);
      return finalRespond(interaction, { content: 'Something went wrong.' }, 'clips failed, please try again.');
    }
  }

  if (interaction.commandName === 'messages') {
    const ok = await safeDefer(interaction); // public
    if (!ok) return;

    try {
      const [list, nameMap] = await Promise.all([fetchLatestMessages(10), getAllUserNames()]);
      const embed = buildMessagesEmbed(list, nameMap);

      // Cache for the buttons handler
      interaction.client.msgCache ??= new Map();
      interaction.client.msgCache.set(interaction.id, { list, nameMap });

      return safeEdit(interaction, { embeds: [embed], components: messageIndexRows(list.length || 0) });
    } catch (e) {
      console.error('messages error:', e);
      return finalRespond(interaction, { content: 'Something went wrong.' }, 'messages failed, please try again.');
    }
  }

  if (interaction.commandName === 'votingscores') {
    const ok = await safeDefer(interaction); // public
    if (!ok) return;

    try {
      const scores = await loadVoteScores();
      const embed = buildVoteEmbed(scores);
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId('votes:refresh').setLabel('Refresh').setStyle(ButtonStyle.Primary)
      );
      return safeEdit(interaction, { embeds: [embed], components: [row] });
    } catch (e) {
      console.error('votingscores error:', e);
      return finalRespond(interaction, { content: 'Something went wrong.' }, 'votingscores failed, please try again.');
    }
  }

  if (interaction.commandName === 'badges') {
    const ok = await safeDefer(interaction); // public
    console.log(`[INT] ${interaction.commandName} deferred ok=${ok}`);
    if (!ok) {
      if (interaction.channel) await interaction.channel.send('Sorry, that timed out. Please run `/badges` again.');
      return;
    }
    
    const done = watchdog(5000, () => {
      finalRespond(interaction,
        { content: 'Still working… one moment please.' },
        'Reply expired while working. Please try again.');
    });

    console.time(`badges:${interaction.id}`);
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
      const bonusVal    = clampStr(profile.bonus, 1024, '—');
      const streakVal   = clampStr(profile.streak, 1024, '—');
      const postsVal    = clampStr(profile.postsText, 1024);

      const discordAvatar = target.displayAvatarURL({ extension: 'png', size: 128 });

      const embed = new EmbedBuilder()
        .setTitle(title)
        .setThumbnail(discordAvatar)
        .setDescription(description)
        .addFields(
          { name: 'Badges', value: badgesVal, inline: false },
          { name: 'Bonus',  value: bonusVal,  inline: true  },
          { name: 'Streak', value: streakVal, inline: true  },
          { name: 'Posts',  value: postsVal,  inline: false },
        );

      if (profile.embedColor) embed.setColor(profile.embedColor);

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
      done();
      console.timeEnd(`badges:${interaction.id}`);
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
