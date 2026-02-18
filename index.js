// index.js
// Discord.js v14 + Firebase Admin (Realtime Database & Firestore)
// Commands: /link, /badges, /whoami, /dumpme, /leaderboard, /clips, /messages, /votingscores, /syncavatar, /post, /help, /vote, /compare, /setclipschannel, /clans, /clanbattles, /battledome, /bdtokc

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

// Battledome UI design system.
const BD_UI = {
  STATUS: { online: '🟢', idle: '🟡', offline: '🔴' },
  ICONS: {
    players: '👥',
    active: '🔥',
    time: '🕒',
    // Region flag icons; fallback to empty string if missing. West removed.
    region: { East: '🇺🇸', EU: '🇪🇺' }
  },
  COLORS: { online: 0x2ECC71, idle: 0xF1C40F, offline: 0xE74C3C }
};

// -----------------------------------------------------------------------------
// Slither server leaderboard constants and cache
const SLITHER = {
  URL: 'https://ntl-slither.com/ss/rs.php',
  TIMEOUT_MS: 8000,
  CACHE_MS: 15000,
};

const slitherCache = {
  fetchedAt: 0,
  servers: new Map(),
  rawLen: 0,
  lastError: null,
};

// Fetch plain text from a URL
async function fetchText(url, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs).unref?.();
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: { 'user-agent': 'KC-Discord-Bot/1.0' },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return await res.text();
  } finally {
    clearTimeout(t);
  }
}

// Parse the NTL real‑time server page
function parseNtlServers(html) {
  const text = html
    .replace(/<br\s*\/?\>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '');
  const lines = text
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean);
  const servers = new Map();
  let cur = null;
  const headerRe = /^(\d{3,6})\s+([0-9.]+:\d+)\s+-\s+(.+)$/;
  const lbRe = /^(\d{1,2})#\s*(.+)$/;
  for (const line of lines) {
    const h = line.match(headerRe);
    if (h) {
      const id = Number(h[1]);
      cur = {
        id,
        ipPort: h[2],
        region: h[3].trim(),
        serverTime: null,
        totalScore: null,
        totalPlayers: null,
        updated: null,
        leaderboard: [],
      };
      servers.set(id, cur);
      continue;
    }
    if (!cur) continue;
    if (/^server time:/i.test(line)) {
      cur.serverTime = line.replace(/^server time:\s*/i, '').trim();
      continue;
    }
    const lbm = line.match(lbRe);
    if (lbm) {
      const rank = Number(lbm[1]);
      const rest = lbm[2].trim();
      const m2 = rest.match(/^(.*?)(\d+)\s*$/);
      if (m2) {
        const name = (m2[1] || '').trim();
        const score = Number(m2[2]);
        cur.leaderboard.push({ rank, name: name || '(no name)', score });
      }
      continue;
    }
    if (/^total score:/i.test(line)) {
      cur.totalScore = Number(line.replace(/[^0-9]/g, '')) || null;
      continue;
    }
    if (/^total players:/i.test(line)) {
      cur.totalPlayers = Number(line.replace(/[^0-9]/g, '')) || null;
      continue;
    }
    if (/^updated:/i.test(line)) {
      cur.updated = line.replace(/^updated:\s*/i, '').trim();
      continue;
    }
  }
  for (const s of servers.values()) {
    s.leaderboard = s.leaderboard
      .filter((e) => e.rank >= 1 && e.rank <= 10)
      .sort((a, b) => a.rank - b.rank);
  }
  return servers;
}

async function getSlitherServersCached() {
  const now = Date.now();
  const age = now - slitherCache.fetchedAt;
  if (slitherCache.servers.size && age < SLITHER.CACHE_MS) {
    return {
      servers: slitherCache.servers,
      fromCache: true,
      fetchedAt: slitherCache.fetchedAt,
    };
  }
  try {
    const html = await fetchText(SLITHER.URL, SLITHER.TIMEOUT_MS);
    const servers = parseNtlServers(html);
    slitherCache.fetchedAt = Date.now();
    slitherCache.servers = servers;
    slitherCache.rawLen = html.length;
    slitherCache.lastError = null;
    return { servers, fromCache: false, fetchedAt: slitherCache.fetchedAt };
  } catch (e) {
    slitherCache.lastError = e.message;
    if (slitherCache.servers.size) {
      return {
        servers: slitherCache.servers,
        fromCache: true,
        fetchedAt: slitherCache.fetchedAt,
        error: e.message,
      };
    }
    throw e;
  }
}

// Simple in-memory cache
const globalCache = {
  userNames: new Map(), // uid -> displayName
  userNamesFetchedAt: 0,
  clipDestinations: new Map(), // guildId -> channelId
  battleDestinations: new Map(),
  bdDestinations: new Map(),
  bdJoinLogs: new Map(),
  // New caches for fix
  bdStatusMessages: new Map(), // guildId -> { channelId, messageId }
  bdToKC: new Map(), // discordId -> { kcUid, kcName, enabled }
  kcNameToUid: new Map(), // kcName -> kcUid
};

// only these will be private
const isEphemeralCommand = (name) =>
  new Set(['whoami', 'dumpme', 'help', 'vote', 'syncavatar', 'post', 'postmessage', 'link', 'setclipschannel', 'latestfive', 'notifybd', 'setbattledomechannel', 'setjoinlogschannel', 'bdtokc']).has(name);

// Parse #RRGGBB -> int
function hexToInt(hex) {
  if (!hex || typeof hex !== 'string') return null;
  const m = hex.match(/#([0-9a-f]{6})/i);
  return m ? parseInt(m[1], 16) : null;
}

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
  const result = await rtdb.ref(LOCK_KEY).transaction(cur => {
    if (!cur) {
      return { owner: OWNER_ID, expiresAt: now + LOCK_TTL_MS };
    }
    if (cur.expiresAt && cur.expiresAt < now) {
      return { owner: OWNER_ID, expiresAt: now + LOCK_TTL_MS };
    }
    return;
  }, undefined, false);

  const snap = await rtdb.ref(LOCK_KEY).get();
  const val = snap.val() || {};
  return val.owner === OWNER_ID;
}

async function renewBotLock() {
  const now = Date.now();
  await rtdb.ref(LOCK_KEY).transaction(cur => {
    if (!cur) return;
    if (cur.owner !== OWNER_ID) return;
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
    console.error("safeReply error", err);
  }
}

// --- Battledome Helpers (NEW) ---
const BD = {
  TIMEOUT_MS: 10000,
  MIN_FETCH_INTERVAL_MS: 3500,  // hard cooldown
  INFO_FRESH_MS: 6000,          // consider fresh
  INFO_STALE_MS: 60000,         // keep stale until
  LB_FRESH_MS: 15000,
  LB_STALE_MS: 5 * 60 * 1000,
  STATS_FRESH_MS: 5 * 60 * 1000,
  STATS_STALE_MS: 60 * 60 * 1000,
};

// Authoritative Server List (HTTP only) - West REMOVED
const BD_SERVERS = {
  East: {
    name: "East Coast Battledome",
    url: "http://206.221.176.241:444/bdinfo.json",
    region: "East"
  },
  EU: {
    name: "EU Battledome",
    url: "http://51.91.19.175:444/bdinfo.json",
    region: "EU"
  }
};

// Map of Server Name -> Region Key (West REMOVED)
const BD_NAME_OVERRIDES = {
  "East Coast Battledome": "East",
  "New York Battledome": "East",
  "EU Battledome": "EU",
};

// Map region -> state for polling (West REMOVED)
const bdState = {
  East: { lastNames: new Set(), lastOnline: 0, lastIndome: 0, lastCheck: 0 },
  EU:   { lastNames: new Set(), lastOnline: 0, lastIndome: 0, lastCheck: 0 },
};

// Persist the most recently fetched info for each region (West REMOVED)
const bdLastInfo  = { East: null, EU: null };
const bdLastFetch = { East: 0,    EU: 0    };

// Status Message Logic
let bdStatusUpdateInFlight = null;

const bdRecent = { East: [], EU: [] };
const BD_RECENT_WINDOW_MS = 15 * 60 * 1000;

function pruneBdRecent() {
  const cutoff = Date.now() - BD_RECENT_WINDOW_MS;
  for (const region of Object.keys(bdRecent)) {
    const list = bdRecent[region];
    let idx = 0;
    while (idx < list.length && list[idx].time < cutoff) {
      idx++;
    }
    if (idx > 0) {
      bdRecent[region] = list.slice(idx);
    }
  }
}

// Helper: Format leaderboard nicely (Code block table)
function formatLeaderboard(players, limit) {
  if (!players || players.length === 0) return '_No players_';
  const top = players.slice(0, limit);
  const lines = top.map((p, i) => {
    const name = (p.name || '').substring(0, 18).padEnd(19, ' ');
    const score = String(p.score).padStart(5, ' ');
    return `${String(i + 1).padEnd(2, ' ')}. ${name} ${score}`;
  });
  return '```\n' + lines.join('\n') + '\n```';
}

function buildBdStatusEmbeds() {
    // Legacy function, kept if needed but we use Unified mostly now.
    // Updated to remove West loop.
    const embeds = [];
    for (const region of ['East','EU']) {
        // ... (implementation matches unified logic but per region)
        // Skipping full implementation as Unified is used.
    }
    return embeds; 
}

// Unified Status Embed (No West, Formatting updated)
function buildBdStatusUnifiedEmbed(options = {}) {
  const showAdvanced = options.showAdvanced || false;
  const limit = showAdvanced ? 10 : 5; // Top 5 vs Top 10 logic

  const embed = new EmbedBuilder();
  embed.setTitle('🏟️ Battledome Status');
  let maxAgeSec = 0;
  let worstStatus = 'online';

  for (const region of ['East', 'EU']) {
    const info = bdLastInfo[region];
    const fetchedAt = bdLastFetch[region];
    const serverName = BD_SERVERS[region]?.name || region;
    const lines = [];

    if (info) {
      const onNow = info.onlinenow ?? 0;
      const inDome = info.indomenow ?? 0;
      let status = 'offline';
      if (onNow > 0) status = (inDome > 0 ? 'online' : 'idle');
      
      if (status === 'offline') worstStatus = 'offline';
      else if (status === 'idle' && worstStatus !== 'offline') worstStatus = 'idle';

      const pct = onNow > 0 ? Math.floor((inDome / onNow) * 100) : 0;
      lines.push(`${BD_UI.STATUS[status]} ${status.charAt(0).toUpperCase() + status.slice(1)}`);
      lines.push(`${BD_UI.ICONS.players} Online: ${onNow}`);
      lines.push(`${BD_UI.ICONS.active} In Dome: ${inDome} (${pct}%)`);
      lines.push(`${BD_UI.ICONS.time} This Hour: ${info.thishour ?? 0}`);

      // Leaderboard on new paragraph/block
      const players = Array.isArray(info.players) ? info.players : [];
      lines.push(formatLeaderboard(players, limit));

      if (fetchedAt) {
        const age = Math.floor((Date.now() - fetchedAt) / 1000);
        if (age > maxAgeSec) maxAgeSec = age;
      }
    } else {
      lines.push(`${BD_UI.STATUS.offline} Offline`);
      lines.push('_Cache warming up..._');
    }

    const regionIcon = BD_UI.ICONS.region[region] || '';
    embed.addFields({ name: `${regionIcon} ${serverName}`, value: lines.join('\n'), inline: false });
  }

  const updatedStr = maxAgeSec > 0 ? (maxAgeSec < 2 ? 'just now' : `${maxAgeSec}s ago`) : 'warming up';
  embed.setDescription(`Monitoring ${Object.keys(BD_SERVERS).length} server(s) • Updated ${updatedStr}`);
  embed.setColor(BD_UI.COLORS[worstStatus]);
  return embed;
}

function buildBdRecentUnifiedEmbed() {
  pruneBdRecent();
  const embed = new EmbedBuilder()
    .setTitle('🧾 Recent Battledome Activity')
    .setColor(DEFAULT_EMBED_COLOR);
  
  // West Removed
  for (const region of ['East','EU']) {
    const info = bdLastInfo[region];
    const events = bdRecent[region] || [];
    const joined = events.filter(e => e.type === 'join');
    const left   = events.filter(e => e.type === 'leave');
    const lines = [];
    if (info) {
      lines.push(`${BD_UI.ICONS.players} Online: ${info.onlinenow ?? '—'}`);
      lines.push(`${BD_UI.ICONS.active} In Dome: ${info.indomenow ?? '—'}`);
    }
    if (joined.length) {
      const joinLines = joined.map(e => `• **${e.name}** — <t:${Math.floor(e.time/1000)}:R>`);
      lines.push(`**Joined (${joined.length})**`);
      lines.push(joinLines.join('\n'));
    }
    if (left.length) {
      const leaveLines = left.map(e => `• **${e.name}** — <t:${Math.floor(e.time/1000)}:R>`);
      lines.push(`**Left (${left.length})**`);
      lines.push(leaveLines.join('\n'));
    }
    if (joined.length === 0 && left.length === 0) {
        lines.push('_No recent activity._');
    }

    const serverName = BD_SERVERS[region]?.name || region;
    const regionIcon = BD_UI.ICONS.region[region] || '';
    embed.addFields({ name: `${regionIcon} ${serverName}`, value: lines.join('\n'), inline: false });
  }
  return embed;
}

function buildBdScoreCompareEmbed() {
  function topEntries(map) {
    const arr = [];
    map.forEach((val, key) => {
      const score = (val && typeof val === 'object') ? (val.score ?? 0) : (Number(val) || 0);
      arr.push({ name: key, score });
    });
    arr.sort((a, b) => (b.score || 0) - (a.score || 0));
    return arr.slice(0, 10);
  }
  const eastTop   = topEntries(bdTop.East);
  const euTop     = topEntries(bdTop.EU);
  // West Removed
  const fmt = list => list.map((p, i) => {
    const rank = String(i + 1).padStart(2, ' ');
    const name = clampName(p.name, 25).padEnd(27);
    const score = String(p.score).padStart(5);
    return `\`${rank}.\` **${name}** \`— ${score}\``;
  }).join('\n') || '_No scores recorded._';
  const embed = new EmbedBuilder()
    .setTitle('🏆 Top Battledome Scores Comparison')
    .setColor(DEFAULT_EMBED_COLOR);
  embed.addFields(
    { name: `${BD_UI.ICONS.region.East || ''} East`, value: fmt(eastTop),   inline: false },
    { name: `${BD_UI.ICONS.region.EU || ''} EU`,     value: fmt(euTop),     inline: false }
  );
  const lastUpdate = bdTopMeta.lastUpdatedAt || Date.now();
  embed.setFooter({ text: `Updated <t:${Math.floor(lastUpdate/1000)}:R>` });
  return embed;
}

// -----------------------------------------------------------------------------
function bdServerHost(region) {
  try {
    return new URL(BD_SERVERS[region]?.url || '').host || 'Unknown';
  } catch {
    return 'Unknown';
  }
}

async function postJoinLogsBlock(guildId, region, { joins = [], leaves = [] }) {
  const joinLogsId = globalCache.bdJoinLogs?.get(guildId);
  if (!joinLogsId) return;
  const chan = await client.channels.fetch(joinLogsId).catch(() => null);
  if (!chan || !chan.isTextBased?.()) return;
  const serverName = BD_SERVERS[region]?.name || region;
  const regionIcon = BD_UI.ICONS.region?.[region] || '';
  const host = bdServerHost(region);
  const embeds = [];
  if (joins.length > 0) {
    const list = joins.slice(0, 10).map(n => `• **${n}**`).join('\n');
    const more = joins.length > 10 ? `\n… +${joins.length - 10} more` : '';
    embeds.push(
      new EmbedBuilder()
        .setTitle('✅ Battledome Join Log')
        .setColor(0x2ECC71)
        .setDescription(
          `${regionIcon} **${serverName}**\n` +
          `🖥️ Server: **${host}**\n` +
          `${BD_UI.ICONS.players} Joined: **${joins.length}**\n\n` +
          (list || '_No named joins_') + more
        )
        .setFooter({ text: `Updated ${new Date().toLocaleString()}` })
    );
  }
  if (leaves.length > 0) {
    const list2 = leaves.slice(0, 10).map(n => `• **${n}**`).join('\n');
    const more2 = leaves.length > 10 ? `\n… +${leaves.length - 10} more` : '';
    embeds.push(
      new EmbedBuilder()
        .setTitle('❌ Battledome Leave Log')
        .setColor(0xE74C3C)
        .setDescription(
          `${regionIcon} **${serverName}**\n` +
          `🖥️ Server: **${host}**\n` +
          `${BD_UI.ICONS.players} Left: **${leaves.length}**\n\n` +
          (list2 || '_No named leaves_') + more2
        )
        .setFooter({ text: `Updated ${new Date().toLocaleString()}` })
    );
  }
  if (embeds.length) {
    await chan.send({ embeds });
  }
}

function buildBdDashboardActionRows(guildId, showAdvanced = false) {
  const controlsBtn = new ButtonBuilder()
    .setCustomId(`bd:controls:${guildId}`)
    .setLabel('⚙️ Controls')
    .setStyle(ButtonStyle.Secondary);
  const refreshBtn = new ButtonBuilder()
    .setCustomId(`bd:refresh:${guildId}`)
    .setLabel('🔄 Refresh')
    .setStyle(ButtonStyle.Primary);
  const detailsBtn = new ButtonBuilder()
    .setCustomId(`bd:details:${guildId}`)
    .setLabel('📊 Details')
    .setStyle(ButtonStyle.Secondary);
  const row = new ActionRowBuilder().addComponents(controlsBtn, refreshBtn, detailsBtn);
  return [row];
}

// Updated Status Message logic with Mutex and Persistence (A2)
async function updateBdStatusMessages() {
  if (bdStatusUpdateInFlight) return bdStatusUpdateInFlight;

  bdStatusUpdateInFlight = (async () => {
    for (const [guildId, channelId] of globalCache.bdDestinations.entries()) {
      try {
        const channel = await client.channels.fetch(channelId).catch(() => null);
        if (!channel || !channel.isTextBased?.()) continue;

        let showAdvanced = false;
        try {
          const snap = await rtdb.ref(`config/bdShowAdvanced/${guildId}`).get();
          if (snap.exists() && snap.val() === true) showAdvanced = true;
        } catch {}

        const embed = buildBdStatusUnifiedEmbed({ showAdvanced });
        const components = buildBdDashboardActionRows(guildId, showAdvanced);
        
        // Retrieve persistent message info
        const cached = globalCache.bdStatusMessages.get(guildId);
        let sentMsg = null;

        if (cached && cached.channelId === channelId && cached.messageId) {
            try {
                // Attempt edit
                const msg = await channel.messages.fetch(cached.messageId);
                sentMsg = await msg.edit({ content: '', embeds: [embed], components });
            } catch (e) {
                // Failed to edit (deleted/perms), will fall through to send new
                // console.warn(`[BD Status] Edit failed for ${guildId}, sending new.`);
            }
        }

        if (!sentMsg) {
            sentMsg = await channel.send({ content: '', embeds: [embed], components });
        }

        if (sentMsg) {
            // Update cache and persistence
            const record = { channelId, messageId: sentMsg.id, updatedAt: Date.now() };
            globalCache.bdStatusMessages.set(guildId, record);
            // Fire & forget save
            rtdb.ref(`config/bdStatusMessage/${guildId}`).set(record).catch(()=>{});
        }

      } catch (e) {
        console.error(`[BD Status] Failed for guild ${guildId}:`, e.message);
      }
    }
  })().finally(() => { bdStatusUpdateInFlight = null; });

  return bdStatusUpdateInFlight;
}

// --- Cache Globals ---
const bdFetchCache = new Map();
// West Removed
const bdTop = {
  global: new Map(),
  East: new Map(),
  EU: new Map(),
};
const bdTopMeta = { lastUpdatedAt: 0, seededHardcoded: false };
let bdTopDirty = false;

async function swrFetch(key, { fetcher, freshMs, staleMs, minIntervalMs }) {
  const now = Date.now();
  let entry = bdFetchCache.get(key);
  if (!entry) {
    entry = { data: null, fetchedAt: 0, lastAttemptAt: 0, inFlight: null, lastError: null };
    bdFetchCache.set(key, entry);
  }

  const age = now - entry.fetchedAt;
  const isFresh = entry.data && age <= freshMs;
  const isStale = entry.data && age <= staleMs;

  const doFetch = () => {
    if (entry.inFlight) return entry.inFlight;
    entry.lastAttemptAt = Date.now();
    entry.inFlight = (async () => {
      try {
        // console.log(`[SWR] Fetching real data for ${key}`);
        const res = await fetcher();
        entry.data = res;
        entry.fetchedAt = Date.now();
        entry.lastError = null;
        return res;
      } catch (e) {
        entry.lastError = e.message;
        throw e;
      } finally {
        entry.inFlight = null;
      }
    })();
    return entry.inFlight;
  };

  if (isFresh) {
    return { data: entry.data, fromCache: true, stale: false, fetchedAt: entry.fetchedAt };
  }

  if (isStale) {
    const timeSinceAttempt = now - entry.lastAttemptAt;
    if (!entry.inFlight && timeSinceAttempt >= minIntervalMs) {
      doFetch().catch(e => console.warn(`[SWR] BG fail ${key}: ${e.message}`));
    }
    return { data: entry.data, fromCache: true, stale: true, fetchedAt: entry.fetchedAt };
  }

  if (entry.inFlight) {
      try {
          const data = await entry.inFlight;
          return { data, fromCache: false, stale: false, fetchedAt: entry.fetchedAt };
      } catch (e) {
          if (entry.data) return { data: entry.data, fromCache: true, stale: true, fetchedAt: entry.fetchedAt, error: e.message };
          throw e;
      }
  }

  const timeSinceAttempt = now - entry.lastAttemptAt;
  if (timeSinceAttempt < minIntervalMs) {
      if (entry.data) return { data: entry.data, fromCache: true, stale: true, fetchedAt: entry.fetchedAt };
      await new Promise(r => setTimeout(r, minIntervalMs - timeSinceAttempt));
  }

  try {
      const data = await doFetch();
      return { data, fromCache: false, stale: false, fetchedAt: entry.fetchedAt };
  } catch (e) {
      if (entry.data) return { data: entry.data, fromCache: true, stale: true, fetchedAt: entry.fetchedAt, error: e.message };
      throw e;
  }
}

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

function parseBdInfo(json) {
  if (!json || typeof json !== 'object') return null;
  let players = Array.isArray(json.players) ? json.players : [];
  players = players.map(p => ({
    name: p.name || '(unknown)',
    score: parseInt(p.score, 10) || 0,
    rank: p.rank,
    indomenow: p.indomenow,
    inactive: p.inactive
  })).filter(p => p.name !== '(unknown)'); 
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

// RTDB Safe Key Helper (Fix for Instruction 1)
function toRtdbKey(raw) {
  const s = String(raw ?? '').trim();
  if (!s) return '_';
  // encodeURIComponent handles # $ / [ ]
  // replace . with %2E manually
  return encodeURIComponent(s).replace(/\./g, '%2E');
}

function updateBdTopScore(regionKey, name, score, serverName = "") {
  const clean = String(name || "").trim();
  if (!clean) return;

  const rec = { score: Number(score) || 0, seenAt: Date.now(), serverName, name: clean };
  let changed = false;

  // Update region map
  if (bdTop[regionKey] instanceof Map) {
    const cur = bdTop[regionKey].get(clean);
    const curScore = (cur && typeof cur === "object") ? (cur.score ?? 0) : (Number(cur) || 0);
    if (rec.score > curScore) {
      bdTop[regionKey].set(clean, rec);
      changed = true;
    }
  }

  // Update global map (West was removed from here anyway, now West is gone entirely)
  const cur = bdTop.global.get(clean);
  const curScore = (cur && typeof cur === 'object') ? (cur.score ?? 0) : (Number(cur) || 0);
  if (rec.score > curScore) {
    bdTop.global.set(clean, rec);
    changed = true;
  }

  // Feature F3: Check /bdtokc linking
  const kcUid = globalCache.kcNameToUid.get(clean);
  if (kcUid) {
      // User has opted in for this specific name
      // Write highscore to users/<uid>/bdHighscore if greater
      rtdb.ref(`users/${kcUid}/bdHighscore`).transaction(current => {
          if (current === null || rec.score > current) return rec.score;
          return; // abort
      }).catch(e => console.error('[BD-KC Sync] Failed:', e.message));
  }

  if (changed) {
    bdTopMeta.lastUpdatedAt = Date.now();
    bdTopDirty = true;
  }
}

function updateBdTop(regionKey, players) {
  if (!Array.isArray(players)) return;
  for (const p of players) {
     if (!p.name || p.score == null) continue;
     updateBdTopScore(regionKey, p.name, p.score, regionKey);
  }
}

async function getBdInfoCached(url) {
    let region = 'East'; // default fallback
    for(const [k,v] of Object.entries(BD_SERVERS)) {
        if (v.url === url) region = k;
    }
    
    return swrFetch(url, {
        fetcher: async () => {
            const data = await fetchBdInfo(url);
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

const bdManualRefreshAt = new Map();

async function announceBdCacheUpdated(regionKey, fetchedAt) {
  return;
}

// Warm caches forever loop (West Removed)
async function warmBdCachesForever() {
  const order = ['EU', 'East'];
  while (true) {
    for (const regionKey of order) {
      const url = BD_SERVERS[regionKey]?.url;
      if (!url) continue;
      try {
        const r = await getBdInfoCached(url);
        if (r && r.data && !r.fromCache) {
          announceBdCacheUpdated(regionKey, r.fetchedAt).catch(() => {});
        }
      } catch (e) {
        console.warn(`[BD Warm] ${regionKey} failed: ${e.message}`);
      }
      await new Promise(res => setTimeout(res, Math.max(3500, BD.MIN_FETCH_INTERVAL_MS)));
    }
  }
}

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


function seedHardcodedTopScoresOnce() {
  if (bdTopMeta.seededHardcoded) return;
  for (const [regionKey, arr] of Object.entries(HARDCODED_TOP_SCORES_BY_REGION)) {
    for (const e of (arr || [])) {
      updateBdTopScore(regionKey, e.name, e.score, "Seed (hardcoded)");
    }
  }
  bdTopMeta.seededHardcoded = true;
  bdTopDirty = true;
}

function clampName(s, n=45){
  s = String(s || "");
  s = s.replace(/\s+/g, " ").trim();
  return s.length > n ? (s.slice(0, n - 1) + "…") : s;
}

// Persistence for Top Scores (Fix for Instruction 1)
async function loadBdTop() {
    try {
        const snap = await rtdb.ref('config/bdTopScores').get();
        if (snap.exists()) {
            const val = snap.val();
            // West Removed
            ['global', 'East', 'EU'].forEach(k => {
                if (val[k]) {
                    // Fix C: Restore Map using entry.name if present
                    for (const [storedKey, entry] of Object.entries(val[k])) {
                        const displayName = (entry && typeof entry === 'object' && entry.name) ? entry.name : storedKey;
                        bdTop[k].set(displayName, entry);
                    }
                }
            });
            console.log('[BdTop] Loaded scores from RTDB');
        }
        seedHardcodedTopScoresOnce();
    } catch (e) {
        console.error('[BdTop] Failed to load:', e.message);
    }
}

async function saveBdTop() {
    // Fix B: Use safe keys + store original name
    const payload = {};
    // West Removed
    for (const k of ['global', 'East', 'EU']) {
        payload[k] = {};
        for (const [name, entry] of bdTop[k].entries()) {
             const key = toRtdbKey(name);
             payload[k][key] = {
                 ...(typeof entry === 'object' && entry ? entry : { score: Number(entry) || 0 }),
                 name, // Original name preserved
             };
        }
    }

    try {
        await rtdb.ref('config/bdTopScores').set(payload);
    } catch (e) {
        console.error('[BdTop] Save failed:', e.message);
    }
}
// --- End Battledome Helpers ---

async function hasEmerald(uid) {
  try {
    const snap = await rtdb.ref(`users/${uid}/codesUnlocked`).get();
    const codes = snap.exists() ? (snap.val() || {}) : {};
    if (codes.emerald === true || codes.diamond === true || codes.content === true) return true;
  } catch (_) {}
  try {
    const fsDoc = await admin.firestore().collection('users').doc(uid).get();
    if (fsDoc.exists) {
      const u = fsDoc.data() || {};
      if (u.codesUnlocked?.emerald === true || u.codesUnlocked?.diamond === true || u.postsUnlocked === true || u.canPost === true) {
        return true;
      }
    }
  } catch (_) {}
  return false;
}

async function setKCAvatar(uid, url) {
  await rtdb.ref(`users/${uid}`).update({
    avatar: url,
    photoURL: url,
    avatarSource: 'discord',
    avatarUpdatedAt: Date.now(),
  });
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
  await rtdb.ref(`users/${uid}`).update({
    avatar: null,
    photoURL: null,
    avatarSource: null,
    avatarUpdatedAt: Date.now(),
  });
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
  const yt = link.match(/(?:youtu\.be\/|youtube\.com\/(?:watch\?.*v=|embed\/|v\/|shorts\/))([\w-]{11})/);
  if (yt) return { type: 'youtube', ytId: yt[1] };
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
  return null;
}
function reactCount(reactions={}) {
  const perEmoji = {};
  for (const e of Object.keys(reactions||{})) perEmoji[e] = Object.keys(reactions[e]||{}).length;
  const total = Object.values(perEmoji).reduce((a,b)=>a+b,0);
  return { perEmoji, total };
}

function getClipsState(interaction) {
  interaction.client.clipsCache ??= new Map();
  const key = interaction.message?.interaction?.id || interaction.id;
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
  try {
    const snap = await withTimeout(rtdb.ref(`users/${uid}`).get(), 6000, `RTDB users/${uid}`);
    const u = snap.exists() ? (snap.val() || {}) : {};
    const codes = u.codesUnlocked || {};
    return !!(codes.emerald || codes.diamond || codes.content || u.postsUnlocked || u.canPost);
  } catch {
    return false;
  }
}

async function postsDisabledGlobally() {
  try {
    const s = await withTimeout(rtdb.ref('config/postsDisabled').get(), 4000, 'RTDB config/postsDisabled');
    return !!s.val();
  } catch { return false; }
}

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

const clamp = (s, n=100) => (s || '').toString().slice(0, n);
function normalize(name=''){ return name.toLowerCase().replace(/[^a-z0-9]/g,''); }

function countReactions(reactionsObj = {}) {
  let n = 0;
  for (const emo of Object.keys(reactionsObj || {})) {
    n += Object.keys(reactionsObj[emo] || {}).length;
  }
  return n;
}

function countComments(commentsObj = {}) {
  let n = 0;
  for (const cid of Object.keys(commentsObj || {})) {
    n += 1;
    const r = commentsObj[cid]?.replies || {};
    n += Object.keys(r).length;
  }
  return n;
}

async function getAllUserNames() {
  const CACHE_DURATION = 60 * 1000;
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
      if (post.draft) return;
      if (post.publishAt && Date.now() < post.publishAt) return;

      const type = (post.type || '').toLowerCase();
      if (platform === 'youtube' && type !== 'youtube') return;
      if (platform === 'tiktok' && type !== 'tiktok') return;

      const reacts = countReactions(post.reactions || {});
      const comments = countComments(post.comments || {});
      const score = reacts + comments * 2;

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
        const isMsg = typeof v === 'object' && (typeof v.text === 'string' || typeof v.user === 'string' || typeof v.uid === 'string');
        if (isMsg) arr.push({ key: c.key, ...(v || {}) });
      });
      arr.sort((a, b) => ((b.time || 0) - (a.time || 0)) || (b.key > a.key ? 1 : -1));
    }
    return arr.slice(0, limit).map(m => ({ ...m, path: `messages/${m.key}` }));
  }
  try {
    const snap = await withTimeout(rtdb.ref('messages').orderByChild('time').limitToLast(OVERFETCH).get(), 8000, 'RTDB messages recent');
    return await snapToMsgs(snap);
  } catch (e) {
    const snap = await withTimeout(rtdb.ref('messages').limitToLast(OVERFETCH).get(), 8000, 'RTDB messages fallback');
    return await snapToMsgs(snap);
  }
}

function buildMessagesEmbed(list, nameMap) {
  const desc = list.map((m, i) => {
    const who = m.user || nameMap.get(m.uid) || m.username || m.displayName || m.name || '(unknown)';
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

function buildMessageDetailEmbed(msg, nameMap) {
  const who = msg.user || nameMap.get(msg.uid) || msg.username || msg.displayName || msg.name || '(unknown)';
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
    const who = r.user || nameMap.get(r.uid) || r.username || r.displayName || r.name || '(unknown)';
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
    if (b.scheduledTime) {
      const d = new Date(b.scheduledTime);
      const dateStr = d.toLocaleDateString('en-GB', { timeZone: 'Europe/London' });
      const timeStr = d.toLocaleTimeString('en-GB', { timeZone: 'Europe/London', hour: '2-digit', minute: '2-digit' });
      line += ` — ${dateStr} ${timeStr}`;
    }
    if (filterType === 'past' || b.status === 'finished') {
      const win = clansData[b.winnerId] || {};
      if (win.name) line += ` — Winner: ${win.name}`;
    } else {
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

function buildBattleDetailEmbed(battleId, battle = {}, clansData = {}, usersData = {}, includeDesc = false) {
  const c1 = clansData[battle.challengerId] || {};
  const c2 = clansData[battle.targetId] || {};
  const title = `${c1.name || 'Unknown'} vs ${c2.name || 'Unknown'}`;
  const embed = new EmbedBuilder()
    .setTitle(title)
    .setColor(DEFAULT_EMBED_COLOR)
    .setFooter({ text: 'KC Bot • /clanbattles' });

  if (battle.scheduledTime) {
    const d = new Date(battle.scheduledTime);
    const dateStr = d.toLocaleDateString('en-GB', { timeZone: 'Europe/London' });
    const timeStr = d.toLocaleTimeString('en-GB', { timeZone: 'Europe/London', hour: '2-digit', minute: '2-digit' });
    embed.setDescription(`Scheduled for **${dateStr} ${timeStr}**`);
  }
  embed.addFields({ name: 'Server', value: battle.server || 'N/A', inline: true });
  embed.addFields({ name: 'Rules', value: battle.rules || 'N/A', inline: true });
  const parts = battle.participants || {};
  const partNames = Object.keys(parts).map(uid => {
    const u = usersData[uid] || {};
    return u.displayName || u.username || u.email || uid;
  });
  const partCount = partNames.length;
  let partValue = partCount > 0 ? partNames.join(', ') : 'No participants yet';
  if (partValue.length > 1024) {
    partValue = partNames.slice(0, 30).join(', ') + ` … (+${partCount - 30} more)`;
  }
  embed.addFields({ name: `Participants (${partCount})`, value: partValue, inline: false });
  if (battle.status === 'finished') {
    const win = clansData[battle.winnerId] || {};
    const winName = win.name || 'Unknown';
    embed.addFields({ name: 'Winner', value: winName, inline: false });
  }

  if (includeDesc) {
    const descText = battle.description || battle.desc || battle.rules || null;
    if (descText) {
      let val = String(descText);
      if (val.length > 1024) val = val.slice(0, 1021) + '…';
      embed.addFields({ name: 'Description', value: val, inline: false });
    }
  }
  return embed;
}

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
        .setCustomId(`c:o:${start+i}`)
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
  if (thumb) e.setImage(thumb);

  return e;
}

function clipsDetailRows(interactionOrMessage, postPath) {
  const rows = [];
  const row1 = new ActionRowBuilder();
  const client = interactionOrMessage.client;
  const message = interactionOrMessage.message || interactionOrMessage;

  for (const emo of POST_EMOJIS) {
    const sid = _cacheForMessage(client.reactCache, message, { postPath, emoji: emo });
    row1.addComponents(
      new ButtonBuilder()
        .setCustomId(`clips:react:${sid}`)
        .setLabel(emo)
        .setStyle(ButtonStyle.Secondary)
    );
  }
  if (row1.components.length) rows.push(row1);

  const sidView = _cacheForMessage(client.reactorsCache, message, { postPath });
  const sidComments = _cacheForMessage(client.commentsCache, message, { postPath, page: 0 });
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
      .setCustomId('clips:back')
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
            user: data.user,
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
            .setCustomId('clips:backDetail')
            .setLabel('Back to clip')
            .setStyle(ButtonStyle.Secondary)
    );
    return row;
}

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
      8000,
      `RTDB discordLinks/${discordId}`
    );
    return snap.exists() ? (snap.val() || {}).uid || null : null;
  } catch {
    return null;
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

  const [userSnapRT, badgeSnapRT, postsSnapRT, bdHighscoreSnapRT] = await Promise.allSettled([
    withTimeout(rtdb.ref(`users/${uid}`).get(), 6000, `RTDB users/${uid}`),
    withTimeout(rtdb.ref(`badges/${uid}`).get(), 6000, `RTDB badges/${uid}`),
    withTimeout(rtdb.ref(`users/${uid}/posts`).get(), 6000, `RTDB users/${uid}/posts`),
    withTimeout(rtdb.ref(`users/${uid}/bdHighscore`).get(), 6000, `RTDB bdHighscore`),
  ]);

  const safeVal = s => (s && s.status === 'fulfilled' && s.value && s.value.exists()) ? s.value.val() : null;
  let user   = safeVal(userSnapRT)  || {};
  let badges = safeVal(badgeSnapRT) || {};
  let posts  = safeVal(postsSnapRT) || {};
  let bdHighscore = safeVal(bdHighscoreSnapRT);

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

  const about = user.about ?? user.aboutMe ?? user.bio ?? 'No "About Me" set.';
  const displayName = user.displayName ?? user.name ?? user.username ?? 'Anonymous User';
  const streak = Number.isFinite(user.loginStreak) ? String(user.loginStreak) : '—';
  const custom = user.profileCustomization || {};
  const nameColor = custom.nameColor || null;
  const gradientColor = custom.gradient ? firstHexFromGradient(custom.gradient) : null;
  const codesUnlocked = user.codesUnlocked || {};
  const postingAllowed = !!(codesUnlocked.content || codesUnlocked.diamond || codesUnlocked.emerald || user.postsUnlocked || user.canPost);

  let postLines = [];
  if (postingAllowed && posts) {
    const list = Object.entries(posts)
      .filter(([,p]) => p && !p.draft && (!p.publishAt || p.publishAt < Date.now()))
      .sort((a, b) => (b[1]?.createdAt || 0) - (a[1]?.createdAt || 0))
      .slice(0, 3);

    for (const [, p] of list) {
      const cap = (p?.caption || '').trim();
      let link = '';
      if (p?.type === 'youtube' && p?.ytId) {
        link = `https://youtu.be/${p.ytId}`;
      } else if (p?.type === 'tiktok' && p?.videoId) {
        link = `https://www.tiktok.com/embed/v2/${p.videoId}`;
      }
      const capPretty = cap ? `"${cap.slice(0, 80)}"` : '(no caption)';
      postLines.push(`• ${capPretty}${link ? ` — ${link}` : ''}`);
    }
  }
  const postsField = !postingAllowed
      ? 'Posts locked. Unlock posting on your profile.'
      : (Object.keys(posts).length === 0 ? 'This user has no posts.' : (postLines.join('\n') || 'This user has no posts.'));

  const counts = {
    offence: parseInt(badges.offence ?? badges.bestOffence ?? 0) || 0,
    defence: parseInt(badges.defence ?? badges.bestDefence ?? 0) || 0,
    overall: parseInt(badges.overall  ?? badges.overallWins  ?? 0) || 0,
  };
  const isVerified = !!(user.emailVerified === true || (user.badges && user.badges.verified === true));
  const hasDiamond = !!codesUnlocked.diamond;
  const hasEmerald = !!codesUnlocked.emerald;

  const e = EMOJI;
  const badgeLines = [];
  if (isVerified)                  badgeLines.push(`${e.verified ?? '✅'} Verified`);
  if (counts.offence > 0)          badgeLines.push(`${e.offence ?? '🏹'} Best Offence x${counts.offence}`);
  if (counts.defence > 0)          badgeLines.push(`${e.defence ?? '🛡️'} Best Defence x${counts.defence}`);
  if (counts.overall > 0)          badgeLines.push(`${e.overall  ?? '🌟'} Overall Winner x${counts.overall}`);
  if (hasDiamond)                  badgeLines.push(`${e.diamond ?? '💎'} Diamond User`);
  if (hasEmerald)                  badgeLines.push(`${e.emerald ?? '🟩'} Emerald User`);

  // F4: Show Battledome Highscore on profile
  if (bdHighscore) {
      badgeLines.push(`🏆 BD Highscore: ${bdHighscore}`);
  }

  const customBadges = user.customBadges || {};
  for (const key of Object.keys(customBadges)) {
    const b = customBadges[key] || {};
    const piece = [b.icon ?? b.emoji, b.name ?? b.label].filter(Boolean).join(' ');
    if (piece) badgeLines.push(piece);
  }

  return {
    id: uid,
    displayName,
    about,
    streak,
    badgesText: badgeLines.length ? badgeLines.join('\n') : 'No badges yet.',
    postsText: postsField,
    postingUnlocked: postingAllowed,
    embedColor: hexToInt(nameColor) || hexToInt(gradientColor) || null,
  };
}

// ---------- Discord Client ----------
const client = new Client({
  intents: [GatewayIntentBits.Guilds],
  partials: [Partials.Channel],
});

client.helpCache = new Map();
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
  const msgId = message?.id;
  if (!msgId) return null;
  return map.get(`${msgId}|${shortId}`) || null;
}

// ---------- Slash Commands ----------
const linkCmd = new SlashCommandBuilder().setName('link').setDescription('Link your Discord to your KC Events account');
const badgesCmd = new SlashCommandBuilder().setName('badges').setDescription('Show a KC Events profile').addUserOption(opt => opt.setName('user').setDescription('Show someone else').setRequired(false));
const whoamiCmd = new SlashCommandBuilder().setName('whoami').setDescription('Show your Discord ID and resolved KC UID');
const dumpCmd = new SlashCommandBuilder().setName('dumpme').setDescription('Debug: dump raw keys for your mapped KC UID');
const lbCmd = new SlashCommandBuilder().setName('leaderboard').setDescription('Show the live KC Events leaderboard');
const clipsCmd = new SlashCommandBuilder().setName('clips').setDescription('Top 5 most popular clips').addStringOption(o => o.setName('platform').setDescription('Filter by platform').addChoices({ name:'All', value:'all' }, { name:'YouTube', value:'youtube' }, { name:'TikTok', value:'tiktok' }).setRequired(false));
const latestFiveCmd = new SlashCommandBuilder().setName('latestfive').setDescription('Post the 5 most recently uploaded clips here').addStringOption(o => o.setName('platform').setDescription('Filter by platform').addChoices({ name: 'All', value: 'all' }, { name: 'YouTube', value: 'youtube' }, { name: 'TikTok', value: 'tiktok' }).setRequired(false));
const messagesCmd = new SlashCommandBuilder().setName('messages').setDescription('Show the latest 10 messageboard posts');
const votingCmd = new SlashCommandBuilder().setName('votingscores').setDescription('Show current live voting scores (Offence/Defence)');
const avatarCmd = new SlashCommandBuilder().setName('syncavatar').setDescription('Use your Discord avatar on KC (Emerald users)').addStringOption(o => o.setName('action').setDescription('Choose what to do').addChoices({ name: 'Set (use Discord avatar)', value: 'set' }, { name: 'Revert (remove override)', value: 'revert' }).setRequired(true));
const postCmd = new SlashCommandBuilder().setName('post').setDescription('Create a YouTube or TikTok post on your KC profile').addStringOption(o => o.setName('link').setDescription('YouTube or TikTok link').setRequired(true)).addStringOption(o => o.setName('caption').setDescription('Caption (max 140 chars)').setRequired(true)).addBooleanOption(o => o.setName('draft').setDescription('Save as draft (default: false)').setRequired(false)).addStringOption(o => o.setName('schedule_at').setDescription('Schedule publish time ISO (e.g. 2025-08-21T10:00)').setRequired(false));
const postMessageCmd = new SlashCommandBuilder().setName('postmessage').setDescription('Post a message to the message board').addStringOption(o => o.setName('text').setDescription('The message to post').setRequired(true));
const helpCmd = new SlashCommandBuilder().setName('help').setDescription('Links to the full KC features');
const voteCmd = new SlashCommandBuilder().setName('vote').setDescription('Vote Best Offence, Best Defence and rate the event');
const compareCmd = new SlashCommandBuilder().setName('compare').setDescription('Compare your KC badges with another player').addUserOption(o => o.setName('user').setDescription('The other Discord user').setRequired(true));
const setClipsChannelCmd = new SlashCommandBuilder().setName('setclipschannel').setDescription('Choose the channel where new KC clips will be posted.').addChannelOption(opt => opt.setName('channel').setDescription('Text or Announcement channel in this server').addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement).setRequired(true));
const clansCmd = new SlashCommandBuilder().setName('clans').setDescription('Browse KC clans and view details');
const clanBattlesCmd = new SlashCommandBuilder().setName('clanbattles').setDescription('View clan battles and sign up if your clan is participating');
const sendClanChallengeCmd = new SlashCommandBuilder().setName('sendclanchallenge').setDescription('Challenge another clan to a battle (owner only)').addStringOption(opt => opt.setName('clan').setDescription('Name or ID of the target clan').setRequired(true));
const incomingChallengesCmd = new SlashCommandBuilder().setName('incomingchallenges').setDescription('View and respond to pending clan battle challenges (owner only)');
const getClanRolesCmd = new SlashCommandBuilder().setName('getclanroles').setDescription('Assign yourself your clan role (creates it if missing)');
const setEventsChannelCmd = new SlashCommandBuilder().setName('seteventschannel').setDescription('Choose the channel where new clan battles will be announced.').addChannelOption(opt => opt.setName('channel').setDescription('Text or Announcement channel in this server').addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement).setRequired(true));
const battledomeCmd = new SlashCommandBuilder().setName('battledome').setDescription('View Battledome servers and who is currently playing');
const setBattledomeChannelCmd = new SlashCommandBuilder().setName('setbattledomechannel').setDescription('Choose the channel for Battledome join/leave updates').addChannelOption(opt => opt.setName('channel').setDescription('Text or Announcement channel').addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement).setRequired(true));
const setJoinLogsChannelCmd = new SlashCommandBuilder().setName('setjoinlogschannel').setDescription('Choose a channel for Battledome join logs (pings instead of DMs)').addChannelOption(opt => opt.setName('channel').setDescription('Text or Announcement channel').addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement).setRequired(true));

// West Removed from Choices
const notifyBdCmd = new SlashCommandBuilder().setName('notifybd').setDescription('Manage Battledome notifications')
  .addStringOption(o => o.setName('region').setDescription('Region to subscribe to').addChoices({ name: 'East Coast (NY)', value: 'East' }, { name: 'EU', value: 'EU' }).setRequired(false))
  .addIntegerOption(o => o.setName('threshold').setDescription('Only ping if player count reaches this number (optional)').setMinValue(1).setMaxValue(200).setRequired(false))
  .addStringOption(o => o.setName('action').setDescription('Manage subscription').addChoices({ name: 'Subscribe (default)', value: 'sub' }, { name: 'Unsubscribe', value: 'unsub' }, { name: 'Turn Off All', value: 'clear' }).setRequired(false));

const battledomeLbCmd = new SlashCommandBuilder().setName('battledomelb').setDescription('Show live leaderboard for a Battledome region')
  .addStringOption(o => o.setName('region').setDescription('Select region').addChoices({ name: 'East Coast (NY)', value: 'East' }, { name: 'EU', value: 'EU' }).setRequired(true));

const battledomeTopCmd = new SlashCommandBuilder().setName('battledometop').setDescription('Show all-time top scores recorded across Battledomes')
  .addStringOption(o => o.setName('region').setDescription('Filter by region').addChoices({ name: 'All Regions', value: 'All' }, { name: 'East Coast', value: 'East' }, { name: 'EU', value: 'EU' }).setRequired(false));

const battledomeStatusCmd = new SlashCommandBuilder().setName('battledomestatus').setDescription('Show live status for all Battledome regions and top 10 players');
const recentlyJoinedCmd = new SlashCommandBuilder().setName('recentlyjoined').setDescription('Show recent join/leave activity across Battledome regions');
const compareBdScoresCmd = new SlashCommandBuilder().setName('comparebdscores').setDescription('Compare top Battledome scores across regions');
const serverCmd = new SlashCommandBuilder().setName('server').setDescription('Show Slither leaderboard for a specific server id (NTL)').addIntegerOption(opt => opt.setName('id').setDescription('Server id from NTL list (e.g. 6622)').setRequired(true));

// F1: /bdtokc command
const bdToKcCmd = new SlashCommandBuilder().setName('bdtokc').setDescription('Link your Battledome highscores to your KC profile')
  .addStringOption(o => o.setName('action').setDescription('Enable or disable tracking').addChoices({name: 'Enable', value: 'enable'}, {name: 'Disable', value: 'disable'}).setRequired(true))
  .addStringOption(o => o.setName('name').setDescription('Your exact Battledome username (defaults to KC name)').setRequired(false));

const commandsJson = [
  linkCmd, badgesCmd, whoamiCmd, dumpCmd, lbCmd, clipsCmd, messagesCmd, votingCmd,
  avatarCmd, postCmd, postMessageCmd, helpCmd, voteCmd, compareCmd, setClipsChannelCmd,
  latestFiveCmd,
  clansCmd, clanBattlesCmd, sendClanChallengeCmd, incomingChallengesCmd, getClanRolesCmd, setEventsChannelCmd,
  battledomeCmd, setBattledomeChannelCmd, setJoinLogsChannelCmd, notifyBdCmd, battledomeLbCmd, battledomeTopCmd,
  battledomeStatusCmd, recentlyJoinedCmd, compareBdScoresCmd
  , serverCmd, bdToKcCmd
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

// D1: Implement setclipschannel logic
async function handleSetClipsChannel(interaction) {
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

  await rtdb.ref(`config/clipDestinations/${interaction.guildId}`).set({
    channelId: chan.id,
    updatedBy: interaction.user.id,
    updatedAt: admin.database.ServerValue.TIMESTAMP
  });
  globalCache.clipDestinations.set(interaction.guildId, chan.id);
  return safeReply(interaction, { content: `✅ KC Clips will now be posted to <#${chan.id}>.`, ephemeral: true });
}

// D1: Implement seteventschannel logic
async function handleSetEventsChannel(interaction) {
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

  await rtdb.ref(`config/battleDestinations/${interaction.guildId}`).set({
    channelId: chan.id,
    updatedBy: interaction.user.id,
    updatedAt: admin.database.ServerValue.TIMESTAMP
  });
  globalCache.battleDestinations.set(interaction.guildId, chan.id);
  return safeReply(interaction, { content: `✅ Clan Battles will now be announced in <#${chan.id}>.`, ephemeral: true });
}

// Clan stubs to prevent crashes
async function handleSendClanChallenge(interaction) {
  return safeReply(interaction, { content: 'Feature coming soon.', ephemeral: true });
}
async function handleIncomingChallenges(interaction) {
  return safeReply(interaction, { content: 'Feature coming soon.', ephemeral: true });
}
async function handleGetClanRoles(interaction) {
  return safeReply(interaction, { content: 'Feature coming soon.', ephemeral: true });
}

// ---------- Interaction handling ----------
let clientReady = false;
const MAX_AGE_MS = 15000;
client.on('interactionCreate', async (interaction) => {
  if (!clientReady) {
    try { await safeReply(interaction, { content: 'Starting up, try again in a second.', ephemeral: true }); } catch {}
    return;
  }
  const age = Date.now() - interaction.createdTimestamp;
  if (age > MAX_AGE_MS) console.warn(`[old interaction ~${age}ms] attempting to acknowledge anyway`);
  
  const seen = globalThis.__seen ??= new Set();
  if (seen.has(interaction.id)) return;
  seen.add(interaction.id);
  setTimeout(() => seen.delete(interaction.id), 60_000);

  // --- Slash Commands ---
  if (interaction.isChatInputCommand()) {
    const { commandName } = interaction;
    const ephemeral = isEphemeralCommand(commandName);

    if (commandName === 'link') {
      try { await safeReply(interaction, { content: `Click to link your account: ${process.env.AUTH_BRIDGE_START_URL}?state=${encodeURIComponent(interaction.user.id)}`, ephemeral: true }); } catch (err) { console.error(`[${commandName}]`, err); }
      return;
    }
    
    if (commandName === 'vote') {
      try { await showVoteModal(interaction); } catch (err) { await safeReply(interaction, { content: 'Sorry, something went wrong.', ephemeral: true }); }
      return;
    }

    if (commandName === 'sendclanchallenge') {
      try { await handleSendClanChallenge(interaction); } catch (err) { await safeReply(interaction, { content: 'Sorry, something went wrong.', ephemeral: true }); }
      return;
    }
    
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
            const json = "```json\n" + JSON.stringify(payload, null, 2) + "\n```";
            await safeReply(interaction, { content: json, ephemeral: true });
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
            const state = { list, nameMap, page: 0, currentPostPath: null }; 
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
            if (!me || !channel?.permissionsFor(me)?.has([PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.EmbedLinks])) {
              return await safeReply(interaction, { content: 'I need **View Channel**, **Send Messages**, and **Embed Links** here.', ephemeral: true });
            }
            await safeReply(interaction, { content: `Posting ${list.length} latest clip${list.length > 1 ? 's' : ''}...`, ephemeral: true });
            for (const item of list) {
              const embed = buildClipDetailEmbed(item, nameMap);
              const msg = await channel.send({ embeds: [embed] });
              const postPath = clipDbPath(item);
              const rows = clipsDetailRows(msg, postPath); 
              await msg.edit({ components: rows });
              await new Promise(r => setTimeout(r, 300));
            }
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
            const pages = [
              { image: 'https://raw.githubusercontent.com/kevinmidnight7-sudo/kc-events-discord-bot/da405cc9608290a6bbdb328b13393c16c8a7f116/link%203.png' },
              { image: 'https://raw.githubusercontent.com/kevinmidnight7-sudo/kc-events-discord-bot/da405cc9608290a6bbdb328b13393c16c8a7f116/link4.png' },
              { image: 'https://kevinmidnight7-sudo.github.io/messageboardkc/link.png' },
            ];
            const parentId = interaction.id;
            interaction.client.helpCache.set(parentId, { pages, index: 0, userId: interaction.user.id });
            setTimeout(() => interaction.client.helpCache.delete(parentId), 15 * 60 * 1000);
            const embed = buildHelpEmbed(pages[0]);
            const prevBtn = new ButtonBuilder().setCustomId(`help:prev:${parentId}`).setLabel('◀').setStyle(ButtonStyle.Secondary).setDisabled(true);
            const nextBtn = new ButtonBuilder().setCustomId(`help:next:${parentId}`).setLabel('▶').setStyle(ButtonStyle.Secondary).setDisabled(pages.length <= 1);
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
            await handleSetClipsChannel(interaction);
        }
        else if (commandName === 'clans') {
          const [clansSnap, usersSnap, badgesSnap] = await Promise.all([
            withTimeout(rtdb.ref('clans').get(), 6000, 'RTDB clans'),
            withTimeout(rtdb.ref('users').get(), 6000, 'RTDB users'),
            withTimeout(rtdb.ref('badges').get(), 6000, 'RTDB badges'),
          ]);
          const clansData = clansSnap.val() || {};
          const usersData = usersSnap.val() || {};
          const badgesData = badgesSnap.val() || {};
          const entries = Object.entries(clansData).map(([id, clan]) => {
            const memberCount = clan.members ? Object.keys(clan.members).length : 0;
            const score = computeClanScore(clan, usersData, badgesData);
            return { id, ...clan, memberCount, score };
          });
          if (entries.length === 0) {
            return safeReply(interaction, { content: 'There are no clans yet.', embeds: [] });
          }
          entries.sort((a, b) => b.score - a.score);
          const top = entries.slice(0, 20);
          const options = top.map(c => ({
            label: clamp(c.name, 100),
            description: clamp(`${c.memberCount} members • ${c.score} points`, 100),
            value: c.id,
          }));
          const select = new StringSelectMenuBuilder()
            .setCustomId(`clans_select:${interaction.id}`)
            .setPlaceholder('Select a clan to view details')
            .addOptions(options);
          interaction.client.clanCache ??= new Map();
          interaction.client.clanCache.set(interaction.id, { entries, usersData, badgesData });
          setTimeout(() => interaction.client.clanCache?.delete(interaction.id), 15 * 60 * 1000).unref();
          const embed = new EmbedBuilder()
            .setTitle('KC Clans')
            .setDescription('Select a clan below to view its members, owner, description and score.')
            .setColor(DEFAULT_EMBED_COLOR);
          await safeReply(interaction, { content: '', embeds: [embed], components: [ new ActionRowBuilder().addComponents(select) ] });
        }
        else if (commandName === 'clanbattles') {
          await safeDefer(interaction);
          try {
            const discordId = interaction.user.id;
            const uid = await getKCUidForDiscord(discordId);
            const [battlesSnap, clansSnap, usersSnap] = await Promise.all([
              withTimeout(rtdb.ref('battles').get(), 8000, 'RTDB battles'),
              withTimeout(rtdb.ref('clans').get(), 8000, 'RTDB clans'),
              withTimeout(rtdb.ref('users').get(), 8000, 'RTDB users'),
            ]);
            const battlesData = battlesSnap && typeof battlesSnap.exists === 'function' && battlesSnap.exists() ? (battlesSnap.val() || {}) : {};
            const clansData   = clansSnap   && typeof clansSnap.exists   === 'function' && clansSnap.exists()   ? (clansSnap.val()   || {}) : {};
            const usersData   = usersSnap   && typeof usersSnap.exists   === 'function' && usersSnap.exists()   ? (usersSnap.val()   || {}) : {};
            let userClanId = null;
            if (uid) {
              for (const cid of Object.keys(clansData)) {
                const c = clansData[cid];
                if (c && c.members && c.members[uid]) { userClanId = cid; break; }
              }
            }
            const entries = Object.entries(battlesData);
            const upcoming = entries.filter(([_, b]) => b && b.status === 'accepted');
            upcoming.sort((a, b) => ((a[1].scheduledTime || 0) - (b[1].scheduledTime || 0)));
            const past = entries.filter(([_, b]) => b && b.status === 'finished');
            past.sort((a, b) => ((b[1].scheduledTime || 0) - (a[1].scheduledTime || 0)));
            const my = upcoming.filter(([_, b]) => userClanId && (b.challengerId === userClanId || b.targetId === userClanId));
            const cache = { lists: { all: upcoming, my: my, past: past }, filter: 'all', clansData, usersData, uid, userClanId };
            interaction.client.battleCache ??= new Map();
            interaction.client.battleCache.set(interaction.id, cache);
            setTimeout(() => interaction.client.battleCache?.delete(interaction.id), 15 * 60 * 1000).unref();
            const list = cache.lists.all;
            const embed = buildBattlesListEmbed(list, 'all', clansData, usersData);
            const rows = [];
            const max = Math.min(list.length, BATTLES_PAGE_SIZE);
            for (let i = 0; i < max; i += 5) {
              const row = new ActionRowBuilder();
              for (let j = i; j < Math.min(i + 5, max); j++) {
                row.addComponents(new ButtonBuilder().setCustomId(`cb:detail:${interaction.id}:${j}`).setLabel(String(j + 1)).setStyle(ButtonStyle.Secondary));
              }
              rows.push(row);
            }
            const filterRow = new ActionRowBuilder();
            ['all', 'my', 'past'].forEach(ft => {
              const label = ft === 'all' ? 'All' : (ft === 'my' ? 'My Clan' : 'Past');
              const style = ft === 'all' ? ButtonStyle.Primary : ButtonStyle.Secondary;
              filterRow.addComponents(new ButtonBuilder().setCustomId(`cb:filter:${interaction.id}:${ft}`).setLabel(label).setStyle(style));
            });
            rows.push(filterRow);
            await safeReply(interaction, { embeds: [embed], components: rows });
          } catch (err) {
            console.error('[clanbattles]', err);
            await safeReply(interaction, { content: '❌ Failed to load clan battles.', embeds: [], components: [] });
          }
        }
        else if (commandName === 'incomingchallenges') {
          await handleIncomingChallenges(interaction);
        }
        else if (commandName === 'getclanroles') {
          await handleGetClanRoles(interaction);
        }
        else if (commandName === 'seteventschannel') {
          await handleSetEventsChannel(interaction);
        }
        else if (commandName === 'battledome') {
          await safeDefer(interaction);
          const servers = Object.values(BD_SERVERS);
          const options = servers.map((s, idx) => ({ label: clampStr(s.name, 100, 'Unknown'), description: clampStr(s.region || 'Unknown Region', 100, '—'), value: String(idx) }));
          const parentId = interaction.id;
          interaction.client.bdCache ??= new Map();
          interaction.client.bdCache.set(parentId, { servers });
          setTimeout(() => interaction.client.bdCache?.delete(parentId), 15 * 60 * 1000).unref?.();
          const select = new StringSelectMenuBuilder().setCustomId(`bd_select:${parentId}`).setPlaceholder('Select a Battledome server…').addOptions(options);
          const embed = new EmbedBuilder().setTitle('Battledome Servers').setDescription('Pick a server to see who is online + who is in the dome right now.').setColor(DEFAULT_EMBED_COLOR).setFooter({ text: 'KC Bot • /battledome' });
          return safeReply(interaction, { embeds: [embed], components: [new ActionRowBuilder().addComponents(select)] });
        }
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

          // Write new dest
          await rtdb.ref(`config/bdDestinations/${interaction.guildId}`).set({
            channelId: chan.id,
            updatedBy: interaction.user.id,
            updatedAt: admin.database.ServerValue.TIMESTAMP
          });
          globalCache.bdDestinations.set(interaction.guildId, chan.id);

          // Fix A3: Clear stored message so we send a fresh one in new channel
          await rtdb.ref(`config/bdStatusMessage/${interaction.guildId}`).remove();
          globalCache.bdStatusMessages.delete(interaction.guildId);

          updateBdStatusMessages().catch(() => {});
          return safeReply(interaction, { content: `✅ Battledome updates will post to <#${chan.id}>.`, ephemeral: true });
        }
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
        else if (commandName === 'notifybd') {
          const region = interaction.options.getString('region');
          const threshold = interaction.options.getInteger('threshold');
          const action = interaction.options.getString('action') || 'sub';
          const userId = interaction.user.id;
          const guildId = interaction.guildId;

          if (!guildId) return safeReply(interaction, { content: 'Run in a server.', ephemeral: true });
          const ref = rtdb.ref(`bdNotify/${guildId}/${userId}`);
          const userActionRaw = interaction.options.getString('action');
          const userAction = userActionRaw ? userActionRaw.toLowerCase() : 'toggle';

          if (!region && typeof threshold !== 'number') {
            let currentSubs = [];
            try {
              const subsSnap = await ref.child('regions').get();
              if (subsSnap.exists()) currentSubs = Object.keys(subsSnap.val() || {});
            } catch {}
            async function subscribeAllRegions() {
              const updateData = { enabled: true, mode: 'join', onlyNamedJoins: false };
              // West Removed
              for (const r of ['East','EU']) await ref.child(`regions/${r}`).update(updateData);
              await ref.child('updatedAt').set(admin.database.ServerValue.TIMESTAMP);
            }
            async function unsubscribeAllRegions() {
              await ref.remove();
            }
            if (['clear','off','unsub'].includes(userAction)) {
              await unsubscribeAllRegions();
              return safeReply(interaction, { content: '🔕 Unsubscribed from all Battledome alerts.',  ephemeral: true });
            }
            if (['on','sub'].includes(userAction)) {
              await subscribeAllRegions();
              return safeReply(interaction, { content: '🔔 Battledome alerts enabled for all regions.',  ephemeral: true });
            }
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
          if (!region && typeof threshold === 'number') {
            return safeReply(interaction, { content: 'Please specify a region.',  ephemeral: true });
          }
          if (action === 'unsub') {
             await ref.child(`regions/${region}`).remove();
             return safeReply(interaction, { content: `🔕 Unsubscribed from **${region}** alerts.`, ephemeral: true });
          } else {
             // Reset state to 'below' on change (Instruction 2C4)
             const updateData = {
                 enabled: true,
                 ...(typeof threshold === 'number' ? { threshold } : {}),
                 mode: typeof threshold === 'number' ? 'active' : 'join',
                 onlyNamedJoins: false,
                 state: 'below',
             };
             await ref.child(`regions/${region}`).update(updateData);
             await ref.child('updatedAt').set(admin.database.ServerValue.TIMESTAMP);
             const msg = typeof threshold === 'number'
                ? `🔔 Subscribed to **${region}**! You will be notified when online players >= ${threshold}.`
                : `🔔 Subscribed to **${region}** join alerts!`;
             return safeReply(interaction, { content: msg, ephemeral: true });
          }
        }
        else if (commandName === 'battledomelb') {
          await safeDefer(interaction);
          const region = interaction.options.getString('region');
          const serverConfig = BD_SERVERS[region];
          if (!serverConfig) return safeReply(interaction, { content: 'Unknown region.', ephemeral: true });

          let info;
          let fromCache = false;
          let fetchTime = 0;
          try {
            const r = await getBdInfoCached(serverConfig.url);
            info = r.data;
            fromCache = r.fromCache;
            fetchTime = r.fetchedAt;
          } catch (e) {}

          if (!info || !info.players) {
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
          const ageSec = fetchTime ? Math.floor((Date.now() - fetchTime) / 1000) : 0;
          const cacheNote = fromCache ? `⚠️ Cached (${ageSec}s ago)` : `✅ Live (${ageSec}s ago)`;
          const embed = new EmbedBuilder().setTitle(`Battledome Leaderboard — ${serverConfig.name}`).setDescription(lines || '_No players listed._').setFooter({ text: `KC Bot • /battledomelb • ${cacheNote}` }).setColor(DEFAULT_EMBED_COLOR);
          return safeReply(interaction, { embeds: [embed] });
        }
        else if (commandName === 'battledometop') {
            await safeDefer(interaction);
            const region = interaction.options.getString('region') || 'All';
            const entries = [];
            if (region === 'All') {
                bdTop.global.forEach((v, k) => {
                    const obj = (v && typeof v === 'object') ? { name: k, score: v.score ?? 0, seenAt: v.seenAt ?? 0, serverName: v.serverName ?? '' } : { name: k, score: Number(v) || 0, seenAt: 0, serverName: '' };
                    entries.push(obj);
                });
            } else if (bdTop[region]) {
                bdTop[region].forEach((v, k) => {
                    const obj = (v && typeof v === 'object') ? { name: k, score: v.score ?? 0, seenAt: v.seenAt ?? 0, serverName: v.serverName ?? '' } : { name: k, score: Number(v) || 0, seenAt: 0, serverName: '' };
                    entries.push(obj);
                });
            } else {
                return safeReply(interaction, { content: 'Unknown region.', ephemeral: true });
            }
            entries.sort((a, b) => (b.score || 0) - (a.score || 0));
            const top15 = entries.slice(0, 15);
            if (top15.length === 0) return safeReply(interaction, { content: `No top scores recorded yet for **${region}**. Check back later!`, ephemeral: true });
            const lines = top15.map((p, i) => {
                const nm = clampName(p.name, 28).padEnd(30, ' ');
                const sc = String(p.score).padStart(5, ' ');
                return `\`${String(i + 1).padStart(2, ' ')}.\` **${nm}** \`— ${sc}\``;
            }).join('\n');
            const lastUpdate = bdTopMeta.lastUpdatedAt || Date.now();
            const embed = new EmbedBuilder().setTitle(region === 'All' ? 'All‑Time Top LB' : `All‑Time Top LB (${region})`).setDescription(lines).setFooter({ text: `Updated <t:${Math.floor(lastUpdate / 1000)}:R> • Cached` }).setColor(DEFAULT_EMBED_COLOR);
            return safeReply(interaction, { embeds: [embed] });
        }
        else if (commandName === 'battledomestatus') {
            await safeDefer(interaction);
            const embed = buildBdStatusUnifiedEmbed();
            return safeReply(interaction, { embeds: [embed] });
        }
        else if (commandName === 'recentlyjoined') {
            await safeDefer(interaction);
            const embed = buildBdRecentUnifiedEmbed();
            return safeReply(interaction, { embeds: [embed] });
        }
        else if (commandName === 'comparebdscores') {
            await safeDefer(interaction);
            try {
              const embed = buildBdScoreCompareEmbed();
              return safeReply(interaction, { embeds: [embed] });
            } catch (err) {
              return safeReply(interaction, { content: 'Failed to build BD score comparison.', ephemeral: true });
            }
        }
        else if (commandName === 'server') {
            const id = interaction.options.getInteger('id');
            try {
              const { servers, fromCache, fetchedAt } = await getSlitherServersCached();
              const s = servers.get(id);
              if (!s) return safeReply(interaction, { content: `I couldn’t find server **${id}** on NTL right now. Try again in a few seconds.`, embeds: [], components: [] });
              const lines = (s.leaderboard.length ? s.leaderboard : []).map(e => {
                const nm = (e.name || '(no name)').replace(/`/g, 'ˋ');
                return `\`${String(e.rank).padStart(2,' ')}.\` **${nm}** — \`${e.score}\``;
              }).join('\n') || '_No leaderboard entries found._';
              const ageSec = Math.floor((Date.now() - fetchedAt) / 1000);
              const cacheNote = fromCache ? `Cached (${ageSec}s)` : `Live (${ageSec}s)`;
              const embed = new EmbedBuilder()
                .setTitle(`🐍 Slither Server ${s.id}`).setDescription(`**${s.ipPort}** — ${s.region}`)
                .addFields({ name: 'Leaderboard (Top 10)', value: lines }, { name: 'Total Score', value: String(s.totalScore ?? '—'), inline: true }, { name: 'Total Players', value: String(s.totalPlayers ?? '—'), inline: true }, { name: 'Updated', value: String(s.updated ?? '—'), inline: true })
                .setFooter({ text: `NTL • ${cacheNote}` });
              return safeReply(interaction, { embeds: [embed] });
            } catch (err) {
              return safeReply(interaction, { content: 'Sorry, failed to fetch server data. Please try again later.', embeds: [], components: [] });
            }
        }
        // F1: /bdtokc command handler
        else if (commandName === 'bdtokc') {
            const discordId = interaction.user.id;
            const kcUid = await getKCUidForDiscord(discordId);
            if (!kcUid) {
                return safeReply(interaction, { content: 'You must link your KC account first using `/link`.', ephemeral: true });
            }
            const action = interaction.options.getString('action');
            if (action === 'disable') {
                await rtdb.ref(`config/bdToKC/${discordId}`).remove();
                return safeReply(interaction, { content: '✅ Battledome score tracking disabled.', ephemeral: true });
            }
            // Enable
            const inputName = interaction.options.getString('name');
            let kcName = inputName;
            if (!kcName) {
                // Fetch KC profile name as default
                const profile = await getKCProfile(kcUid);
                kcName = profile.displayName;
            }
            if (!kcName) {
                return safeReply(interaction, { content: 'Could not determine your name. Please specify it explicitly.', ephemeral: true });
            }
            await rtdb.ref(`config/bdToKC/${discordId}`).set({
                kcUid,
                kcName,
                enabled: true,
                updatedAt: admin.database.ServerValue.TIMESTAMP
            });
            return safeReply(interaction, { content: `✅ Battledome score tracking enabled for name **${kcName}**!`, ephemeral: true });
        }

    } catch (err) {
        console.error(`[${commandName}]`, err);
        await safeReply(interaction, { content: '❌ Sorry — something went wrong while processing your request.', ephemeral: true });
    }
  } 
  else if (interaction.isModalSubmit()) {
    const modalId = interaction.customId || '';
    if (modalId.startsWith('bd:alert_settings_modal')) {
      try {
        const parts = modalId.split(':');
        const guildId = parts[2] || interaction.guildId;
        const minPlayersStr = interaction.fields.getTextInputValue('minPlayers') || '';
        const cooldownStr = interaction.fields.getTextInputValue('cooldownMinutes') || '';
        let minPlayersVal = undefined;
        let cooldownVal = undefined;
        if (minPlayersStr) {
          const mp = parseInt(minPlayersStr.replace(/\D/g, ''), 10);
          if (!isNaN(mp)) minPlayersVal = Math.max(1, Math.min(100, mp));
        }
        if (cooldownStr) {
          const cd = parseInt(cooldownStr.replace(/\D/g, ''), 10);
          if (!isNaN(cd)) cooldownVal = Math.max(1, Math.min(1440, cd));
        }
        const updates = {};
        if (typeof minPlayersVal === 'number') updates[`config/bdAlertSettings/${guildId}/minPlayers`] = minPlayersVal;
        if (typeof cooldownVal === 'number') updates[`config/bdAlertSettings/${guildId}/cooldownMinutes`] = cooldownVal;
        if (Object.keys(updates).length > 0) await rtdb.ref().update(updates);
        await rtdb.ref(`config/bdAlertSettings/${guildId}/updatedAt`).set(admin.database.ServerValue.TIMESTAMP);
        await safeReply(interaction, { content: '✅ Alert settings saved.', embeds: [], components: [], ephemeral: true });
        updateBdStatusMessages().catch(() => {});
      } catch (err) {
        await safeReply(interaction, { content: '❌ Failed to save settings.', embeds: [], components: [], ephemeral: true });
      }
      return;
    }
  }
  else if (interaction.isButton()) {
    const id = interaction.customId;
    if (id && id.startsWith('bd:')) {
      const parts = id.split(':');
      const action = parts[1] || '';
      const guildId = parts[2] || interaction.guildId;
      if (action === 'controls') {
        let alertsEnabled = true;
        try {
          const snap = await rtdb.ref(`config/bdAlertsEnabled/${guildId}`).get();
          if (snap.exists() && snap.val() === false) alertsEnabled = false;
        } catch {}
        let minPlayers = undefined;
        let cooldownMinutes = undefined;
        try {
          const snap = await rtdb.ref(`config/bdAlertSettings/${guildId}`).get();
          if (snap.exists()) {
            const val = snap.val() || {};
            if (typeof val.minPlayers === 'number') minPlayers = val.minPlayers;
            if (typeof val.cooldownMinutes === 'number') cooldownMinutes = val.cooldownMinutes;
          }
        } catch {}
        let servers = [];
        try {
          const subSnap = await rtdb.ref(`bdNotify/${guildId}/${interaction.user.id}/regions`).get();
          if (subSnap.exists()) servers = Object.keys(subSnap.val() || {});
        } catch {}
        const serverList = servers.length ? servers.join(', ') : 'None';
        let dmEnabled = false;
        try {
          const snap = await rtdb.ref(`config/bdAlertPrefs/${guildId}/${interaction.user.id}/dmEnabled`).get();
          if (snap.exists() && snap.val() === true) dmEnabled = true;
        } catch {}
        const embed = new EmbedBuilder().setTitle('Battledome Controls').setDescription('Manage your alert preferences and settings.').setColor(DEFAULT_EMBED_COLOR)
          .addFields({ name: 'Alerts', value: alertsEnabled ? '✅ Enabled' : '❌ Disabled', inline: true }, { name: 'Servers', value: serverList, inline: true }, { name: 'Cooldown', value: cooldownMinutes ? `${cooldownMinutes} min` : '—', inline: true }, { name: 'Min Players', value: minPlayers ? String(minPlayers) : '—', inline: true }, { name: 'DM Notifications', value: dmEnabled ? '✅ Enabled' : '❌ Disabled', inline: true });
        const toggleBtn = new ButtonBuilder().setCustomId(`bd:toggle_alerts:${guildId}`).setLabel(alertsEnabled ? 'Disable Alerts' : 'Enable Alerts').setStyle(alertsEnabled ? ButtonStyle.Danger : ButtonStyle.Success);
        const settingsBtn = new ButtonBuilder().setCustomId(`bd:alert_settings:${guildId}`).setLabel('Alert Settings').setStyle(ButtonStyle.Secondary);
        const serversBtn = new ButtonBuilder().setCustomId(`bd:open_server_filter:${guildId}`).setLabel('Select Servers').setStyle(ButtonStyle.Secondary);
        const dmBtn = new ButtonBuilder().setCustomId(`bd:toggle_dm:${guildId}`).setLabel(dmEnabled ? 'Disable DMs' : 'Enable DMs').setStyle(dmEnabled ? ButtonStyle.Danger : ButtonStyle.Success);
        const row = new ActionRowBuilder().addComponents(toggleBtn, settingsBtn, serversBtn, dmBtn);
        await safeReply(interaction, { embeds: [embed], components: [row], ephemeral: true });
        return;
      }
      if (action === 'refresh') {
        const last = bdManualRefreshAt.get(guildId) || 0;
        const now = Date.now();
        const diff = now - last;
        if (diff < 5000) {
          const secondsLeft = Math.ceil((5000 - diff) / 1000);
          return safeReply(interaction, { content: `⏳ Please wait ${secondsLeft}s before refreshing again.`, ephemeral: true });
        }
        bdManualRefreshAt.set(guildId, now);
        await Promise.all([checkRegion('East'), checkRegion('EU')]); // West Removed
        updateBdStatusMessages().catch(() => {});
        return safeReply(interaction, { content: '🔄 Refreshing Battledome data…', ephemeral: true });
      }
      if (action === 'details') {
        let showAdvanced = false;
        try {
          const snap = await rtdb.ref(`config/bdShowAdvanced/${guildId}`).get();
          if (snap.exists() && snap.val() === true) showAdvanced = true;
        } catch {}
        showAdvanced = !showAdvanced;
        await rtdb.ref(`config/bdShowAdvanced/${guildId}`).set(showAdvanced);
        updateBdStatusMessages().catch(() => {});
        return safeReply(interaction, { content: '📊 Details view updated.',  ephemeral: true });
      }
      if (action === 'toggle_alerts') {
        let alertsEnabled = true;
        try {
          const snap = await rtdb.ref(`config/bdAlertsEnabled/${guildId}`).get();
          if (snap.exists() && snap.val() === false) alertsEnabled = false;
        } catch {}
        alertsEnabled = !alertsEnabled;
        await rtdb.ref(`config/bdAlertsEnabled/${guildId}`).set(alertsEnabled);
        updateBdStatusMessages().catch(() => {});
        return safeReply(interaction, { content: alertsEnabled ? '🔔 Alerts enabled.' : '🔕 Alerts disabled.', ephemeral: true });
      }
      if (action === 'toggle_dm') {
        let dmEnabled = false;
        try {
          const snap = await rtdb.ref(`config/bdAlertPrefs/${guildId}/${interaction.user.id}/dmEnabled`).get();
          if (snap.exists() && snap.val() === true) dmEnabled = true;
        } catch {}
        dmEnabled = !dmEnabled;
        await rtdb.ref(`config/bdAlertPrefs/${guildId}/${interaction.user.id}`).update({
          dmEnabled,
          updatedAt: admin.database.ServerValue.TIMESTAMP,
          updatedBy: interaction.user.id
        });
        return safeReply(interaction, { content: dmEnabled ? '✅ DM notifications enabled.' : '✅ DM notifications disabled.', embeds: [], components: [], ephemeral: true });
      }
      if (action === 'alert_settings') {
        const modal = new ModalBuilder().setCustomId(`bd:alert_settings_modal:${guildId}`).setTitle('Edit Battledome Alert Settings');
        const minPlayersInput = new TextInputBuilder().setCustomId('minPlayers').setLabel('Minimum players to trigger (1–100)').setStyle(TextInputStyle.Short).setRequired(false);
        const cooldownInput = new TextInputBuilder().setCustomId('cooldownMinutes').setLabel('Cooldown in minutes (1–1440)').setStyle(TextInputStyle.Short).setRequired(false);
        modal.addComponents(new ActionRowBuilder().addComponents(minPlayersInput), new ActionRowBuilder().addComponents(cooldownInput));
        await interaction.showModal(modal);
        return;
      }
      if (action === 'open_server_filter') {
        let selected = [];
        try {
          const snap = await rtdb.ref(`bdNotify/${guildId}/${interaction.user.id}/regions`).get();
          if (snap.exists()) selected = Object.keys(snap.val() || {});
        } catch {}
        // West Removed
        const select = new StringSelectMenuBuilder().setCustomId(`bd:server_filter_select:${guildId}`).setMinValues(0).setMaxValues(2).setPlaceholder('Select regions')
          .addOptions(
            { label: 'East Coast (NY)', value: 'East', default: selected.includes('East'), description: 'Subscribe to East Battledome alerts' },
            { label: 'EU', value: 'EU', default: selected.includes('EU'), description: 'Subscribe to EU Battledome alerts' }
          );
        const row = new ActionRowBuilder().addComponents(select);
        const embed = new EmbedBuilder().setTitle('Select Battledome Servers').setDescription('Choose which regions you want alerts for.').setColor(DEFAULT_EMBED_COLOR);
        await safeReply(interaction, { embeds: [embed], components: [row], ephemeral: true });
        return;
      }
      return;
    }
  }
  else if (interaction.isStringSelectMenu()) {
    const [prefix, parentInteractionId] = interaction.customId.split(':');
    if (interaction.customId.startsWith('bd:server_filter_select')) {
      try {
        const parts = interaction.customId.split(':');
        const guildId = parts[2] || interaction.guildId;
        const userId = interaction.user.id;
        const selected = interaction.values || [];
        const updates = {};
        for (const regionKey of ['East','EU']) { // West Removed
          const path = `bdNotify/${guildId}/${userId}/regions/${regionKey}`;
          if (selected.includes(regionKey)) {
            updates[path + '/enabled'] = true;
            updates[path + '/mode'] = 'join';
          } else {
            updates[path] = null;
          }
        }
        await rtdb.ref().update(updates);
        await rtdb.ref(`bdNotify/${guildId}/${userId}/updatedAt`).set(admin.database.ServerValue.TIMESTAMP);
        const serverList = selected.length ? selected.join(', ') : 'None';
        return safeReply(interaction, { content: `✅ Your Battledome servers updated to: ${serverList}`, embeds: [], components: [], ephemeral: true });
      } catch (err) {
        return safeReply(interaction, { content: '❌ Failed to update your servers.', embeds: [], components: [], ephemeral: true });
      }
    }
    if (prefix === 'bd_select') {
      try {
          await safeDefer(interaction, { intent: 'update' });
          const cache = interaction.client.bdCache?.get(parentInteractionId);
          if (!cache) return safeReply(interaction, { content: 'This Battledome menu expired. Run `/battledome` again.', ephemeral: true });
          const idx = parseInt(interaction.values?.[0] || '', 10);
          const s = cache.servers?.[idx];
          if (!s) return safeReply(interaction, { content: 'Invalid selection.', ephemeral: true });
          const region = s.region || BD_NAME_OVERRIDES[s.name] || "East"; // Fallback changed from West
          const url = BD_SERVERS[region]?.url || s.url;
          if (!url) return safeReply(interaction, { content: `No URL configured for region: ${region}`, ephemeral: true });
          let info;
          let fromCache = false;
          let fetchedAt = 0;
          try {
            const r = await getBdInfoCached(url);
            info = r?.data;
            fromCache = !!r?.fromCache;
            fetchedAt = r?.fetchedAt || 0;
          } catch (e) {}
          if (!info) {
              const warmEmbed = new EmbedBuilder().setTitle(`Battledome — ${s.name || 'Server'}`)
                  .addFields({ name: 'Online now', value: '—', inline: true }, { name: 'In dome now', value: '—', inline: true }, { name: 'Players this hour', value: '—', inline: true }, { name: 'Players (0)', value: '_No snapshot yet—warming cache. Try again in a few seconds._' })
                  .setColor(DEFAULT_EMBED_COLOR).setFooter({ text: 'Cache warming up' });
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
          const ageSec = fetchedAt ? Math.floor((Date.now() - fetchedAt) / 1000) : 0;
          const cacheNote = fromCache ? `⚠️ Cached (${ageSec}s ago)` : `✅ Live (${ageSec}s ago)`;
          const embed = new EmbedBuilder().setTitle(info?.name ? `Battledome — ${info.name}` : `Battledome — ${s.name || 'Server'}`)
            .addFields({ name: 'Online now', value: String(info?.onlinenow ?? '—'), inline: true }, { name: 'In dome now', value: String(info?.indomenow ?? '—'), inline: true }, { name: 'Players this hour', value: String(info?.thishour ?? '—'), inline: true }, { name: `Players (${players.length})`, value: lines.length ? lines.join('\n') : '_No players listed._' })
            .setColor(DEFAULT_EMBED_COLOR).setFooter({ text: `KC Bot • /battledome • ${cacheNote}` });
          return safeReply(interaction, { embeds: [embed], components: interaction.message.components });
      } catch (err) {
        await safeReply(interaction, { content: '❌ Sorry, something went wrong.', ephemeral: true, embeds: [], components: [] });
      }
     }
  }
});

async function checkRegion(regionKey) {
  const config = BD_SERVERS[regionKey];
  if (!config) return;
  const r = await getBdInfoCached(config.url).catch(() => null);
  const info = r?.data;
  if (!info || !Array.isArray(info.players)) return;
  bdLastInfo[regionKey] = info;
  bdLastFetch[regionKey] = r?.fetchedAt || Date.now();
  updateBdTop(regionKey, info.players);
  if (r && !r.fromCache && !r.stale) {
      announceBdCacheUpdated(regionKey, r.fetchedAt).catch(() => {});
  }
  const state = bdState[regionKey];
  const currentNames = new Set(info.players.map(p => p.name));
  const currentOnline = info.onlinenow;
  const currentIndome = info.indomenow || 0;
  if (state.lastCheck === 0) {
    state.lastNames = currentNames;
    state.lastOnline = currentOnline;
    state.lastIndome = currentIndome;
    state.lastCheck = Date.now();
    return;
  }
  const joins = [];
  const leaves = [];
  for (const name of currentNames) if (!state.lastNames.has(name)) joins.push(name);
  for (const name of state.lastNames) if (!currentNames.has(name)) leaves.push(name);
  
  const namedCountPrev = state.lastNames.size;
  const namedCountCurr = currentNames.size;
  const unnamedPrev = Math.max(0, state.lastOnline - namedCountPrev);
  const unnamedCurr = Math.max(0, currentOnline - namedCountCurr);
  let unnamedDiff = unnamedCurr - unnamedPrev; 
  
  state.lastNames = currentNames;
  state.lastOnline = currentOnline;
  state.lastIndome = currentIndome;
  state.lastCheck = Date.now();

  if (joins.length === 0 && leaves.length === 0 && unnamedDiff === 0) return;

  await broadcastBdUpdate(regionKey, { joins, leaves, unnamedDiff, info });

  pruneBdRecent();
  const now = Date.now();
  for (const name of joins) bdRecent[regionKey].push({ name, time: now, type: 'join' });
  for (const name of leaves) bdRecent[regionKey].push({ name, time: now, type: 'leave' });

  if (joins.length > 0 || leaves.length > 0) {
    for (const [guildId] of globalCache.bdJoinLogs.entries()) {
      try {
        await postJoinLogsBlock(guildId, regionKey, { joins, leaves });
      } catch (e) {}
    }
  }
}

// Updated Broadcast logic (Instruction 2C)
async function broadcastBdUpdate(regionKey, { joins, leaves, unnamedDiff, info }) {
  const serverName = BD_SERVERS[regionKey]?.name || regionKey;
  // C1: Metric is onlinenow (Total online)
  const currentMetric = info.onlinenow || 0; 

  for (const [guildId, _channelId] of globalCache.bdDestinations.entries()) {
    try {
      let alertsEnabled = true;
      try {
        const enabledSnap = await rtdb.ref(`config/bdAlertsEnabled/${guildId}`).get();
        if (enabledSnap.exists() && enabledSnap.val() === false) alertsEnabled = false;
      } catch {}
      if (!alertsEnabled) continue;

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
          // Check metric against threshold
          if (currentMetric >= threshold) {
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
          // join mode
          if (joins.length > 0) triggered.push({ userId, mode: 'join' });
        }
      });
      if (Object.keys(stateUpdates).length > 0) {
        rtdb.ref().update(stateUpdates).catch(e => console.error('State update failed', e));
      }

      for (const { userId, mode, threshold } of triggered) {
        if (mode !== 'active') continue; // Joins handled by logs
        
        const message = `✅ ${serverName} Battledome has reached your player count (online) of **${threshold}** (now ${currentMetric}).`;
        
        // C2: Read DM pref default false
        let dmEnabledPref = false;
        try {
          const snap = await rtdb.ref(`config/bdAlertPrefs/${guildId}/${userId}/dmEnabled`).get();
          if (snap.exists() && snap.val() === true) dmEnabledPref = true;
        } catch {}
        
        const joinLogsId = globalCache.bdJoinLogs?.get(guildId);
        
        // C3: DM rule
        if (dmEnabledPref === true) {
          try {
            const user = await client.users.fetch(userId).catch(() => null);
            if (user) await user.send({ content: message });
          } catch {}
        } else {
            // If DM disabled -> check join logs -> else silent
            if (joinLogsId) {
                try {
                    const chan = await client.channels.fetch(joinLogsId).catch(() => null);
                    if (chan && chan.isTextBased?.()) {
                        await chan.send({ content: `<@${userId}> ${message}` });
                    }
                } catch {}
            }
        }
      }
    } catch (err) {
      console.error('[BD Broadcast] Error while processing guild', guildId, err);
    }
  }
}

// Polling Loop: 15s interval (Instruction 2B)
async function pollBattledome() {
  setInterval(async () => {
      try {
          // Refresh both East and EU concurrently
          await Promise.all([checkRegion('East'), checkRegion('EU')]);
          // Update status messages
          await updateBdStatusMessages();
      } catch (e) {
          console.error('[BD Poller] Interval error:', e);
      }
  }, 15000);
}

// ---------- Startup ----------
client.once('ready', async () => {
  console.log(`Logged in as ${client.user.tag}`);
  clientReady = true;
  
  // A1: Load persistent status message IDs
  try {
      const snap = await rtdb.ref('config/bdStatusMessage').get();
      if (snap.exists()) {
          const val = snap.val();
          for(const [gid, data] of Object.entries(val)) {
              if (data && data.channelId && data.messageId) {
                  globalCache.bdStatusMessages.set(gid, data);
              }
          }
      }
  } catch(e) { console.error('Failed to load status messages', e); }

  // F3: Load BD-KC mappings
  try {
      const snap = await rtdb.ref('config/bdToKC').get();
      if (snap.exists()) {
          const val = snap.val();
          for(const [did, data] of Object.entries(val)) {
              if (data.enabled) {
                  globalCache.bdToKC.set(did, data);
                  if (data.kcName) globalCache.kcNameToUid.set(data.kcName, data.kcUid);
              }
          }
      }
      // Listener for updates
      rtdb.ref('config/bdToKC').on('child_changed', s => {
          const data = s.val();
          if (data.enabled) {
              globalCache.bdToKC.set(s.key, data);
              if (data.kcName) globalCache.kcNameToUid.set(data.kcName, data.kcUid);
          } else {
              globalCache.bdToKC.delete(s.key);
              // rebuild reverse map costly? assume infrequent.
          }
      });
      rtdb.ref('config/bdToKC').on('child_added', s => {
        const data = s.val();
        if (data.enabled) {
            globalCache.bdToKC.set(s.key, data);
            if (data.kcName) globalCache.kcNameToUid.set(data.kcName, data.kcUid);
        }
      });
      rtdb.ref('config/bdToKC').on('child_removed', s => {
          globalCache.bdToKC.delete(s.key);
      });
  } catch(e) { console.error('Failed to load BD-KC map', e); }

  try {
    const snap = await rtdb.ref('config/clipDestinations').get();
    globalCache.clipDestinations.clear();
    if (snap.exists()) {
      const all = snap.val() || {};
      for (const [guildId, cfg] of Object.entries(all)) {
        if (cfg?.channelId) globalCache.clipDestinations.set(guildId, cfg.channelId);
      }
    }
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
  } catch (e) {}

  try {
    const snap = await rtdb.ref('config/bdDestinations').get();
    globalCache.bdDestinations.clear();
    if (snap.exists()) {
      const all = snap.val() || {};
      for (const [guildId, cfg] of Object.entries(all)) {
        if (cfg?.channelId) globalCache.bdDestinations.set(guildId, cfg.channelId);
      }
    }
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
  } catch (e) {}

  try {
    const snap = await rtdb.ref('config/bdJoinLogsChannel').get();
    globalCache.bdJoinLogs.clear();
    if (snap.exists()) {
      const all = snap.val() || {};
      for (const [guildId, cfg] of Object.entries(all)) {
        if (cfg?.channelId) globalCache.bdJoinLogs.set(guildId, cfg.channelId);
      }
    }
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
  } catch (e) {}

  try {
    const usersSnap = await rtdb.ref('users').get();
    if (usersSnap.exists()) {
        Object.keys(usersSnap.val()).forEach(attachPostsListener);
    }
    rtdb.ref('users').on('child_added', s => attachPostsListener(s.key));
  } catch (e) {}

  try {
    const snap2 = await rtdb.ref('config/battleDestinations').get();
    globalCache.battleDestinations.clear();
    if (snap2.exists()) {
      const all = snap2.val() || {};
      for (const [guildId, cfg] of Object.entries(all)) {
        if (cfg?.channelId) globalCache.battleDestinations.set(guildId, cfg.channelId);
      }
    }
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
  } catch (e) {}

  // We rely on the Poller to start status updates.
});

function attachPostsListener(uid) {}

(async () => {
  try {
    console.log('Registering slash commands…');
    await registerCommands();
    console.log('Slash command registration complete.');
  } catch (e) {
    console.error('registerCommands FAILED:', e?.rawError || e);
  }

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
    
    await loadBdTop();
    setInterval(async () => {
        if(bdTopDirty) {
            bdTopDirty = false;
            await saveBdTop();
        }
    }, 30000).unref();

    const gracefulSave = async (exitCode = 0) => {
      try {
        if (bdTopDirty) {
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

    await client.login(process.env.DISCORD_BOT_TOKEN);

    pollBattledome().catch(e => console.error('BD Poller failed to start:', e));
    warmBdCachesForever().catch(err => console.error('[BD Warm] fatal:', err));
    setInterval(() => renewBotLock().catch(() => {}), 30_000).unref();
  }

  startWhenLocked().catch(e => {
    console.error('Failed to start bot:', e);
    process.exit(1);
  });
})();
