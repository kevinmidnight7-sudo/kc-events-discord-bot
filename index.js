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

async function safeDefer(interaction, opts = {}) {
  // If we’re already acknowledged, skip
  if (interaction.deferred || interaction.replied) return true;

  // If the token is about to expire, bail (prevents 10062)
  const age = Date.now() - interaction.createdTimestamp;
  if (age > 2500) return false;

  try {
    await interaction.deferReply(opts);
    return true;
  } catch (err) {
    if (isUnknownInteraction(err)) return false;
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
    if (isUnknownInteraction(err)) return null;
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

// Always produce a user-visible message even if interaction token expired
async function finalRespond(interaction, data, fallbackText = null) {
  try {
    if (interaction.deferred || interaction.replied) {
      return await interaction.editReply(data);
    }
    return await interaction.reply({ ...data, ephemeral: true });
  } catch (err) {
    if (isUnknownInteraction(err) && interaction.channel && fallbackText) {
      // Token expired: send a normal message in channel instead of failing silently
      try { await interaction.channel.send(fallbackText); } catch (_) {}
      return null;
    }
    throw err;
  }
}

async function loadLeaderboardData() {
  const [usersSnap, badgesSnap] = await Promise.all([
    rtdb.ref('users').get(),
    rtdb.ref('badges').get(),
  ]);
  const users = usersSnap.val() || {};
  const badges = badgesSnap.val() || {};
  return Object.entries(users).map(([uid, u]) => {
    const b = badges[uid] || {};
    return {
      name: u.displayName || u.email || '(unknown)',
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

  // Compile recent post links (YouTube/TikTok)
  const extractYouTubeAndTikTokLinks = (str = '') => {
    const urls = [];
    const rx = /(https?:\/\/(?:www\.)?(?:youtube\.com\/watch\?v=[\w-]+|youtu\.be\/[\w-]+|tiktok\.com\/@[A-Za-z0-9_.-]+\/video\/\d+))/gi;
    let m; while ((m = rx.exec(str)) && urls.length < 3) urls.push(m[1]);
    return urls;
  };

  let postLines = [];
  if (contentUnlocked && posts) {
    const list = Object.entries(posts)
      .sort((a, b) => (b[1]?.createdAt || 0) - (a[1]?.createdAt || 0))
      .slice(0, 3);

    for (const [, p] of list) {
      const cap = p?.caption || '';
      const links = new Set();
      if (p?.url) links.add(p.url);
      for (const u of extractYouTubeAndTikTokLinks(cap)) links.add(u);
      if (links.size) postLines.push(`• ${[...links].join(' • ')}`);
      if (postLines.length >= 3) break;
    }
  }

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
    postsText: contentUnlocked ? (postLines.length ? postLines.join('\n') : '—') : '—',
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

// Register (include it in commands array)
const commandsJson = [linkCmd, badgesCmd, whoamiCmd, dumpCmd, lbCmd].map(c => c.toJSON());


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

  if (interaction.isButton() && interaction.customId.startsWith('lb:')) {
    // customId format: lb:type:catIdx:page
    const [, type, catStr, pageStr] = interaction.customId.split(':');
    const catIdx = Math.max(0, Math.min(2, parseInt(catStr,10) || 0));
    const page = Math.max(0, parseInt(pageStr,10) || 0);

    // Find the rows. If message belongs to a previous interaction, rebuild:
    let rows;
    const cache = interaction.client.lbCache?.get(interaction.message.interaction?.id || '');
    if (Array.isArray(cache)) rows = cache;
    else rows = await loadLeaderboardData();

    const embed = buildLbEmbed(rows, catIdx, page);
    return interaction.update({ embeds: [embed], components: [lbRow(catIdx, page)] });
  }

  if (!interaction.isChatInputCommand()) return;

  if (interaction.commandName === 'link') {
    const start = process.env.AUTH_BRIDGE_START_URL;
    const url = `${start}?state=${encodeURIComponent(interaction.user.id)}`;
    return interaction.reply({ content: `Click to link your account:\n${url}`, ephemeral: true });
  }

  if (interaction.commandName === 'whoami') {
    const ok = await safeDefer(interaction, { ephemeral: true });
    if (!ok) {
      if (interaction.channel) await interaction.channel.send('Sorry, that timed out. Please run `/whoami` again.');
      return;
    }

    console.time(`whoami:${interaction.id}`);
    let kcUid = null;
    try {
      kcUid = await getKCUidForDiscord(interaction.user.id) || 'not linked';
      await finalRespond(
        interaction,
        { content: `Discord ID: \`${interaction.user.id}\`\nKC UID: \`${kcUid}\`` },
        'The whoami reply expired. Please try again.'
      );
    } catch (e) {
      console.error('whoami error:', e);
      await finalRespond(interaction, { content: 'Something went wrong.' }, 'whoami failed, please try again.');
    } finally {
      console.timeEnd(`whoami:${interaction.id}`);
    }
    return;
  }

  if (interaction.commandName === 'dumpme') {
    const ok = await safeDefer(interaction, { ephemeral: true });
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

      return safeEdit(interaction, { content: '```json\n' + JSON.stringify(payload, null, 2).slice(0, 1900) + '\n```' });
    } catch (e) {
      console.error('dumpme error:', e);
      return safeEdit(interaction, { content: 'Error reading data (see logs).' });
    }
  }

  if (interaction.commandName === 'leaderboard') {
    const ok = await safeDefer(interaction); // public
    if (!ok) return;
    const rows = await loadLeaderboardData();
    const catIdx = 0, page = 0;
    const embed = buildLbEmbed(rows, catIdx, page);
    return safeEdit(interaction, { embeds: [embed], components: [lbRow(catIdx, page)] })
      .then(msg => { /* store rows in memory for button handler */ interaction.client.lbCache ??= new Map(); interaction.client.lbCache.set(interaction.id, rows); });
  }

  if (interaction.commandName === 'badges') {
    const ok = await safeDefer(interaction); // public
    if (!ok) {
      if (interaction.channel) await interaction.channel.send('Sorry, that timed out. Please run `/badges` again.');
      return;
    }

    console.time(`badges:${interaction.id}`);
    try {
      // target: provided user or self
      const target = interaction.options.getUser('user') || interaction.user;
      const discordId = target.id;

      const kcUid = await getKCUidForDiscord(discordId);
      if (!kcUid) {
        return safeEdit(interaction, {
          content: target.id === interaction.user.id
            ? 'I can’t find your KC account. Use `/link` to connect it first.'
            : `I can’t find a KC account linked to **${target.tag}**.`
        });
      }

      const profile = await withTimeout(getKCProfile(kcUid), 5000, `getKCProfile(${kcUid})`);

      if (!profile) {
        return finalRespond(interaction, { content: 'No profile data found.' }, 'No profile data found.');
      }

      const title       = clampStr(`${profile.displayName} — KC Profile`, 256, 'KC Profile');
      const description = clampStr(profile.about, 4096);
      const badgesVal   = clampStr(profile.badgesText, 1024);
      const bonusVal    = clampStr(profile.bonus, 1024, '—');
      const streakVal   = clampStr(profile.streak, 1024, '—');
      const postsVal    = clampStr(profile.postsText, 1024);

      const discordAvatar = interaction.user.displayAvatarURL({ extension: 'png', size: 128 });

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
          .setURL('https://kcevents.uk/#loginpage') // or append ?uid=${kcUid} if you want
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
  if (isUnknownInteraction(err)) {
    console.warn('Ignored Unknown interaction (token expired).');
    return;
  }
  console.error('unhandledRejection:', err);
});

client.on('error', (err) => {
  if (isUnknownInteraction(err)) {
    console.warn('Client error: Unknown interaction (token expired).');
    return;
  }
  console.error('Client error:', err);
});

(async () => {
  await registerCommands();
  await client.login(process.env.DISCORD_BOT_TOKEN);
})();
