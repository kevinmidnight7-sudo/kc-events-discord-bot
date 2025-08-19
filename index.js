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
function extractYouTubeAndTikTokLinks(str = '') {
  const urls = [];
  const rx = /(https?:\/\/(?:www\.)?(?:youtube\.com\/watch\?v=[\w-]+|youtu\.be\/[\w-]+|tiktok\.com\/@[A-Za-z0-9_.-]+\/video\/\d+))/gi;
  let m;
  while ((m = rx.exec(str)) !== null) {
    urls.push(m[1]);
    if (urls.length >= 3) break;
  }
  return urls;
}

async function getKCUidForDiscord(discordId) {
  const snap = await rtdb.ref(`discordLinks/${discordId}`).get();
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
    rtdb.ref(`users/${uid}`).get(),
    rtdb.ref(`badges/${uid}`).get(),
    rtdb.ref(`users/${uid}/posts`).get(),
  ]);

  const safeVal = s => (s && s.status === 'fulfilled' && s.value && s.value.exists()) ? s.value.val() : null;
  let user   = safeVal(userSnapRT)  || {};
  let badges = safeVal(badgeSnapRT) || {};
  let posts  = safeVal(postsSnapRT) || {};

  // 2) Firestore fallbacks (if RTDB missing)
  if (!user || Object.keys(user).length === 0) {
    try {
      const fsUser = await firestore.collection('users').doc(uid).get();
      if (fsUser.exists) user = fsUser.data() || {};
    } catch (_) {}
  }
  if (!posts || Object.keys(posts).length === 0) {
    try {
      const fsPosts = await firestore.collection('users').doc(uid).collection('posts').get();
      if (!fsPosts.empty) {
        posts = {};
        fsPosts.forEach(d => { posts[d.id] = d.data(); });
      }
    } catch (_) {}
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
  const badgeLines = [];
  if (isVerified)                       badgeLines.push('✅ Verified');
  if (counts.offence > 0)               badgeLines.push(`🏹 Best Offence x${counts.offence}`);
  if (counts.defence > 0)               badgeLines.push(`🛡️ Best Defence x${counts.defence}`);
  if (counts.overall > 0)               badgeLines.push(`🌟 Overall Winner x${counts.overall}`);
  if (hasDiamond)                        badgeLines.push('💎 Diamond User');
  if (hasEmerald)                        badgeLines.push('🟩 Emerald User');

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
  .setDescription('Show your KC Events profile');

const whoamiCmd = new SlashCommandBuilder()
  .setName('whoami')
  .setDescription('Show your Discord ID and resolved KC UID');

const dumpCmd = new SlashCommandBuilder()
  .setName('dumpme')
  .setDescription('Debug: dump raw keys for your mapped KC UID');

// Register (include it in commands array)
const commandsJson = [linkCmd, badgesCmd, whoamiCmd, dumpCmd].map(c => c.toJSON());


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
  if (!interaction.isChatInputCommand()) return;

  if (interaction.commandName === 'link') {
    const start = process.env.AUTH_BRIDGE_START_URL;
    const url = `${start}?state=${encodeURIComponent(interaction.user.id)}`;
    return interaction.reply({ content: `Click to link your account:\n${url}`, ephemeral: true });
  }

  if (interaction.commandName === 'whoami') {
      await interaction.deferReply({ ephemeral: true });
      const kcUid = await getKCUidForDiscord(interaction.user.id) || 'not linked';
      await interaction.editReply({
          content: `Discord ID: \`${interaction.user.id}\`\nKC UID: \`${kcUid}\``,
      });
      return;
  }

  if (interaction.commandName === 'dumpme') {
    await interaction.deferReply({ ephemeral: true });
    const discordId = interaction.user.id;
    const uid = await getKCUidForDiscord(discordId);
    if (!uid) return interaction.editReply('Not linked. Run `/link` first.');

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

      return interaction.editReply('```json\n' + JSON.stringify(payload, null, 2).slice(0, 1900) + '\n```');
    } catch (e) {
      console.error('dumpme error:', e);
      return interaction.editReply('Error reading data (see logs).');
    }
  }

  if (interaction.commandName === 'badges') {
    try {
      await interaction.deferReply(); // public

      const discordId = interaction.user.id;
      const kcUid = await getKCUidForDiscord(discordId);
      if (!kcUid) {
        console.warn('badges: no mapping for', discordId);
        return interaction.editReply('I can’t find your KC account. Use `/link` to connect it first.');
      }

      let profile;
      try {
        profile = await getKCProfile(kcUid);
      } catch (e) {
        console.error('getKCProfile threw:', e);
        return interaction.editReply('Profile lookup failed (see logs).');
      }
      
      if (!profile) {
        console.warn('badges: empty profile for', kcUid);
        return interaction.editReply('No profile data found.');
      }

      const title       = clampStr(`${profile.displayName} — KC Profile`, 256, 'KC Profile');
      const description = clampStr(profile.about, 4096);
      const badgesVal   = clampStr(profile.badgesText, 1024);
      const bonusVal    = clampStr(profile.bonus, 1024, '—');
      const streakVal   = clampStr(profile.streak, 1024, '—');
      const postsVal    = clampStr(profile.postsText, 1024);
      
      // Use the requester's *Discord* avatar (short CDN URL)
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

      // Tint the embed with the user’s chosen name colour (or gradient’s first colour)
      if (profile.embedColor) embed.setColor(profile.embedColor);

      return interaction.editReply({ embeds: [embed] });
    } catch (err) {
      console.error('badges command error:', err);
      console.error('raw error body:', err?.rawError);
      console.error('raw errors tree:', err?.rawError?.errors);
      try {
        if (interaction.deferred || interaction.replied) {
          await interaction.editReply('Something went wrong (see logs).');
        } else {
          await interaction.reply({ content: 'Something went wrong (see logs).', ephemeral: true });
        }
      } catch (e) {
        console.error('failed to send error reply:', e);
      }
    }
    return;
  }
});

// ---------- Startup ----------
client.once('ready', () => {
  console.log(`Logged in as ${client.user.tag}`);
});

(async () => {
  await registerCommands();
  await client.login(process.env.DISCORD_BOT_TOKEN);
})();
