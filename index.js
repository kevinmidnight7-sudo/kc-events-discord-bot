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

  // --- 1) Try RTDB first ---
  const [userSnapRT, badgeSnapRT, postsSnapRT] = await Promise.all([
    rtdb.ref(`users/${uid}`).get(),
    rtdb.ref(`badges/${uid}`).get(),
    rtdb.ref(`users/${uid}/posts`).get(),
  ]);

  let user = userSnapRT.val();
  let badges = badgeSnapRT.val();
  let posts  = postsSnapRT.val();

  // --- 2) If RTDB user is missing or empty, try Firestore fallbacks ---
  if (!user || Object.keys(user).length === 0) {
    try {
      const fsUser = await firestore.collection('users').doc(uid).get();
      if (fsUser.exists) user = fsUser.data();
    } catch (e) {
      console.warn('FS user read failed:', e);
    }
  }

  if (!badges || Object.keys(badges).length === 0) {
    try {
      // common FS layouts: badges/{uid} or badges/users/{uid}
      const fs1 = await firestore.collection('badges').doc(uid).get();
      if (fs1.exists) badges = fs1.data();
      else {
        const fs2 = await firestore.collection('badges').doc('users').collection('users').doc(uid).get();
        if (fs2.exists) badges = fs2.data();
      }
    } catch (e) {
      console.warn('FS badges read failed:', e);
    }
  }

  if (!posts || Object.keys(posts).length === 0) {
    try {
      // try Firestore posts: users/{uid}/posts
      const fsPosts = await firestore.collection('users').doc(uid).collection('posts').get();
      if (!fsPosts.empty) {
        posts = {};
        fsPosts.forEach(d => { posts[d.id] = d.data(); });
      }
    } catch (e) {
      console.warn('FS posts read failed:', e);
    }
  }

  // still allow empty objects to avoid crashes
  user = user || {};
  badges = badges || {};
  posts = posts || {};

  // field name differences
  // RTDB uses "about"; some sites use "aboutMe" or "bio" in FS
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

  const avatar =
    user.avatar ??
    user.photoURL ??
    'https://kevinmidnight7-sudo.github.io/messageboardkc/1.png';

  const bonus = user.bonus || 0;
  const streak = Number.isFinite(user.loginStreak) ? String(user.loginStreak) : '—';

  // content unlock flag can be in different shapes; try a few
  const contentUnlocked =
    !!(user.codesUnlocked?.content) ||
    !!user.postsUnlocked ||
    !!user.canPost;

  // Posts (YouTube/TikTok only) — latest 3
  let postLines = [];
  if (contentUnlocked && posts) {
    const list = Object.entries(posts)
      .sort((a, b) => (b[1]?.createdAt || 0) - (a[1]?.createdAt || 0))
      .slice(0, 3);

    for (const [, p] of list) {
      const type = (p?.type || '').toLowerCase();
      const cap = p?.caption || '';
      const links = new Set();
      if (p?.url) links.add(p.url);

      for (const u of extractYouTubeAndTikTokLinks(cap)) links.add(u);

      if (links.size === 0) {
        if (type.includes('youtube')) links.add('YouTube');
        if (type.includes('tiktok')) links.add('TikTok');
      }

      const line = [...links].join(' • ');
      if (line) postLines.push(`• ${line}`);
      if (postLines.length >= 3) break;
    }
  }

  // ----- Badges summary (robust across common shapes) -----
  const badgeLines = [];

  // 1) Trophy/counter-style badges (either RTDB or Firestore)
  const offenceCount = badges.offence ?? badges.bestOffence ?? badges.offense; // spelling variants
  const defenceCount = badges.defence ?? badges.bestDefence ?? badges.defense;
  const overallCount = badges.overall ?? badges.overallWins ?? badges.totalWins;

  if (Number.isFinite(offenceCount) && offenceCount > 0) {
    badgeLines.push(`🏹 Best Offence x${offenceCount}`);
  }
  if (Number.isFinite(defenceCount) && defenceCount > 0) {
    badgeLines.push(`🛡️ Best Defence x${defenceCount}`);
  }
  if (Number.isFinite(overallCount) && overallCount > 0) {
    badgeLines.push(`🌟 Overall Winner x${overallCount}`);
  }

  // 2) “Membership/flag” style badges (booleans or array of strings)
  // Try a user.badges object/array first (many sites store “Verified”, “Diamond”, etc. here).
  const pretty = (k) => {
    const map = {
      verified: '✅ Verified',
      diamond: '💎 Diamond User',
      diamondmember: '💎 Diamond User',
      emerald: '💚 Emerald User',
      emeraldmember: '💚 Emerald User',
      mrupdater: '👑 mr updater',
      updater: '👑 mr updater',
      vl: '🏎️ VL »»',
      vip: '⭐ VIP',
    };
    const key = String(k).toLowerCase().replace(/\s+/g, '');
    return map[key] || `• ${k}`;
  };

  if (Array.isArray(user.badges)) {
    for (const k of user.badges) badgeLines.push(pretty(k));
  } else if (user.badges && typeof user.badges === 'object') {
    for (const [k, v] of Object.entries(user.badges)) {
      if (v === true || v === 'true' || v === 1) badgeLines.push(pretty(k));
    }
  }

  // 3) Common single-field flags on user or badges
  if (user.diamondMember === true || badges.diamond === true) badgeLines.push('💎 Diamond User');
  if (user.verified === true || badges.verified === true) badgeLines.push('✅ Verified');
  if (user.emeraldMember === true || badges.emerald === true) badgeLines.push('💚 Emerald User');

  // Final text
  const badgesText = badgeLines.length ? [...new Set(badgeLines)].join('\n') : 'No badges yet.';


  return {
    displayName,
    about,
    avatar,
    bonus,
    streak,
    badgesText,
    postsText: contentUnlocked ? (postLines.length ? postLines.join('\n') : '—') : '—',
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
      
      // Prefer Discord avatar to avoid very long KC URLs
      // (discord.js v14 – displayAvatarURL() returns a safe CDN URL)
      const discordAvatar = interaction.user.displayAvatarURL({ size: 256 });

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
