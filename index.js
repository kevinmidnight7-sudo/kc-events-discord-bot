// index.js
// Discord.js v14 + Firebase Admin (Realtime Database & Firestore)
// Commands: /link (DMs auth link), /badges (public profile embed), /whoami (debug)

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
  InteractionResponseFlags,
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

  // Badges summary (covers common fields)
  const badgeLines = [];
  if (badges.offence) badgeLines.push(`🏹 Best Offence x${badges.offence}`);
  if (badges.defence) badgeLines.push(`🛡️ Best Defence x${badges.defence}`);
  if (badges.overall)  badgeLines.push(`🌟 Overall Winner x${badges.overall}`);
  if (user.diamondMember === true || badges.diamond === true) badgeLines.push('💎 Diamond Member');

  return {
    displayName,
    about,
    avatar,
    bonus,
    streak,
    badgesText: badgeLines.length ? badgeLines.join('\n') : 'No badges yet.',
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

// Add command:
const whoamiCmd = new SlashCommandBuilder()
  .setName('whoami')
  .setDescription('Show your Discord ID and resolved KC UID');

// Register (include it in commands array)
const commandsJson = [linkCmd, badgesCmd, whoamiCmd].map(c => c.toJSON());


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

  try {
    if (interaction.commandName === 'link') {
      const start = process.env.AUTH_BRIDGE_START_URL;
      const url = `${start}?state=${encodeURIComponent(interaction.user.id)}`;
      return interaction.reply({
        content: `Click to link your account:\n${url}`,
        flags: 64, // 64 is the flag for EPHEMERAL
      });
    }

    if (interaction.commandName === 'whoami') {
        // Defer reply to prevent timeout if database is slow
        await interaction.deferReply({ flags: 64 }); // Ephemeral defer
        const kcUid = await getKCUidForDiscord(interaction.user.id) || 'not linked';
        await interaction.editReply({
            content: `Discord ID: \`${interaction.user.id}\`\nKC UID: \`${kcUid}\``,
        });
        return;
    }

    if (interaction.commandName === 'badges') {
      await interaction.deferReply({ ephemeral: false }); // Public reply

      const discordId = interaction.user.id;

      // 1) Resolve KC uid from RTDB mapping
      const kcUid = await getKCUidForDiscord(discordId);
      if (!kcUid) {
        // The ephemeral status is inherited from deferReply, but we want this error to be private.
        // We edit the original deferred reply to show the error.
        return interaction.editReply({
          content: 'I can’t find your KC account. Use `/link` to connect it first.',
        });
      }

      // 2) Load profile data from RTDB/Firestore
      const p = await getKCProfile(kcUid);

      // 3) Build public embed
      const embed = new EmbedBuilder()
        .setTitle(`${p.displayName} — KC Profile`)
        .setThumbnail(p.avatar)
        .setDescription(p.about)
        .addFields(
          { name: 'Badges', value: p.badgesText, inline: false },
          { name: 'Bonus',  value: String(p.bonus), inline: true },
          { name: 'Streak', value: p.streak,        inline: true },
          { name: 'Posts',  value: p.postsText,     inline: false },
        );

      return interaction.editReply({ embeds: [embed] }); // PUBLIC
    }
  } catch (err) {
    console.error('Command error:', err);
    try {
        if (interaction.deferred || interaction.replied) {
            await interaction.editReply({ content: 'Something went wrong.' });
        } else {
            await interaction.reply({ content: 'Something went wrong.', flags: 64 });
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

(async () => {
  await registerCommands();
  await client.login(process.env.DISCORD_BOT_TOKEN);
})();
