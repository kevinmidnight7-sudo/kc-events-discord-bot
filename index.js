// index.js
// Discord.js v14 + Firebase Admin (Realtime Database)
// Commands: /link (DMs auth link), /badges (public profile embed)

require('dotenv').config();

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

async function getKCProfile(uid) {
  const [userSnap, badgeSnap, postsSnap] = await Promise.all([
    rtdb.ref(`users/${uid}`).get(),
    rtdb.ref(`badges/${uid}`).get(),
    rtdb.ref(`users/${uid}/posts`).get(), // posts live under users/<uid>/posts per your rules
  ]);

  const user = userSnap.val() || {};
  const badges = badgeSnap.val() || {};
  const posts = postsSnap.val() || {};

  const contentUnlocked = !!(user.codesUnlocked && user.codesUnlocked.content === true);

  // Posts (only if unlocked)
  let postLines = [];
  if (contentUnlocked && posts) {
    const list = Object.entries(posts)
      .sort((a, b) => (b[1]?.createdAt || 0) - (a[1]?.createdAt || 0))
      .slice(0, 3);

    for (const [, p] of list) {
      const type = (p?.type || '').toLowerCase();
      const cap = p?.caption || '';
      const links = new Set();

      if (p?.url) links.add(p.url); // if your post model has explicit url

      for (const u of extractYouTubeAndTikTokLinks(cap)) links.add(u);

      // add labels if type hints are present but no URL found
      if (links.size === 0) {
        if (type.includes('youtube')) links.add('YouTube');
        if (type.includes('tiktok')) links.add('TikTok');
      }

      const line = [...links].join(' • ');
      if (line) postLines.push(`• ${line}`);
      if (postLines.length >= 3) break;
    }
  }

  // Badges summary
  const badgeLines = [];
  if (badges.offence) badgeLines.push(`🏹 Best Offence x${badges.offence}`);
  if (badges.defence) badgeLines.push(`🛡️ Best Defence x${badges.defence}`);
  if (badges.overall)  badgeLines.push(`🌟 Overall Winner x${badges.overall}`);
  if (user.diamondMember === true) badgeLines.push('💎 Diamond Member');
  // If you add emerald or others in RTDB, add similar lines here

  return {
    displayName: user.displayName || 'Anonymous User',
    about: user.about || 'No "About Me" set.',
    avatar: user.avatar || 'https://kevinmidnight7-sudo.github.io/messageboardkc/1.png',
    bonus: user.bonus || 0,
    streak: Number.isFinite(user.loginStreak) ? String(user.loginStreak) : '—',
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

const commandsJson = [linkCmd, badgesCmd].map(c => c.toJSON());

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
        ephemeral: true,
      });
    }

    if (interaction.commandName === 'badges') {
      const discordId = interaction.user.id;

      // 1) Resolve KC uid from RTDB mapping
      const kcUid = await getKCUidForDiscord(discordId);
      if (!kcUid) {
        return interaction.reply({
          content: 'I can’t find your KC account. Use `/link` to connect it first.',
          ephemeral: true,
        });
      }

      // 2) Load profile data from RTDB
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

      return interaction.reply({ embeds: [embed] }); // PUBLIC
    }
  } catch (err) {
    console.error('Command error:', err);
    if (interaction.deferred || interaction.replied) {
      return interaction.editReply({ content: 'Something went wrong.' });
    }
    return interaction.reply({ content: 'Something went wrong.', ephemeral: true });
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
