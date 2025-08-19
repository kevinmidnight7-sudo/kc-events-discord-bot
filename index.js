const { Client, GatewayIntentBits, EmbedBuilder, REST, Routes, SlashCommandBuilder } = require('discord.js');
const admin = require('firebase-admin');
const crypto = require('crypto');

/*
 * KC Events Discord Bot
 *
 * This bot registers three slash commands: /link, /badges and /unlink.
 * It uses the Firebase Admin SDK to query and update KC Events user
 * documents stored in Firestore. The bot token, client ID and other
 * secrets must be provided via environment variables. The service
 * account JSON should have access to the `users` and `linkStates`
 * collections in Firestore.
 */

// Initialize Firebase Admin SDK
// Supports two ways of providing credentials: via the JSON string in
// FB_SERVICE_ACCOUNT_JSON or a path specified by FB_SERVICE_ACCOUNT_PATH.
let credential;
if (process.env.FB_SERVICE_ACCOUNT_JSON) {
  const json = JSON.parse(process.env.FB_SERVICE_ACCOUNT_JSON);
  credential = admin.credential.cert(json);
} else if (process.env.FB_SERVICE_ACCOUNT_PATH) {
  const json = require(process.env.FB_SERVICE_ACCOUNT_PATH);
  credential = admin.credential.cert(json);
} else {
  throw new Error('Missing FB_SERVICE_ACCOUNT_JSON or FB_SERVICE_ACCOUNT_PATH environment variable');
}
    admin.initializeApp({
      credential,
      storageBucket: process.env.FB_STORAGE_BUCKET,
      // Provide a Realtime Database URL if available. Without this, admin.database()
      // will attempt to infer the URL from the service account project ID.
      databaseURL: process.env.FB_DATABASE_URL,
    });
    const db = admin.firestore();
    const bucket = admin.storage().bucket();
    // Reference to the Firebase Realtime Database (used for KC profiles and links)
    const rtdb = admin.database();

// Define slash commands
const commands = [
  new SlashCommandBuilder()
    .setName('link')
    .setDescription('Link your Discord account to your KC Events profile'),
  new SlashCommandBuilder()
    .setName('badges')
    .setDescription('View your KC Events profile badges and bonus'),
  new SlashCommandBuilder()
    .setName('unlink')
    .setDescription('Unlink your Discord account from your KC Events profile'),
].map(cmd => cmd.toJSON());

// Register slash commands globally. In a production bot you may want to
// register commands per guild for faster updates.
const rest = new REST({ version: '10' }).setToken(process.env.DISCORD_BOT_TOKEN);
(async () => {
  try {
    console.log('Started refreshing application (/) commands.');
    await rest.put(
      Routes.applicationCommands(process.env.DISCORD_CLIENT_ID),
      { body: commands },
    );
    console.log('Successfully reloaded application (/) commands.');
  } catch (error) {
    console.error('Error registering commands', error);
  }
})();

// Create the Discord client. Only the Guilds intent is required for
// slash commands. Additional intents can be added if you expand the
// functionality later.
const client = new Client({ intents: [GatewayIntentBits.Guilds] });

client.once('ready', () => {
  console.log(`Logged in as ${client.user.tag}`);
});

client.on('interactionCreate', async interaction => {
  if (!interaction.isChatInputCommand()) return;

  try {
    if (interaction.commandName === 'link') {
      // Generate a random UUID for the link state
      const stateId = crypto.randomUUID();
      await db.collection('linkStates').doc(stateId).set({
        createdAt: Date.now(),
        used: false,
        initiator: 'bot',
        discordMessageUserId: interaction.user.id,
      });
      const startUrl = `${process.env.AUTH_BRIDGE_START_URL}?state=${stateId}`;
      await interaction.reply({
        content: `Click the link below to link your account:\n${startUrl}`,
        // Use a transient reply so only the user sees the link
        ephemeral: true,
      });
      } else if (interaction.commandName === 'badges_old') {
      // Find the user by discordId
      const snap = await db
        .collection('users')
        .where('discordId', '==', interaction.user.id)
        .limit(1)
        .get();
      if (snap.empty) {
        await interaction.reply({
          content: 'Not linked yet. Use /link to connect your account.',
          // Ephemeral ensures only the invoking user sees this message
          ephemeral: true,
        });
        return;
      }
      const doc = snap.docs[0];
      const data = doc.data();
      const name = data.displayName || data.username || 'KC Player';
      const bonus = Number.isFinite(data.customBonus) ? data.customBonus : 0;
      let badgeLines = [];
      if (Array.isArray(data.customBadges)) {
        badgeLines = data.customBadges.map(b => `• ${String(b)}`);
      } else if (data.customBadges && typeof data.customBadges === 'object') {
        badgeLines = Object.entries(data.customBadges).map(([k, v]) => `• ${k}: ${String(v)}`);
      }
      const description = badgeLines.length ? badgeLines.join('\n') : 'No badges yet.';
      const embed = new EmbedBuilder()
        .setTitle(`${name} — KC Profile`)
        .addFields(
          { name: 'Badges', value: description },
          { name: 'Bonus', value: String(bonus), inline: true },
        );
      // Prefer Discord avatar URL if present, otherwise fall back to KC avatar if it’s
      // a regular URL (not a data URL)
      if (data.discordAvatarURL) {
        embed.setThumbnail(data.discordAvatarURL);
      } else if (data.avatar && typeof data.avatar === 'string' && !data.avatar.startsWith('data:')) {
        embed.setThumbnail(data.avatar);
      }
      await interaction.reply({
        embeds: [embed],
        // Only visible to the invoking user
        ephemeral: true,
      });
      } else if (interaction.commandName === 'badges') {
        // Build a public KC Events profile embed using Realtime Database and mapping from Discord ID to KC UID
        // Look up the KC user UID via the discordLinks mapping in the Realtime Database
        const linkSnap = await rtdb.ref(`discordLinks/${interaction.user.id}`).once('value');
        if (!linkSnap.exists() || !linkSnap.val() || !linkSnap.val().uid) {
          await interaction.reply({
            content: 'Not linked yet. Use /link to connect your account.',
            // Only visible to the invoking user to avoid clutter
            ephemeral: true,
          });
          return;
        }
        const kcUid = linkSnap.val().uid;
        // Fetch KC profile, badges and posts from the Realtime Database. The website stores
        // profile information under users/{uid}, badge counts under badges/{uid}, and posts
        // under users/{uid}/posts.
        const [userSnap, badgeSnap, postsSnap] = await Promise.all([
          rtdb.ref(`users/${kcUid}`).once('value'),
          rtdb.ref(`badges/${kcUid}`).once('value'),
          rtdb.ref(`users/${kcUid}/posts`).once('value'),
        ]);
        const userData = userSnap.val() || {};
        const badgeCounts = badgeSnap.val() || {};
        const postsData = postsSnap.val() || {};

        // Determine the display name. Fall back to Discord username if no KC name set
        const name = userData.displayName || userData.username || interaction.user.username || 'KC Player';
        // Compose badge list: verified status, skilled badges, and unlockable codes
        const badgeList = [];
        // Verified: check user flags that indicate verification (isVerified, verified, emailVerified)
        if (userData.isVerified || userData.verified || userData.emailVerified) {
          badgeList.push('✅ Verified');
        }
        // Badge counts: check both badgeCounts and userData.badges for offence/defence/overall
        const offenceCount = (badgeCounts.offence || (userData.badges && userData.badges.offence) || 0);
        const defenceCount = (badgeCounts.defence || (userData.badges && userData.badges.defence) || 0);
        const overallCount = (badgeCounts.overall || (userData.badges && userData.badges.overall) || 0);
        if (offenceCount > 0) badgeList.push(`🏹 Best Offence x${offenceCount}`);
        if (defenceCount > 0) badgeList.push(`🛡️ Best Defence x${defenceCount}`);
        if (overallCount > 0) badgeList.push(`🏆 Overall Winner x${overallCount}`);
        // Unlockable codes (diamond, emerald and any other custom codes)
        if (userData.codesUnlocked && typeof userData.codesUnlocked === 'object') {
          if (userData.codesUnlocked.diamond) badgeList.push('💎 Diamond User');
          if (userData.codesUnlocked.emerald) badgeList.push('🟩 Emerald User');
          // Include all other truthy codes
          Object.entries(userData.codesUnlocked).forEach(([key, val]) => {
            if (val && !['diamond', 'emerald'].includes(key)) {
              // Capitalize the key for display if needed
              const label = key
                .split(/[_-]/)
                .map(part => part.charAt(0).toUpperCase() + part.slice(1))
                .join(' ');
              badgeList.push(`🧩 ${label}`);
            }
          });
        }
        const badgeFieldValue = badgeList.length ? badgeList.join('\n') : 'No badges yet.';
        // Bonus and streak fields. If customBonus exists, use it; fallback to bonus; else 0
        const bonusValue = Number.isFinite(userData.customBonus)
          ? userData.customBonus
          : Number.isFinite(userData.bonus)
          ? userData.bonus
          : 0;
        const streakValue = Number.isFinite(userData.loginStreak) ? userData.loginStreak : 0;
        // About me
        const aboutText = userData.aboutMe || 'No "About Me" set.';
        // Build posts summary if posts exist
        let postsFieldValue = null;
        const postLines = [];
        if (postsData && typeof postsData === 'object') {
          Object.values(postsData).forEach(post => {
            if (post && post.type === 'youtube' && post.ytId) {
              const url = `https://youtu.be/${post.ytId}`;
              postLines.push(`[YouTube](${url})`);
            } else if (post && post.type === 'tiktok' && post.videoId) {
              // Construct a TikTok link. We don’t know the username, so link directly via video id.
              const url = `https://www.tiktok.com/@/video/${post.videoId}`;
              postLines.push(`[TikTok](${url})`);
            }
          });
          if (postLines.length > 0) {
            postsFieldValue = postLines.join('\n');
          }
        }
        // Build the embed for the KC profile
        const embed = new EmbedBuilder()
          .setTitle(`${name} — KC Profile`)
          .setDescription(aboutText)
          .addFields(
            { name: 'Badges', value: badgeFieldValue, inline: false },
            { name: 'Bonus', value: String(bonusValue), inline: true },
            {
              name: 'Streak',
              value: streakValue
                ? `🔥 ${streakValue} day${streakValue === 1 ? '' : 's'}`
                : '—',
              inline: true,
            },
          );
        // If posts exist and user has at least one post, include a posts field
        if (postsFieldValue) {
          embed.addFields({ name: 'Posts', value: postsFieldValue, inline: false });
        }
        // Set thumbnail: prefer KC avatar if not data URI; fallback to Discord avatar
        if (
          userData.avatar &&
          typeof userData.avatar === 'string' &&
          !userData.avatar.startsWith('data:')
        ) {
          embed.setThumbnail(userData.avatar);
        } else if (userData.discordAvatarURL) {
          embed.setThumbnail(userData.discordAvatarURL);
        }
        await interaction.reply({
          embeds: [embed],
        });
      } else if (interaction.commandName === 'unlink') {
      // Find the user by discordId and remove Discord fields
      const snap = await db
        .collection('users')
        .where('discordId', '==', interaction.user.id)
        .limit(1)
        .get();
      if (snap.empty) {
        await interaction.reply({
          content: 'No linked account found.',
          // Reply ephemerally when no link is found
          
          
          
          
ephemeral: true,
        });
        return;
      }
      const doc = snap.docs[0];
      await doc.ref.update({
        discordId: admin.firestore.FieldValue.delete(),
        discordUsername: admin.firestore.FieldValue.delete(),
        discordAvatarURL: admin.firestore.FieldValue.delete(),
      });
      await interaction.reply({
        content: 'Your account has been unlinked.',
        // Only the user sees the confirmation
        
        
        
        
ephemeral: true,
      });
    }
  } catch (err) {
    console.error(err);
    await interaction.reply({
      content: 'An error occurred while processing your request.',
      // Hide error messages from other users
      
      
      
      
ephemeral: true,
    });
  }
});

client.login(process.env.DISCORD_BOT_TOKEN);