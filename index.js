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
});
const db = admin.firestore();
const bucket = admin.storage().bucket();

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
        // Find the KC user document by the Discord snowflake and build a public profile embed
        const snap = await db
          .collection('users')
          .where('discordId', '==', interaction.user.id)
          .limit(1)
          .get();
        if (snap.empty) {
          // If no linked account is found, reply privately to avoid channel clutter
          await interaction.reply({
            content: 'Not linked yet. Use /link to connect your account.',
            ephemeral: true,
          });
          return;
        }
        const doc = snap.docs[0];
        const data = doc.data();
        const name = data.displayName || data.username || 'KC Player';
        // Build a list of badge descriptions based off the KC profile
        const badgeList = [];
        // Verified status
        if (data.isVerified || data.verified || data.emailVerified) {
          badgeList.push('✅ Verified');
        }
        // Skilled badges: offence/defence/overall counts
        if (data.badges) {
          if (data.badges.offence && data.badges.offence > 0) {
            badgeList.push(`🏹 Best Offence x${data.badges.offence}`);
          }
          if (data.badges.defence && data.badges.defence > 0) {
            badgeList.push(`🛡️ Best Defence x${data.badges.defence}`);
          }
          if (data.badges.overall && data.badges.overall > 0) {
            badgeList.push(`🏆 Overall Winner x${data.badges.overall}`);
          }
        }
        // Unlockable codes: diamond, emerald, and any other truthy flags
        if (data.codesUnlocked && typeof data.codesUnlocked === 'object') {
          if (data.codesUnlocked.diamond) badgeList.push('💎 Diamond User');
          if (data.codesUnlocked.emerald) badgeList.push('🟩 Emerald User');
          Object.entries(data.codesUnlocked).forEach(([key, val]) => {
            if (val && !['diamond', 'emerald'].includes(key)) {
              badgeList.push(`🧩 ${key}`);
            }
          });
        }
        const badgeFieldValue = badgeList.length
          ? badgeList.join('\n')
          : 'No badges yet.';
        // Bonus and streak values
        const bonusValue = Number.isFinite(data.customBonus)
          ? data.customBonus
          : Number.isFinite(data.bonus)
          ? data.bonus
          : 0;
        const streakValue = Number.isFinite(data.loginStreak) ? data.loginStreak : 0;
        // About Me description
        const aboutText = data.aboutMe || 'No "About Me" set.';
        // Build the embed using KC profile fields
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
        // Prefer KC avatar URL if present and not a data URL; fallback to Discord avatar
        if (
          data.avatar &&
          typeof data.avatar === 'string' &&
          !data.avatar.startsWith('data:')
        ) {
          embed.setThumbnail(data.avatar);
        } else if (data.discordAvatarURL) {
          embed.setThumbnail(data.discordAvatarURL);
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