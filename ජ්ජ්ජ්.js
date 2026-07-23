// index.js
const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const moment = require('moment-timezone');
const Jimp = require('jimp');
const crypto = require('crypto');
const axios = require('axios');
const FileType = require('file-type');
const fetch = require('node-fetch');
const { MongoClient } = require('mongodb');

const {
  default: makeWASocket,
  useMultiFileAuthState,
  delay,
  getContentType,
  makeCacheableSignalKeyStore,
  Browsers,
  jidNormalizedUser,
  downloadContentFromMessage,
  DisconnectReason,
  proto
} = require('baileys');

const config = require('./config');
const animeCmds = require('./cmd/anime');
const aestheticCmds = require('./cmd/kelumxz');
const primeCmds = require('./cmd/prime');

// ============== SETTINGS MAP ==============
const SETTINGS_MAP = {
  '1.1': { key: 'WORK_TYPE', value: 'public', label: 'Public' },
  '1.2': { key: 'WORK_TYPE', value: 'inbox', label: 'Inbox Only' },
  '1.3': { key: 'WORK_TYPE', value: 'groups', label: 'Groups Only' },
  '1.4': { key: 'WORK_TYPE', value: 'group_admins', label: 'Group Admins Only' },
  '1.5': { key: 'WORK_TYPE', value: 'channel', label: 'Channel Only' },
  '1.6': { key: 'WORK_TYPE', value: 'private', label: 'Private (Owner Only)' },
  '2.1': { key: 'FAKE_PRESENCE', value: 'typing', label: 'Fake Typing', extra: { AUTO_TYPING: 'true', AUTO_RECORDING: 'false' } },
  '2.2': { key: 'FAKE_PRESENCE', value: 'recording', label: 'Fake Recording', extra: { AUTO_TYPING: 'false', AUTO_RECORDING: 'true' } },
  '2.3': { key: 'FAKE_PRESENCE', value: 'off', label: 'Off', extra: { AUTO_TYPING: 'false', AUTO_RECORDING: 'false' } },
  '3.1': { key: 'PRESENCE', value: 'available', label: 'Always Online' },
  '3.2': { key: 'PRESENCE', value: 'unavailable', label: 'Always Offline' },
  '4.1': { key: 'STATUS_VIEW', value: 'view', label: 'Auto Status View', extra: { AUTO_VIEW_STATUS: 'true', AUTO_LIKE_STATUS: 'false' } },
  '4.2': { key: 'STATUS_VIEW', value: 'view_like', label: 'Auto View + Like', extra: { AUTO_VIEW_STATUS: 'true', AUTO_LIKE_STATUS: 'true' } },
  '4.3': { key: 'STATUS_VIEW', value: 'off', label: 'Off', extra: { AUTO_VIEW_STATUS: 'false', AUTO_LIKE_STATUS: 'false' } },
  '5.1': { key: 'AUTO_READ_MESSAGE', value: 'all', label: 'Read All Messages' },
  '5.2': { key: 'AUTO_READ_MESSAGE', value: 'cmd', label: 'Read Commands Only' },
  '5.3': { key: 'AUTO_READ_MESSAGE', value: 'off', label: 'Off' },
  '6.1': { key: 'ANTI_CALL', value: 'on', label: 'Reject Call' },
  '6.2': { key: 'ANTI_CALL', value: 'reject_msg', label: 'Reject With Message' },
  '6.3': { key: 'ANTI_CALL', value: 'off', label: 'Off' },
  '7.1': { key: 'ANTI_DELETE_MSG', value: 'enable', label: 'Enable' },
  '7.2': { key: 'ANTI_DELETE_MSG', value: 'disable', label: 'Disable' },
  '8.1': { key: 'ANTI_DELETE_STATUS', value: 'enable', label: 'Enable' },
  '8.2': { key: 'ANTI_DELETE_STATUS', value: 'disable', label: 'Disable' },
  '9.1': { key: 'PREFIX_ENABLED', value: 'on', label: 'Prefix On' },
  '9.2': { key: 'PREFIX_ENABLED', value: 'off', label: 'Prefix Off' },
  '10.1': { key: 'STYLE', value: 'anime', label: 'Anime Style' },
  '10.2': { key: 'STYLE', value: 'aesthetic', label: 'Aesthetic Style' },
  '10.3': { key: 'STYLE', value: 'prime', label: 'Prime Style' },
  '11.1': { key: 'AUTO_REPLY', value: 'enable', label: 'Auto Reply On' },
  '11.2': { key: 'AUTO_REPLY', value: 'disable', label: 'Auto Reply Off' },
};

// Default config for reset
const DEFAULT_USER_CONFIG = {
  WORK_TYPE: 'private',
  FAKE_PRESENCE: 'off',
  PRESENCE: 'available',
  STATUS_VIEW: 'view',
  AUTO_VIEW_STATUS: 'true',
  AUTO_LIKE_STATUS: 'false',
  AUTO_READ_MESSAGE: 'off',
  ANTI_CALL: 'off',
  ANTI_DELETE_MSG: 'disable',
  ANTI_DELETE_STATUS: 'disable',
  PREFIX_ENABLED: 'on',
  STYLE: 'anime',
  AUTO_TYPING: 'false',
  AUTO_RECORDING: 'false',
  AUTO_REPLY: 'disable',
  AUTO_LIKE_EMOJI: ['🎀', '🧃', '🪼', '🦄', '🍒', '🍫', '🧸', '☁️', '🌟', '👀', '💎'],
  botName: '© ᴀꜱʜɪ ᴍᴅ ᴍɪɴɪ',
  ownerName: 'Abdul kalam',
  ownerDetails: '',
  likeEmoji: '❤️',
  logo: '',
};

// ============== MONGO ==============

let mongoClient, mongoDB;
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol, autoRepliesCol;

async function initMongo() {
  try { if (mongoClient?.topology?.isConnected?.()) return; } catch (e) { }
  mongoClient = new MongoClient(config.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
  await mongoClient.connect();
  mongoDB = mongoClient.db(config.MONGO_DB);
  sessionsCol = mongoDB.collection('sessions');
  numbersCol = mongoDB.collection('numbers');
  adminsCol = mongoDB.collection('admins');
  newsletterCol = mongoDB.collection('newsletter_list');
  configsCol = mongoDB.collection('configs');
  newsletterReactsCol = mongoDB.collection('newsletter_reacts');
  autoRepliesCol = mongoDB.collection('auto_replies');
  await sessionsCol.createIndex({ number: 1 }, { unique: true });
  await numbersCol.createIndex({ number: 1 }, { unique: true });
  await newsletterCol.createIndex({ jid: 1 }, { unique: true });
  await newsletterReactsCol.createIndex({ jid: 1 }, { unique: true });
  await configsCol.createIndex({ number: 1 }, { unique: true });
  await autoRepliesCol.createIndex({ number: 1, trigger: 1 }, { unique: true });
  console.log('✅ Mongo initialized');
}

async function saveCredsToMongo(number, creds, keys = null) {
  try { await initMongo(); const s = number.replace(/[^0-9]/g, ''); await sessionsCol.updateOne({ number: s }, { $set: { number: s, creds, keys, updatedAt: new Date() } }, { upsert: true }); } catch (e) { console.error('saveCredsToMongo:', e); }
}
async function loadCredsFromMongo(number) {
  try { await initMongo(); return await sessionsCol.findOne({ number: number.replace(/[^0-9]/g, '') }) || null; } catch (e) { return null; }
}
async function removeSessionFromMongo(number) {
  try { await initMongo(); await sessionsCol.deleteOne({ number: number.replace(/[^0-9]/g, '') }); } catch (e) { }
}
async function addNumberToMongo(number) {
  try { await initMongo(); const s = number.replace(/[^0-9]/g, ''); await numbersCol.updateOne({ number: s }, { $set: { number: s } }, { upsert: true }); } catch (e) { }
}
async function removeNumberFromMongo(number) {
  try { await initMongo(); await numbersCol.deleteOne({ number: number.replace(/[^0-9]/g, '') }); } catch (e) { }
}
async function getAllNumbersFromMongo() {
  try { await initMongo(); return (await numbersCol.find({}).toArray()).map(d => d.number); } catch (e) { return []; }
}
async function loadAdminsFromMongo() {
  try { await initMongo(); return (await adminsCol.find({}).toArray()).map(d => d.jid || d.number).filter(Boolean); } catch (e) { return []; }
}
async function addAdminToMongo(j) { try { await initMongo(); await adminsCol.updateOne({ jid: j }, { $set: { jid: j } }, { upsert: true }); } catch (e) { } }
async function removeAdminFromMongo(j) { try { await initMongo(); await adminsCol.deleteOne({ jid: j }); } catch (e) { } }
async function addNewsletterToMongo(jid, emojis = []) { try { await initMongo(); await newsletterCol.updateOne({ jid }, { $set: { jid, emojis: Array.isArray(emojis) ? emojis : [], addedAt: new Date() } }, { upsert: true }); } catch (e) { throw e; } }
async function removeNewsletterFromMongo(jid) { try { await initMongo(); await newsletterCol.deleteOne({ jid }); } catch (e) { throw e; } }
async function listNewslettersFromMongo() { try { await initMongo(); return (await newsletterCol.find({}).toArray()).map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] })); } catch (e) { return []; } }
async function saveNewsletterReaction(jid, messageId, emoji, sessionNumber) { try { await initMongo(); await mongoDB.collection('newsletter_reactions_log').insertOne({ jid, messageId, emoji, sessionNumber, ts: new Date() }); } catch (e) { } }

async function setUserConfigInMongo(number, conf) {
  try { await initMongo(); const s = number.replace(/[^0-9]/g, ''); await configsCol.updateOne({ number: s }, { $set: { number: s, config: conf, updatedAt: new Date() } }, { upsert: true }); } catch (e) { console.error('setUserConfigInMongo:', e); }
}
async function loadUserConfigFromMongo(number) {
  try { await initMongo(); const doc = await configsCol.findOne({ number: number.replace(/[^0-9]/g, '') }); return doc ? doc.config : null; } catch (e) { return null; }
}

async function listNewsletterReactsFromMongo() { try { await initMongo(); return (await newsletterReactsCol.find({}).toArray()).map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] })); } catch (e) { return []; } }

// ============== AUTO REPLY MONGO ==============

async function addAutoReply(number, trigger, reply, type = 'text', imageUrl = '') {
  await initMongo();
  const sn = number.replace(/[^0-9]/g, '');
  await autoRepliesCol.updateOne(
    { number: sn, trigger: trigger.toLowerCase().trim() },
    { $set: { number: sn, trigger: trigger.toLowerCase().trim(), reply, type, imageUrl, updatedAt: new Date() } },
    { upsert: true }
  );
}

async function removeAutoReply(number, trigger) {
  await initMongo();
  const sn = number.replace(/[^0-9]/g, '');
  const result = await autoRepliesCol.deleteOne({ number: sn, trigger: trigger.toLowerCase().trim() });
  return result.deletedCount > 0;
}

async function getAutoReplies(number) {
  await initMongo();
  const sn = number.replace(/[^0-9]/g, '');
  return await autoRepliesCol.find({ number: sn }).toArray();
}

async function findAutoReply(number, text) {
  await initMongo();
  const sn = number.replace(/[^0-9]/g, '');
  return await autoRepliesCol.findOne({ number: sn, trigger: text.toLowerCase().trim() });
}

async function removeImageReply(number, trigger) {
  await initMongo();
  const sn = number.replace(/[^0-9]/g, '');
  const result = await autoRepliesCol.deleteOne({ number: sn, trigger: trigger.toLowerCase().trim(), type: 'image' });
  return result.deletedCount > 0;
}

// ============== Utils ==============

function formatMessage(title, content, footer) { return `*${title}*\n\n${content}\n\n> *${footer}*`; }
function generateOTP() { return Math.floor(100000 + Math.random() * 900000).toString(); }
function getSriLankaTimestamp() { return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); }

const activeSockets = new Map();
const socketCreationTime = new Map();
const otpStore = new Map();
const messageCache = new Map();
const settingsTracker = new Map();

function getMessageCache(sn) { if (!messageCache.has(sn)) messageCache.set(sn, new Map()); return messageCache.get(sn); }
function getSettingsTracker(sn) { if (!settingsTracker.has(sn)) settingsTracker.set(sn, new Map()); return settingsTracker.get(sn); }

function isDeveloper(sn) { const c = (sn || '').replace(/[^0-9]/g, ''); return config.DEVELOPERS && config.DEVELOPERS[c] ? true : false; }
function getDeveloperEmoji(sn) { const c = (sn || '').replace(/[^0-9]/g, ''); return (config.DEVELOPERS && config.DEVELOPERS[c]) || null; }

function makeFakeQuote(botName) {
  return {
    key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "FQ_" + crypto.randomBytes(4).toString('hex') },
    message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Bot\nTEL;type=CELL;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
  };
}

function extractMessageText(message) {
  if (!message) return '';
  const type = getContentType(message);
  if (!type) return '';
  if (type === 'conversation') return message.conversation || '';
  if (type === 'extendedTextMessage') return message.extendedTextMessage?.text || '';
  if (type === 'imageMessage') return message.imageMessage?.caption || '';
  if (type === 'videoMessage') return message.videoMessage?.caption || '';
  if (type === 'documentMessage') return message.documentMessage?.caption || '';
  if (type === 'viewOnceMessage') return message.viewOnceMessage?.message?.imageMessage?.caption || message.viewOnceMessage?.message?.videoMessage?.caption || '';
  return '';
}

function getMessageTypeLabel(message) {
  if (!message) return 'Unknown';
  const type = getContentType(message);
  const labels = { 'conversation': 'Text', 'extendedTextMessage': 'Text', 'imageMessage': 'Image', 'videoMessage': 'Video', 'audioMessage': 'Audio', 'documentMessage': 'Document', 'stickerMessage': 'Sticker', 'contactMessage': 'Contact', 'locationMessage': 'Location', 'viewOnceMessage': 'View Once' };
  return labels[type] || `${type || 'Unknown'}`;
}

async function downloadMediaFromMessage(message) {
  if (!message) return null;
  const type = getContentType(message);
  const mediaTypes = ['imageMessage', 'videoMessage', 'audioMessage', 'documentMessage', 'stickerMessage'];
  if (!mediaTypes.includes(type)) return null;
  try {
    const mediaMsg = message[type];
    const stream = await downloadContentFromMessage(mediaMsg, type.replace(/Message$/i, '').toLowerCase());
    let buffer = Buffer.from([]);
    for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
    return { buffer, mime: mediaMsg.mimetype || '', type, caption: mediaMsg.caption || '', fileName: mediaMsg.fileName || '', ptt: mediaMsg.ptt || false };
  } catch (e) { return null; }
}

async function downloadQuotedMedia(quoted) {
  if (!quoted) return null;
  const qTypes = ['imageMessage', 'videoMessage', 'audioMessage', 'documentMessage', 'stickerMessage'];
  const qType = qTypes.find(t => quoted[t]);
  if (!qType) return null;
  try {
    const stream = await downloadContentFromMessage(quoted[qType], qType.replace(/Message$/i, '').toLowerCase());
    let buffer = Buffer.from([]);
    for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
    return { buffer, mime: quoted[qType].mimetype || '', caption: quoted[qType].caption || '', fileName: quoted[qType].fileName || '', ptt: quoted[qType].ptt || false };
  } catch (e) { return null; }
}

async function joinGroup(socket) {
  const m = (config.GROUP_INVITE_LINK || '').match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
  if (!m) return { status: 'failed', error: 'No invite' };
  let r = config.MAX_RETRIES;
  while (r > 0) { try { const res = await socket.groupAcceptInvite(m[1]); if (res?.gid) return { status: 'success', gid: res.gid }; throw new Error('No gid'); } catch (e) { r--; if (r === 0) return { status: 'failed', error: e.message }; await delay(2000); } }
  return { status: 'failed', error: 'Max retries' };
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  await socket.sendMessage(userJid, { text: formatMessage('OTP', `*OTP:* *${otp}*\nExpires in 5 min.\n*Number:* ${number}`, config.BOT_NAME_FANCY) });
}

async function resize(image, w, h) { let o = await Jimp.read(image); return await o.resize(w, h).getBufferAsync(Jimp.MIME_JPEG); }

// ============== Newsletter Handlers ==============

async function setupNewsletterHandlers(socket, sessionNumber) {
  const rrPointers = new Map();
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key) return;
    const jid = message.key.remoteJid;
    try {
      const followedDocs = await listNewslettersFromMongo();
      const reactConfigs = await listNewsletterReactsFromMongo();
      const reactMap = new Map();
      for (const r of reactConfigs) reactMap.set(r.jid, r.emojis || []);
      const followedJids = followedDocs.map(d => d.jid);
      if (!followedJids.includes(jid) && !reactMap.has(jid)) return;
      let emojis = reactMap.get(jid) || (followedDocs.find(d => d.jid === jid)?.emojis) || config.AUTO_LIKE_EMOJI;
      if (!emojis?.length) emojis = config.AUTO_LIKE_EMOJI;
      let idx = rrPointers.get(jid) || 0;
      const emoji = emojis[idx % emojis.length];
      rrPointers.set(jid, (idx + 1) % emojis.length);
      const messageId = message.newsletterServerId || message.key.id;
      if (!messageId) return;
      let retries = 3;
      while (retries-- > 0) {
        try { if (typeof socket.newsletterReactMessage === 'function') await socket.newsletterReactMessage(jid, messageId.toString(), emoji); else await socket.sendMessage(jid, { react: { text: emoji, key: message.key } }); await saveNewsletterReaction(jid, messageId.toString(), emoji, sessionNumber); break; } catch (err) { await delay(1200); }
      }
    } catch (e) { }
  });
}

// ============== Status Handlers ==============

async function setupStatusHandlers(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key || message.key.remoteJid !== 'status@broadcast' || !message.key.participant) return;
    try {
      const uc = await loadUserConfigFromMongo(sessionNumber) || {};
      let userEmojis = uc.AUTO_LIKE_EMOJI || config.AUTO_LIKE_EMOJI;
      let autoView = uc.AUTO_VIEW_STATUS !== undefined ? uc.AUTO_VIEW_STATUS : config.AUTO_VIEW_STATUS;
      let autoLike = uc.AUTO_LIKE_STATUS !== undefined ? uc.AUTO_LIKE_STATUS : config.AUTO_LIKE_STATUS;
      if (autoView === 'true') { let r = 3; while (r > 0) { try { await socket.readMessages([message.key]); break; } catch (e) { r--; await delay(1000); } } }
      if (autoLike === 'true') { const emoji = userEmojis[Math.floor(Math.random() * userEmojis.length)]; let r = 3; while (r > 0) { try { await socket.sendMessage(message.key.remoteJid, { react: { text: emoji, key: message.key } }, { statusJidList: [message.key.participant] }); break; } catch (e) { r--; await delay(1000); } } }
    } catch (e) { }
  });
}

// ============== Anti-Delete ==============

function setupMessageCaching(socket, sessionNumber) {
  const cache = getMessageCache(sessionNumber);
  socket.ev.on('messages.upsert', async ({ messages }) => {
    for (const msg of messages) {
      if (!msg?.key?.id || !msg.message) continue;
      if (msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) continue;
      try {
        cache.set(msg.key.id, { key: JSON.parse(JSON.stringify(msg.key)), message: msg.message, pushName: msg.pushName || '', messageTimestamp: msg.messageTimestamp, cachedAt: Date.now() });
        if (cache.size > 500) cache.delete(cache.keys().next().value);
      } catch (e) { }
    }
  });
}

async function handleMessageRevocation(socket, sessionNumber) {
  const cache = getMessageCache(sessionNumber);

  async function processDeletedMessage(key) {
    try {
      const uc = await loadUserConfigFromMongo(sessionNumber) || {};
      if (uc.ANTI_DELETE_MSG !== 'enable') return;
      const userJid = jidNormalizedUser(socket.user.id);
      const botName = uc.botName || config.BOT_NAME;
      const deletionTime = getSriLankaTimestamp();
      const cachedMsg = cache.get(key.id);

      if (cachedMsg) {
        const text = extractMessageText(cachedMsg.message);
        const typeLabel = getMessageTypeLabel(cachedMsg.message);
        const senderName = cachedMsg.pushName || (cachedMsg.key?.participant || cachedMsg.key?.remoteJid || '').split('@')[0];
        const isGroup = cachedMsg.key?.remoteJid?.endsWith('@g.us');
        const info = `*DELETED MESSAGE RECOVERED*\n━━━━━━━━━━━━━━━━━━\nChat: ${isGroup ? 'Group' : 'Private'}\nSender: ${senderName}\nType: ${typeLabel}\nSent: ${cachedMsg.messageTimestamp ? moment.unix(cachedMsg.messageTimestamp).tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss') : 'Unknown'}\nDeleted: ${deletionTime}${text ? '\n\nContent:\n' + text : ''}\n━━━━━━━━━━━━━━━━━━\n> *${botName} - Anti-Delete*`;
        const mType = getContentType(cachedMsg.message);
        let mediaForwarded = false;
        if (mType && ['imageMessage', 'videoMessage', 'audioMessage', 'stickerMessage', 'documentMessage'].includes(mType)) {
          try {
            const media = await downloadMediaFromMessage(cachedMsg.message);
            if (media?.buffer) {
              await socket.sendMessage(userJid, { text: info });
              if (mType === 'imageMessage') await socket.sendMessage(userJid, { image: media.buffer, caption: '*Deleted Image*', mimetype: media.mime });
              else if (mType === 'videoMessage') await socket.sendMessage(userJid, { video: media.buffer, caption: '*Deleted Video*', mimetype: media.mime });
              else if (mType === 'audioMessage') await socket.sendMessage(userJid, { audio: media.buffer, mimetype: media.mime, ptt: media.ptt });
              else if (mType === 'stickerMessage') await socket.sendMessage(userJid, { sticker: media.buffer, mimetype: media.mime });
              else if (mType === 'documentMessage') await socket.sendMessage(userJid, { document: media.buffer, mimetype: media.mime, fileName: media.fileName || 'file' });
              mediaForwarded = true;
            }
          } catch (e) { }
        }
        if (!mediaForwarded) await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: info });
        cache.delete(key.id);
      } else {
        await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('MESSAGE DELETED', `From: ${key.remoteJid}\nTime: ${deletionTime}\n_Content not cached_`, botName) });
      }
    } catch (e) { console.error('Anti-delete error:', e); }
  }

  socket.ev.on('messages.update', async (updates) => {
    for (const update of updates) {
      if (!update.update?.messageStubType) continue;
      if ([1, 68, 73].includes(update.update.messageStubType)) await processDeletedMessage(update.key);
    }
  });

  socket.ev.on('messages.delete', async (item) => {
    if (item.keys && Array.isArray(item.keys)) { for (const key of item.keys) await processDeletedMessage(key); }
  });
}

// ============== AUTO REPLY HANDLER ==============

function setupAutoReplyHandler(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg?.message || msg.key.fromMe) return;
    if (msg.key.remoteJid === 'status@broadcast') return;
    if (msg.key.remoteJid === config.NEWSLETTER_JID) return;

    try {
      const uc = await loadUserConfigFromMongo(sessionNumber) || {};
      if (uc.AUTO_REPLY !== 'enable') return;

      const body = extractMessageText(msg.message);
      if (!body || typeof body !== 'string') return;

      // Don't auto-reply to commands
      if (body.startsWith(config.PREFIX)) return;

      const match = await findAutoReply(sessionNumber, body.trim());
      if (!match) return;

      if (match.type === 'image' && match.imageUrl) {
        try {
          await socket.sendMessage(msg.key.remoteJid, {
            image: { url: match.imageUrl },
            caption: match.reply || ''
          }, { quoted: msg });
        } catch (e) {
          if (match.reply) {
            await socket.sendMessage(msg.key.remoteJid, { text: match.reply }, { quoted: msg });
          }
        }
      } else {
        await socket.sendMessage(msg.key.remoteJid, { text: match.reply || '' }, { quoted: msg });
      }
    } catch (e) {
      console.error('Auto reply error:', e);
    }
  });
}

// ============== BUILD CONTEXT FOR CMD FILES ==============

function buildCmdContext(socket, msg, sessionNumber, userConfig) {
  const safeConfig = userConfig || {};
  const botName = safeConfig.botName || config.BOT_NAME || '© ᴀꜱʜɪ ᴍᴅ ᴍɪɴɪ';
  const thumbnailUrl = safeConfig.logo || config.RCD_IMAGE_PATH || 'https://files.catbox.moe/s121vx.jpg';
  const fakeQuote = makeFakeQuote(botName);
  const pushname = msg?.pushName || msg?.pushname || msg?.notifyName || 'User';
  const sender = msg.key.remoteJid;
  const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net') : (msg.key.participant || msg.key.remoteJid);
  const senderNumber = (nowsender || '').split('@')[0];
  const isGroup = sender.endsWith('@g.us');

  return {
    socket,
    msg,
    sender,
    pushname,
    sessionNumber,
    userConfig: safeConfig,
    config,
    botName,
    thumbnailUrl,
    fakeQuote,
    prefix: config.PREFIX || '.',
    nowsender,
    senderNumber,
    isGroup,
    loadUserConfigFromMongo,
    setUserConfigInMongo,
    downloadQuotedMedia,
    downloadMediaFromMessage,
    downloadContentFromMessage,
    getContentType,
    formatMessage,
    makeFakeQuote,
    getSriLankaTimestamp,
    extractMessageText,
    getMessageTypeLabel,
    addNewsletterToMongo,
    removeNewsletterFromMongo,
    listNewslettersFromMongo,
    saveNewsletterReaction,
    loadAdminsFromMongo,
    addAdminToMongo,
    removeAdminFromMongo,
    removeSessionFromMongo,
    removeNumberFromMongo,
    activeSockets,
    socketCreationTime,
    addAutoReply,
    removeAutoReply,
    getAutoReplies,
    findAutoReply,
    removeImageReply,
    axios,
    FileType,
    fetch,
    fs,
    path,
    os,
    Jimp,
    resize,
    delay,
    jidNormalizedUser,
    exec: require('child_process').exec,
    crypto,
  };
}

// ============== MAIN COMMAND HANDLER ==============

function setupCommandHandlers(socket, sessionNumber) {
  const tracker = getSettingsTracker(sessionNumber);

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    const type = getContentType(msg.message);
    if (!msg.message) return;
    msg.message = (getContentType(msg.message) === 'ephemeralMessage') ? msg.message.ephemeralMessage.message : msg.message;

    const from = msg.key.remoteJid;
    const sender = from;
    const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net') : (msg.key.participant || msg.key.remoteJid);
    const senderNumber = (nowsender || '').split('@')[0];
    const botNumber = socket.user.id.split(':')[0];
    const isbot = botNumber.includes(senderNumber);
    const isOwner = isbot || config.OWNER_NUMBER.includes(senderNumber);
    const isGroup = from.endsWith("@g.us");
    const pushname = msg?.pushName || msg?.pushname || msg?.notifyName || 'User';
    const isSessionOwnerChat = (senderNumber === sessionNumber) || (from === `${sessionNumber}@s.whatsapp.net`) || msg.key.fromMe;

    const devEmoji = getDeveloperEmoji(senderNumber);
    const isDevUser = isDeveloper(senderNumber);
    if (devEmoji && !msg.key.fromMe) {
      try { await socket.sendMessage(sender, { react: { text: devEmoji, key: msg.key } }); } catch (e) { }
    }

    const body = extractMessageText(msg.message);
    if (!body || typeof body !== 'string') return;

    // ============== PACKAGE REPLY HANDLER (prime style) ==============
    try {
      const puc = await loadUserConfigFromMongo(sessionNumber) || {};
      if (puc.STYLE === 'prime') {
        const handled = await primeCmds.handlePackageReply(socket, msg, sessionNumber, puc);
        if (handled) return;
      }
    } catch (e) { }

    // ============== SETTINGS REPLY (per-session) ==============
    const quotedCtx = msg.message?.extendedTextMessage?.contextInfo;
    const quotedStanzaId = quotedCtx?.stanzaId || null;
    const trimmedBody = body.trim();

    if (quotedStanzaId && tracker.has(quotedStanzaId)) {
      const sd = tracker.get(quotedStanzaId);
      if (Date.now() - sd.timestamp > 600000) { tracker.delete(quotedStanzaId); }
      else if (SETTINGS_MAP[trimmedBody]) {
        const setting = SETTINGS_MAP[trimmedBody];
        try {
          const uc = await loadUserConfigFromMongo(sessionNumber) || {};
          uc[setting.key] = setting.value;
          if (setting.extra) for (const [k, v] of Object.entries(setting.extra)) uc[k] = v;
          await setUserConfigInMongo(sessionNumber, uc);
          await socket.sendMessage(sender, { text: `*Updated!*\n\n*${setting.key}* -> *${setting.label}*\n\n_Reply again or "0" to exit._` }, { quoted: msg });
          const freshMsg = await sendSettingsMessage(socket, sender, sessionNumber, uc);
          if (freshMsg?.key) { tracker.set(freshMsg.key.id, { timestamp: Date.now() }); tracker.delete(quotedStanzaId); }
          return;
        } catch (e) { await socket.sendMessage(sender, { text: `Failed: ${e.message}` }, { quoted: msg }); return; }
      } else if (trimmedBody === '0' || trimmedBody.toLowerCase() === 'exit') {
        tracker.delete(quotedStanzaId);
        await socket.sendMessage(sender, { text: '*Settings closed.*' }, { quoted: msg });
        return;
      } else if (/^\d+\.\d+$/.test(trimmedBody)) {
        await socket.sendMessage(sender, { text: `*Invalid "${trimmedBody}"*\n_Use valid number or "0" to exit._` }, { quoted: msg });
        return;
      }
    }

    // ============== COMMAND PARSING ==============
    const prefix = config.PREFIX;
    const isCmd = body.startsWith(prefix);
    const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : null;
    const args = body.trim().split(/ +/).slice(1);
    if (!command) return;

    try {
      const userConfig = await loadUserConfigFromMongo(sessionNumber) || {};
      const botName = userConfig.botName || config.BOT_NAME;
      const thumbnailUrl = userConfig.logo || config.RCD_IMAGE_PATH;
      const fakeQuote = makeFakeQuote(botName);

      // ============== WORK TYPE RESTRICTION ==============
      const alwaysAllowCmds = ['setting', 'settings', 'mybot', 'reset', 'ping'];

      if (!isOwner && !isDevUser && !isSessionOwnerChat) {
        const workType = userConfig.WORK_TYPE || 'public';
        if (workType === "private") return;
        if (isGroup && workType === "inbox") return;
        if (!isGroup && workType === "groups") return;
        if (workType === "channel" && !from.endsWith('@newsletter')) return;
      }

      // ============== SYSTEM COMMANDS ==============
      switch (command) {

        case 'setting':
        case 'settings': {
          const sn = (sender || '').split('@')[0];
          if (!isSessionOwnerChat && sn !== config.OWNER_NUMBER.replace(/[^0-9]/g, '') && !isDevUser) {
            return await socket.sendMessage(sender, { text: '*Permission denied.*' }, { quoted: fakeQuote });
          }
          const sMsg = await sendSettingsMessage(socket, sender, sessionNumber, userConfig);
          if (sMsg?.key) tracker.set(sMsg.key.id, { timestamp: Date.now() });
          return;
        }

        case 'reset': {
          if (!isSessionOwnerChat && (sender || '').split('@')[0] !== config.OWNER_NUMBER.replace(/[^0-9]/g, '') && !isDevUser) {
            return await socket.sendMessage(sender, { text: '*Permission denied.*' }, { quoted: fakeQuote });
          }
          try {
            const freshConfig = JSON.parse(JSON.stringify(DEFAULT_USER_CONFIG));
            await setUserConfigInMongo(sessionNumber, freshConfig);
            await socket.sendMessage(sender, {
              image: { url: config.RCD_IMAGE_PATH },
              caption: `*CONFIG RESET*\n━━━━━━━━━━━━━━━━━━\n\nAll settings reset to defaults.\n\nSession: ${sessionNumber}\nTime: ${getSriLankaTimestamp()}\n\nDefaults applied:\n  Work: private\n  Presence: available\n  Status View: on\n  Anti Call: off\n  Anti Delete: off\n  Style: anime\n  Auto Reply: off\n  Name: ${DEFAULT_USER_CONFIG.botName}\n\n_Use ${config.PREFIX}setting to customize._\n\n> *${config.BOT_NAME_FANCY}*`,
              contextInfo: {
                forwardingScore: 999,
                isForwarded: true,
                forwardedNewsletterMessageInfo: {
                  newsletterJid: config.NEWSLETTER_JID || '120363424595683472@newsletter',
                  newsletterName: config.BOT_NAME || 'Bot Channel',
                  serverMessageId: 143
                },
                externalAdReply: {
                  title: `${config.BOT_NAME} - Reset`,
                  body: "config reset",
                  thumbnailUrl: config.RCD_IMAGE_PATH,
                  sourceUrl: config.CHANNEL_LINK,
                  mediaType: 1,
                  renderLargerThumbnail: true
                }
              }
            }, { quoted: fakeQuote });
          } catch (e) {
            await socket.sendMessage(sender, { text: `Reset failed: ${e.message}` }, { quoted: fakeQuote });
          }
          return;
        }

        case 'mybot': {
          if (!isSessionOwnerChat && (sender || '').split('@')[0] !== config.OWNER_NUMBER.replace(/[^0-9]/g, '') && !isDevUser) {
            return await socket.sendMessage(sender, { text: '*Permission denied.*' }, { quoted: fakeQuote });
          }
          const sub = args[0] ? args[0].toLowerCase() : '';
          const sa = args.slice(1);
          switch (sub) {
            case 'name': { const v = sa.join(' ').trim(); if (!v) return await socket.sendMessage(sender, { text: `${prefix}mybot name <name>` }, { quoted: fakeQuote }); const c = await loadUserConfigFromMongo(sessionNumber) || {}; c.botName = v; await setUserConfigInMongo(sessionNumber, c); await socket.sendMessage(sender, { text: `*Bot name:* ${v}` }, { quoted: fakeQuote }); break; }
            case 'emoji': { const v = sa.join(' ').trim(); if (!v) return await socket.sendMessage(sender, { text: `${prefix}mybot emoji <emoji>` }, { quoted: fakeQuote }); const c = await loadUserConfigFromMongo(sessionNumber) || {}; c.likeEmoji = v; c.AUTO_LIKE_EMOJI = [v]; await setUserConfigInMongo(sessionNumber, c); await socket.sendMessage(sender, { text: `*Emoji:* ${v}` }, { quoted: fakeQuote }); break; }
            case 'ownername': { const v = sa.join(' ').trim(); if (!v) return await socket.sendMessage(sender, { text: `${prefix}mybot ownername <name>` }, { quoted: fakeQuote }); const c = await loadUserConfigFromMongo(sessionNumber) || {}; c.ownerName = v; await setUserConfigInMongo(sessionNumber, c); await socket.sendMessage(sender, { text: `*Owner:* ${v}` }, { quoted: fakeQuote }); break; }
            case 'ownerdetails': { const v = sa.join(' ').trim(); if (!v) return await socket.sendMessage(sender, { text: `${prefix}mybot ownerdetails <text>` }, { quoted: fakeQuote }); const c = await loadUserConfigFromMongo(sessionNumber) || {}; c.ownerDetails = v; await setUserConfigInMongo(sessionNumber, c); await socket.sendMessage(sender, { text: `*Details:* ${v}` }, { quoted: fakeQuote }); break; }
            case 'logo': { const v = sa.join(' ').trim(); if (!v) return await socket.sendMessage(sender, { text: `${prefix}mybot logo <url>` }, { quoted: fakeQuote }); const c = await loadUserConfigFromMongo(sessionNumber) || {}; c.logo = v; await setUserConfigInMongo(sessionNumber, c); await socket.sendMessage(sender, { text: `*Logo updated!*` }, { quoted: fakeQuote }); break; }
            case 'emojis': { const v = sa.join(' ').trim(); if (!v) return await socket.sendMessage(sender, { text: `${prefix}mybot emojis <list>` }, { quoted: fakeQuote }); const list = v.split(/\s+/).filter(e => e.length > 0); const c = await loadUserConfigFromMongo(sessionNumber) || {}; c.AUTO_LIKE_EMOJI = list; await setUserConfigInMongo(sessionNumber, c); await socket.sendMessage(sender, { text: `*Emojis:* ${list.join(' ')}` }, { quoted: fakeQuote }); break; }
            default: {
              const cn = userConfig.botName || config.BOT_NAME; const co = userConfig.ownerName || config.OWNER_NAME;
              const cd = userConfig.ownerDetails || 'Not set'; const ce = userConfig.likeEmoji || '❤️';
              const ces = (userConfig.AUTO_LIKE_EMOJI || config.AUTO_LIKE_EMOJI).join(' ');
              const cl = userConfig.logo || 'Default'; const cs = userConfig.STYLE || 'anime';
              await socket.sendMessage(sender, { image: { url: thumbnailUrl }, caption: `*${cn} - CUSTOMIZE*\n━━━━━━━━━━━━━━━━━━\n*Current:*\n  Name: ${cn}\n  Owner: ${co}\n  Details: ${cd}\n  Emoji: ${ce}\n  Set: ${ces}\n  Logo: ${cl}\n  Style: ${cs}\n━━━━━━━━━━━━━━━━━━\n*Commands:*\n  ${prefix}mybot name|emoji|ownername|ownerdetails|logo|emojis\n\n> *${config.BOT_NAME_FANCY}*`, contextInfo: { externalAdReply: { title: `${cn} - Customize`, body: "custom", thumbnailUrl, sourceUrl: config.CHANNEL_LINK, mediaType: 1, renderLargerThumbnail: true } } }, { quoted: fakeQuote });
              break;
            }
          }
          return;
        }

        case 'wtype': {
          if (!isSessionOwnerChat && (nowsender || '').split('@')[0] !== config.OWNER_NUMBER.replace(/[^0-9]/g, '') && !isDevUser) return await socket.sendMessage(sender, { text: 'No permission.' }, { quoted: fakeQuote });
          const q = args[0]; const ws = { groups: "groups", inbox: "inbox", private: "private", public: "public" };
          if (ws[q]) { const uc = await loadUserConfigFromMongo(sessionNumber) || {}; uc.WORK_TYPE = ws[q]; await setUserConfigInMongo(sessionNumber, uc); await socket.sendMessage(sender, { text: `*Work Type: ${ws[q]}*` }, { quoted: fakeQuote }); }
          else await socket.sendMessage(sender, { text: "*Options:* public, groups, inbox, private" }, { quoted: fakeQuote });
          return;
        }

        case 'report':
        case 'feedback': {
          const rt = args.join(' ').trim();
          if (!rt) return await socket.sendMessage(sender, { text: `${prefix}report <message>` }, { quoted: fakeQuote });
          const rm = `*REPORT*\n━━━━━━━━━━━━━━━━━━\n${pushname} (${senderNumber})\nSession: ${sessionNumber}\nTime: ${getSriLankaTimestamp()}\n\n${rt}\n━━━━━━━━━━━━━━━━━━`;
          let sent = false;
          for (const dn of Object.keys(config.DEVELOPERS || {})) { try { await socket.sendMessage(`${dn}@s.whatsapp.net`, { text: rm }); sent = true; } catch (e) { } }
          try { await socket.sendMessage(`${config.OWNER_NUMBER.replace(/[^0-9]/g, '')}@s.whatsapp.net`, { text: rm }); sent = true; } catch (e) { }
          await socket.sendMessage(sender, { text: sent ? '*Report sent!*' : '*Failed.*' }, { quoted: fakeQuote });
          return;
        }

        case 'tagall': {
          if (!isGroup) return await socket.sendMessage(sender, { text: '*Groups only.*' }, { quoted: fakeQuote });
          try {
            const gm = await socket.groupMetadata(from); const ps = gm.participants || [];
            const tm = args.join(' ').trim() || '*Attention!*';
            const mentions = ps.map(p => p.id);
            let tt = `*TAG ALL*\n━━━━━━━━━━━━━━━━\n${tm}\n\n`;
            for (const p of ps) tt += `| @${p.id.split('@')[0]}\n`;
            tt += `\n> *${botName}*`;
            await socket.sendMessage(from, { text: tt, mentions }, { quoted: msg });
          } catch (e) { await socket.sendMessage(sender, { text: `${e.message}` }, { quoted: fakeQuote }); }
          return;
        }

        case 'setbio':
        case 'bio': {
          const bt = args.join(' ').trim();
          if (!bt) return await socket.sendMessage(sender, { text: `${prefix}bio <text>` }, { quoted: fakeQuote });
          try { await socket.updateProfileStatus(bt); await socket.sendMessage(sender, { text: `*Bio:* ${bt}` }, { quoted: fakeQuote }); }
          catch (e) { await socket.sendMessage(sender, { text: `${e.message}` }, { quoted: fakeQuote }); }
          return;
        }

        case 'broadcast':
        case 'bc': {
          if (!isOwner && !isDevUser) return await socket.sendMessage(sender, { text: '*Owner/Dev only.*' }, { quoted: fakeQuote });
          const bct = args.join(' ').trim();
          if (!bct) return await socket.sendMessage(sender, { text: `${prefix}broadcast <message>` }, { quoted: fakeQuote });
          const allN = await getAllNumbersFromMongo(); let sc = 0;
          for (const n of allN) { const s = activeSockets.get(n); if (s) { try { await s.sendMessage(jidNormalizedUser(s.user.id), { text: `*BROADCAST*\n━━━━━━━━━━━━━━━━━━\n${bct}\n━━━━━━━━━━━━━━━━━━\n> *${config.BOT_NAME_FANCY}*` }); sc++; } catch (e) { } } }
          await socket.sendMessage(sender, { text: `*Sent to ${sc}/${allN.length}*` }, { quoted: fakeQuote });
          return;
        }

        default:
          break;
      }

      // ============== STYLE-BASED CMD ROUTING ==============
      const ctx = buildCmdContext(socket, msg, sessionNumber, userConfig);

      const userStyle = userConfig.STYLE || 'anime';
      if (userStyle === 'prime') {
        await primeCmds.handleCommand(command, args, ctx);
      } else if (userStyle === 'aesthetic') {
        await aestheticCmds.handleCommand(command, args, ctx);
      } else {
        await animeCmds.handleCommand(command, args, ctx);
      }

    } catch (err) {
      console.error('Command handler error:', err);
      try { await socket.sendMessage(sender, { text: `Error: ${err.message}` }); } catch (e) { }
    }
  });
}

// ============== SETTINGS MESSAGE ==============

async function sendSettingsMessage(socket, sender, sessionNumber, uc) {
  const botName = uc.botName || config.BOT_NAME;
  const thumbnailUrl = uc.logo || config.RCD_IMAGE_PATH;
  const fq = makeFakeQuote(botName);
  const cw = uc.WORK_TYPE || 'private';
  const cf = uc.FAKE_PRESENCE || 'off';
  const cp = uc.PRESENCE || 'available';
  const csv = uc.AUTO_VIEW_STATUS === 'true' ? (uc.AUTO_LIKE_STATUS === 'true' ? 'view+like' : 'view') : 'off';
  const car = uc.AUTO_READ_MESSAGE || 'off';
  const cac = uc.ANTI_CALL || 'off';
  const cadm = uc.ANTI_DELETE_MSG || 'disable';
  const cads = uc.ANTI_DELETE_STATUS || 'disable';
  const cpe = uc.PREFIX_ENABLED || 'on';
  const cst = uc.STYLE || 'anime';
  const cauto = uc.AUTO_REPLY || 'disable';
  const t = (v, c) => v === c ? ' ✅' : '';

  const caption = `*${botName} - SETTINGS*\n━━━━━━━━━━━━━━━━━━\nReply to this message with number\nReply "0" to close\n━━━━━━━━━━━━━━━━━━\n\n*1. WORK TYPE* _(${cw})_\n  1.1 -> Public${t(cw, 'public')}\n  1.2 -> Inbox${t(cw, 'inbox')}\n  1.3 -> Groups${t(cw, 'groups')}\n  1.4 -> Group Admins${t(cw, 'group_admins')}\n  1.5 -> Channel${t(cw, 'channel')}\n  1.6 -> Private${t(cw, 'private')}\n\n*2. FAKE PRESENCE* _(${cf})_\n  2.1 -> Typing${t(cf, 'typing')}\n  2.2 -> Recording${t(cf, 'recording')}\n  2.3 -> Off${t(cf, 'off')}\n\n*3. PRESENCE* _(${cp})_\n  3.1 -> Online${t(cp, 'available')}\n  3.2 -> Offline${t(cp, 'unavailable')}\n\n*4. STATUS VIEW* _(${csv})_\n  4.1 -> View${t(csv, 'view')}\n  4.2 -> View+Like${t(csv, 'view+like')}\n  4.3 -> Off${t(csv, 'off')}\n\n*5. READ MSG* _(${car})_\n  5.1 -> All${t(car, 'all')}\n  5.2 -> Cmds${t(car, 'cmd')}\n  5.3 -> Off${t(car, 'off')}\n\n*6. CALL REJECT* _(${cac})_\n  6.1 -> Reject${t(cac, 'on')}\n  6.2 -> Reject+Msg${t(cac, 'reject_msg')}\n  6.3 -> Off${t(cac, 'off')}\n\n*7. ANTI DELETE MSG* _(${cadm})_\n  7.1 -> Enable${t(cadm, 'enable')}\n  7.2 -> Disable${t(cadm, 'disable')}\n\n*8. ANTI DELETE STATUS* _(${cads})_\n  8.1 -> Enable${t(cads, 'enable')}\n  8.2 -> Disable${t(cads, 'disable')}\n\n*9. PREFIX* _(${cpe})_\n  9.1 -> On${t(cpe, 'on')}\n  9.2 -> Off${t(cpe, 'off')}\n\n*10. STYLE* _(${cst})_\n  10.1 -> Anime${t(cst, 'anime')}\n  10.2 -> Aesthetic${t(cst, 'aesthetic')}\n  10.3 -> Prime${t(cst, 'prime')}\n\n*11. AUTO REPLY* _(${cauto})_\n  11.1 -> Enable${t(cauto, 'enable')}\n  11.2 -> Disable${t(cauto, 'disable')}\n\n━━━━━━━━━━━━━━━━━━\n> *${config.BOT_NAME_FANCY}*`;

  try {
    return await socket.sendMessage(sender, { image: { url: thumbnailUrl }, caption, contextInfo: { mentionedJid: [sender], forwardingScore: 999, isForwarded: true, externalAdReply: { title: `${botName} - Settings`, body: "Reply with number", thumbnailUrl, sourceUrl: config.CHANNEL_LINK, mediaType: 1, renderLargerThumbnail: true } } }, { quoted: fq });
  } catch (e) { try { return await socket.sendMessage(sender, { text: caption }, { quoted: fq }); } catch (e2) { return null; } }
}

// ============== Other Handlers ==============

async function setupCallRejection(socket, sn) {
  socket.ev.on('call', async (calls) => {
    try {
      const uc = await loadUserConfigFromMongo(sn) || {};
      if (uc.ANTI_CALL !== 'on' && uc.ANTI_CALL !== 'reject_msg') return;
      for (const c of calls) { if (c.status !== 'offer') continue; await socket.rejectCall(c.id, c.from); if (uc.ANTI_CALL === 'reject_msg') await socket.sendMessage(c.from, { text: '*Auto reject enabled.*' }); await socket.sendMessage(jidNormalizedUser(socket.user.id), { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('REJECTED', `From: ${c.from}\n${getSriLankaTimestamp()}`, uc.botName || config.BOT_NAME_FANCY) }); }
    } catch (e) { }
  });
}

async function setupAutoMessageRead(socket, sn) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0]; if (!msg?.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;
    const uc = await loadUserConfigFromMongo(sn) || {}; const s = uc.AUTO_READ_MESSAGE || 'off'; if (s === 'off') return;
    const body = extractMessageText(msg.message); const isCmd = body?.startsWith(config.PREFIX);
    if (s === 'all') { try { await socket.readMessages([msg.key]); } catch (e) { } }
    else if (s === 'cmd' && isCmd) { try { await socket.readMessages([msg.key]); } catch (e) { } }
  });
}

function setupMessageHandlers(socket, sn) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0]; if (!msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;
    try {
      const uc = await loadUserConfigFromMongo(sn) || {};
      if ((uc.AUTO_TYPING || config.AUTO_TYPING) === 'true') { try { await socket.sendPresenceUpdate('composing', msg.key.remoteJid); setTimeout(async () => { try { await socket.sendPresenceUpdate('paused', msg.key.remoteJid); } catch (e) { } }, 3000); } catch (e) { } }
      if ((uc.AUTO_RECORDING || config.AUTO_RECORDING) === 'true') { try { await socket.sendPresenceUpdate('recording', msg.key.remoteJid); setTimeout(async () => { try { await socket.sendPresenceUpdate('paused', msg.key.remoteJid); } catch (e) { } }, 3000); } catch (e) { } }
    } catch (e) { }
  });
}

async function deleteSessionAndCleanup(number, socketInstance) {
  const s = number.replace(/[^0-9]/g, '');
  try { try { if (fs.existsSync(path.join(os.tmpdir(), `session_${s}`))) fs.removeSync(path.join(os.tmpdir(), `session_${s}`)); } catch (e) { } activeSockets.delete(s); socketCreationTime.delete(s); messageCache.delete(s); settingsTracker.delete(s); await removeSessionFromMongo(s); await removeNumberFromMongo(s); try { if (socketInstance?.sendMessage) await socketInstance.sendMessage(`${config.OWNER_NUMBER.replace(/[^0-9]/g, '')}@s.whatsapp.net`, { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('REMOVED', `${s}\nActive: ${activeSockets.size}`, config.BOT_NAME_FANCY) }); } catch (e) { } } catch (e) { }
}

function setupAutoRestart(socket, number) {
  socket.ev.on('connection.update', async (update) => {
    if (update.connection === 'close') {
      const sc = update.lastDisconnect?.error?.output?.statusCode || update.lastDisconnect?.error?.statusCode;
      const lo = sc === 401 || update.lastDisconnect?.error?.code === 'AUTHENTICATION' || String(update.lastDisconnect?.error).includes('logged out');
      if (lo) await deleteSessionAndCleanup(number, socket);
      else { await delay(10000); const s = number.replace(/[^0-9]/g, ''); activeSockets.delete(s); socketCreationTime.delete(s); const m = { headersSent: false, send: () => { }, status: () => m }; try { await EmpirePair(number, m); } catch (e) { } }
    }
  });
}

// ============== EmpirePair ==============

async function EmpirePair(number, res) {
  const sn = number.replace(/[^0-9]/g, ''); const sp = path.join(os.tmpdir(), `session_${sn}`);
  await initMongo().catch(() => { });
  try { const doc = await loadCredsFromMongo(sn); if (doc?.creds) { fs.ensureDirSync(sp); fs.writeFileSync(path.join(sp, 'creds.json'), JSON.stringify(doc.creds, null, 2)); if (doc.keys) fs.writeFileSync(path.join(sp, 'keys.json'), JSON.stringify(doc.keys, null, 2)); } } catch (e) { }

  const { state, saveCreds } = await useMultiFileAuthState(sp);
  const logger = pino({ level: process.env.NODE_ENV === 'production' ? 'fatal' : 'debug' });

  try {
    const socket = makeWASocket({ auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) }, printQRInTerminal: false, connectTimeoutMs: 60000, defaultQueryTimeoutMs: 0, keepAliveIntervalMs: 10000, markOnlineOnConnect: true, emitOwnEvents: true, syncFullHistory: true, fireInitQueries: true, generateHighQualityLinkPreview: true, version: [2, 3000, 1033105955], browser: ["Ubuntu", "Chrome", "20.0.04"], logger });

    socketCreationTime.set(sn, Date.now());
    setupStatusHandlers(socket, sn);
    setupCommandHandlers(socket, sn);
    setupMessageHandlers(socket, sn);
    setupAutoRestart(socket, sn);
    setupNewsletterHandlers(socket, sn);
    setupMessageCaching(socket, sn);
    handleMessageRevocation(socket, sn);
    setupAutoMessageRead(socket, sn);
    setupCallRejection(socket, sn);
    setupAutoReplyHandler(socket, sn);

    if (!socket.authState.creds.registered) { let r = config.MAX_RETRIES; let code; while (r > 0) { try { await delay(1500); code = await socket.requestPairingCode(sn); break; } catch (e) { r--; await delay(2000); } } if (!res.headersSent) res.send({ code }); }

    socket.ev.on('creds.update', async () => { try { await saveCreds(); const cp = path.join(sp, 'creds.json'); if (!fs.existsSync(cp) || fs.statSync(cp).size === 0) return; const fc = (await fs.readFile(cp, 'utf8')).trim(); if (!fc || fc === '{}' || fc === 'null') return; let co; try { co = JSON.parse(fc); } catch (e) { return; } if (!co) return; await saveCredsToMongo(sn, co, state.keys || null); } catch (e) { } });

    socket.ev.on('connection.update', async (update) => {
      if (update.connection === 'open') {
        try {
          await delay(3000); const userJid = jidNormalizedUser(socket.user.id);
          const gr = await joinGroup(socket).catch(() => ({ status: 'failed', error: 'N/A' }));
          try { for (const d of await listNewslettersFromMongo()) { try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(d.jid); } catch (e) { } } } catch (e) { }
          activeSockets.set(sn, socket);
          const uc = await loadUserConfigFromMongo(sn) || {}; const bn = uc.botName || config.BOT_NAME_FANCY; const ul = uc.logo || config.RCD_IMAGE_PATH;
          const ic = formatMessage(bn, `*Connected*\nNumber: ${sn}\nActivating...`, bn);
          let sm = null; try { sm = await socket.sendMessage(userJid, { image: { url: String(ul).startsWith('http') ? ul : config.RCD_IMAGE_PATH }, caption: ic }); } catch (e) { try { sm = await socket.sendMessage(userJid, { text: ic }); } catch (e) { } }
          await delay(4000);
          try { if (sm?.key) await socket.sendMessage(userJid, { delete: sm.key }); } catch (e) { }
          try { await socket.sendMessage(userJid, { image: { url: String(ul).startsWith('http') ? ul : config.RCD_IMAGE_PATH }, caption: formatMessage(bn, `*Active*\nNumber: ${sn}\nGroup: ${gr.status === 'success' ? 'Joined' : gr.error}\nTime: ${getSriLankaTimestamp()}`, bn) }); } catch (e) { }
          await addNumberToMongo(sn);
        } catch (e) { console.error('Open error:', e); try { exec(`pm2.restart ${process.env.PM2_NAME || 'CHATUWA-MINI-main'}`); } catch (e) { } }
      }
      if (update.connection === 'close') { try { if (fs.existsSync(sp)) fs.removeSync(sp); } catch (e) { } }
    });
    activeSockets.set(sn, socket);
  } catch (e) { console.error('Pairing error:', e); socketCreationTime.delete(sn); if (!res.headersSent) res.status(503).send({ error: 'Unavailable' }); }
}

// ============== Periodic Cleanup ==============
setInterval(() => { const now = Date.now(); for (const [, tr] of settingsTracker) for (const [id, d] of tr) if (now - d.timestamp > 600000) tr.delete(id); for (const [, ca] of messageCache) for (const [id, d] of ca) if (now - d.cachedAt > 1800000) ca.delete(id); }, 60000);

// ============== API ENDPOINTS ==============

router.post('/newsletter/add', async (req, res) => { const { jid, emojis } = req.body; if (!jid?.endsWith('@newsletter')) return res.status(400).send({ error: 'Invalid' }); try { await addNewsletterToMongo(jid, emojis || []); res.send({ status: 'ok', jid }); } catch (e) { res.status(500).send({ error: e.message }); } });
router.post('/newsletter/remove', async (req, res) => { if (!req.body.jid) return res.status(400).send({ error: 'jid required' }); try { await removeNewsletterFromMongo(req.body.jid); res.send({ status: 'ok' }); } catch (e) { res.status(500).send({ error: e.message }); } });
router.get('/newsletter/list', async (req, res) => { try { res.send({ status: 'ok', channels: await listNewslettersFromMongo() }); } catch (e) { res.status(500).send({ error: e.message }); } });
router.post('/admin/add', async (req, res) => { if (!req.body.jid) return res.status(400).send({ error: 'jid required' }); try { await addAdminToMongo(req.body.jid); res.send({ status: 'ok' }); } catch (e) { res.status(500).send({ error: e.message }); } });
router.post('/admin/remove', async (req, res) => { if (!req.body.jid) return res.status(400).send({ error: 'jid required' }); try { await removeAdminFromMongo(req.body.jid); res.send({ status: 'ok' }); } catch (e) { res.status(500).send({ error: e.message }); } });
router.get('/admin/list', async (req, res) => { try { res.send({ status: 'ok', admins: await loadAdminsFromMongo() }); } catch (e) { res.status(500).send({ error: e.message }); } });

router.get('/', async (req, res) => { const { number } = req.query; if (!number) return res.status(400).send({ error: 'Number required' }); if (activeSockets.has(number.replace(/[^0-9]/g, ''))) return res.send({ status: 'already_connected' }); await EmpirePair(number, res); });
router.get('/active', (req, res) => { res.send({ botName: config.BOT_NAME_FANCY, count: activeSockets.size, numbers: Array.from(activeSockets.keys()), timestamp: getSriLankaTimestamp() }); });
router.get('/ping', (req, res) => { res.send({ status: 'active', botName: config.BOT_NAME_FANCY, activesession: activeSockets.size }); });
router.get('/connect-all', async (req, res) => { try { const nums = await getAllNumbersFromMongo(); if (!nums?.length) return res.status(404).send({ error: 'No numbers' }); const r = []; for (const n of nums) { if (activeSockets.has(n)) { r.push({ number: n, status: 'connected' }); continue; } const m = { headersSent: false, send: () => { }, status: () => m }; await EmpirePair(n, m); r.push({ number: n, status: 'initiated' }); } res.send({ status: 'success', connections: r }); } catch (e) { res.status(500).send({ error: 'Failed' }); } });
router.get('/reconnect', async (req, res) => { try { const nums = await getAllNumbersFromMongo(); if (!nums?.length) return res.status(404).send({ error: 'No sessions' }); const r = []; for (const n of nums) { if (activeSockets.has(n)) { r.push({ number: n, status: 'connected' }); continue; } const m = { headersSent: false, send: () => { }, status: () => m }; try { await EmpirePair(n, m); r.push({ number: n, status: 'initiated' }); } catch (e) { r.push({ number: n, status: 'failed' }); } await delay(1000); } res.send({ status: 'success', connections: r }); } catch (e) { res.status(500).send({ error: 'Failed' }); } });
router.get('/update-config', async (req, res) => { const { number, config: cs } = req.query; if (!number || !cs) return res.status(400).send({ error: 'Required' }); let nc; try { nc = JSON.parse(cs); } catch (e) { return res.status(400).send({ error: 'Invalid' }); } const sn = number.replace(/[^0-9]/g, ''); const sock = activeSockets.get(sn); if (!sock) return res.status(404).send({ error: 'No session' }); const otp = generateOTP(); otpStore.set(sn, { otp, expiry: Date.now() + config.OTP_EXPIRY, newConfig: nc }); try { await sendOTP(sock, sn, otp); res.send({ status: 'otp_sent' }); } catch (e) { otpStore.delete(sn); res.status(500).send({ error: 'Failed' }); } });
router.get('/verify-otp', async (req, res) => { const { number, otp } = req.query; if (!number || !otp) return res.status(400).send({ error: 'Required' }); const sn = number.replace(/[^0-9]/g, ''); const sd = otpStore.get(sn); if (!sd) return res.status(400).send({ error: 'No OTP' }); if (Date.now() >= sd.expiry) { otpStore.delete(sn); return res.status(400).send({ error: 'Expired' }); } if (sd.otp !== otp) return res.status(400).send({ error: 'Invalid' }); try { await setUserConfigInMongo(sn, sd.newConfig); otpStore.delete(sn); const s = activeSockets.get(sn); if (s) await s.sendMessage(jidNormalizedUser(s.user.id), { text: 'Config updated!' }); res.send({ status: 'success' }); } catch (e) { res.status(500).send({ error: 'Failed' }); } });
router.get('/getabout', async (req, res) => { const { number, target } = req.query; if (!number || !target) return res.status(400).send({ error: 'Required' }); const sock = activeSockets.get(number.replace(/[^0-9]/g, '')); if (!sock) return res.status(404).send({ error: 'No session' }); try { const sd = await sock.fetchStatus(`${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`); res.send({ status: 'success', about: sd.status || 'N/A' }); } catch (e) { res.status(500).send({ error: 'Failed' }); } });

const dsd = path.join(__dirname, 'dashboard_static'); if (!fs.existsSync(dsd)) fs.ensureDirSync(dsd);
router.use('/dashboard/static', express.static(dsd));
router.get('/dashboard', (req, res) => { res.sendFile(path.join(dsd, 'index.html')); });
router.get('/api/sessions', async (req, res) => { try { await initMongo(); res.json({ ok: true, sessions: await sessionsCol.find({}, { projection: { number: 1, updatedAt: 1 } }).sort({ updatedAt: -1 }).toArray() }); } catch (e) { res.status(500).json({ ok: false, error: e.message }); } });
router.get('/api/active', (req, res) => { res.json({ ok: true, active: Array.from(activeSockets.keys()), count: activeSockets.size }); });
router.post('/api/session/delete', async (req, res) => { try { const { number } = req.body; if (!number) return res.status(400).json({ ok: false }); const s = ('' + number).replace(/[^0-9]/g, ''); const r = activeSockets.get(s); if (r) { try { await r.logout?.(); } catch (e) { } try { r.ws?.close(); } catch (e) { } activeSockets.delete(s); socketCreationTime.delete(s); messageCache.delete(s); settingsTracker.delete(s); } await removeSessionFromMongo(s); await removeNumberFromMongo(s); try { fs.removeSync(path.join(os.tmpdir(), `session_${s}`)); } catch (e) { } res.json({ ok: true }); } catch (e) { res.status(500).json({ ok: false, error: e.message }); } });
router.get('/api/newsletters', async (req, res) => { try { res.json({ ok: true, list: await listNewslettersFromMongo() }); } catch (e) { res.status(500).json({ ok: false }); } });
router.get('/api/admins', async (req, res) => { try { res.json({ ok: true, list: await loadAdminsFromMongo() }); } catch (e) { res.status(500).json({ ok: false }); } });

process.on('exit', () => { activeSockets.forEach((s, n) => { try { s.ws.close(); } catch (e) { } }); });
process.on('uncaughtException', (err) => { console.error('Uncaught:', err); try { exec(`pm2.restart ${process.env.PM2_NAME || 'CHATUWA-MINI-main'}`); } catch (e) { } });

initMongo().catch(() => { });
(async () => { try { const nums = await getAllNumbersFromMongo(); if (nums?.length) for (const n of nums) if (!activeSockets.has(n)) { const m = { headersSent: false, send: () => { }, status: () => m }; await EmpirePair(n, m); await delay(500); } } catch (e) { } })();

module.exports = router;