import express from 'express';
import fs from 'fs';
import http from 'http';
import https from 'https';
import path from 'path';
import { JSDOM } from 'jsdom';

const DATA_FOLDER = 'data';
const IMAGE_FOLDER = 'images';
const DATA_FILE_PATH = path.join(DATA_FOLDER, 'data.csv');
const WEB_PROXY_URL = process.env.WEB_PROXY_URL || '';
const DEFAULT_TIMER_DELAY = Number(process.env.TIMER_DELAY_MS || 0);

if (!fs.existsSync(DATA_FOLDER)) {
  fs.mkdirSync(DATA_FOLDER, { recursive: true });
}

if (!fs.existsSync(IMAGE_FOLDER)) {
  fs.mkdirSync(IMAGE_FOLDER, { recursive: true });
}

let cachedInstructions = null;

async function loadInstructions() {
  if (cachedInstructions) {
    return cachedInstructions;
  }

  const rawContent = await fs.promises.readFile('list.json', 'utf-8');
  const instructions = JSON.parse(rawContent);

  if (!Array.isArray(instructions)) {
    throw new Error('list.json должен содержать массив объектов');
  }

  cachedInstructions = instructions;
  return instructions;
}

function getTimerDelay() {
  if (!Number.isFinite(DEFAULT_TIMER_DELAY) || DEFAULT_TIMER_DELAY <= 0) {
    return 0;
  }

  return DEFAULT_TIMER_DELAY;
}

async function processQueue() {
  const instructions = await loadInstructions();
  const delayAfterLoad = getTimerDelay();

  for (const instruction of instructions) {
    await handleScrapeInstructions(instruction, delayAfterLoad);
  }

  console.log('Все задания из list.json обработаны');
}

async function handleScrapeInstructions(instruction, delayAfterLoad) {
  try {
    const result = await openAndScrape(instruction, delayAfterLoad);
    console.log('✅ Scraping completed:', result?.id ?? 'без id');
  } catch (err) {
    console.error('❌ Scraping error:', err);
    throw err;
  }
}

async function openAndScrape(item, delayAfterLoad = 0) {
  const flags = Array.isArray(item.flags) ? item.flags : [];

  const rawUrl = typeof item.url === 'string' ? item.url : '';
  const decoded = rawUrl ? decodeURIComponent(rawUrl) : rawUrl;
  const proxifiedUrl = WEB_PROXY_URL
    ? `${WEB_PROXY_URL}${encodeURIComponent(decoded)}`
    : decoded;

  const newItem = { ...item, proxifiedUrl };

  const response = await fetch(proxifiedUrl);
  if (!response.ok) {
    throw new Error(`Failed to load url: ${proxifiedUrl}. HTTP ${response.status}`);
  }

  const html = await response.text();
  console.log(html)
  const dom = new JSDOM(html, {
    url: response.url || proxifiedUrl,
    pretendToBeVisual: true,
  });

  const context = {
    document: dom.window.document,
    window: dom.window,
  };

  if (delayAfterLoad > 0) {
    await waitMs(delayAfterLoad);
  } else if (item.sleep) {
    const sleepDelay = Number(item.sleep);
    if (!Number.isNaN(sleepDelay) && sleepDelay > 0) {
      await waitMs(sleepDelay);
    }
  }

  const result = await contentScriptFunction(newItem, context, flags);

  if (!result) {
    throw new Error('Scraped data is null or undefined.');
  }

  await persistScrapedData(result);

  const waiter = Array.isArray(item.requests)
    ? item.requests.find((cmd) => cmd.type?.toLowerCase().trim() === 'waiter')
    : null;

  if (waiter && !isNaN(waiter.time)) {
    console.log(`Post-script wait for ${waiter.time} ms...`);
    await waitMs(waiter.time);
  }

  return result;
}

async function contentScriptFunction(item, context, flags) {
  const { document, window } = context;
  const safeFlags = Array.isArray(flags) ? flags : [];

  if (safeFlags.includes('remove-videos')) {
    document.querySelectorAll('video').forEach((video) => video.remove());
  } else if (safeFlags.includes('pause-videos')) {
    document.querySelectorAll('video').forEach((video) => video.pause());
  }

  if (safeFlags.includes('clear-local-storage')) {
    try {
      window.localStorage.clear();
    } catch (e) {
      console.warn('Could not clear localStorage:', e);
    }
  }

  if (safeFlags.includes('clear-session-storage')) {
    try {
      window.sessionStorage.clear();
    } catch (e) {
      console.warn('Could not clear sessionStorage:', e);
    }
  }

  if (safeFlags.includes('clear-cookies')) {
    try {
      document.cookie.split(';').forEach((cookie) => {
        const name = cookie.trim().split('=')[0];
        document.cookie = `${name}=;expires=${new Date(0).toUTCString()};path=/;`;
      });
    } catch (e) {
      console.warn('Could not clear cookies:', e);
    }
  }

  if (safeFlags.includes('disable-animation')) {
    try {
      const styleEl = document.createElement('style');
      styleEl.textContent = `
        *,
        *::before,
        *::after {
          animation: none !important;
          transition: none !important;
        }
        * {
          opacity: 1 !important;
          background-color: #FFF !important;
        }
      `;
      document.head.appendChild(styleEl);
    } catch (e) {
      console.warn('Could not disable animations/transparency:', e);
    }
  }

  if (safeFlags.includes('disable-indexed-db')) {
    try {
      Object.defineProperty(window, 'indexedDB', {
        get() {
          console.warn('indexedDB is disabled by script injection.');
          return undefined;
        },
        configurable: false,
      });
    } catch (e) {
      console.warn("Could not redefine 'indexedDB':", e);
    }
  }

  if (item.waitFor) {
    let maxChecks = 50;
    let found = false;
    while (maxChecks--) {
      if (document.querySelector(item.waitFor)) {
        found = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 200));
    }
    if (!found) {
      return { error: `Element ${item.waitFor} not found within time limit` };
    }
  }

  const requests = Array.isArray(item.requests) ? item.requests : [];
  const patentCommands = [];
  const extractionCommands = [];

  for (const cmd of requests) {
    const cmdType = cmd?.type?.toLowerCase().trim();
    if (!cmdType) {
      continue;
    }

    if (cmdType === 'waiter') {
      const waitTime = Number(cmd.time);
      if (!isNaN(waitTime) && waitTime > 0) {
        console.log(`Waiting for ${waitTime} ms...`);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      }
      continue;
    }

    if (cmdType === 'patent') {
      patentCommands.push(cmd);
      continue;
    }

    if (
      cmdType === 'attr' ||
      cmdType === 'tag' ||
      cmdType === 'html' ||
      cmdType === 'img'
    ) {
      extractionCommands.push(cmd);
      continue;
    }

    if (cmdType === 'click') {
      const els = queryElements(cmd.selector, document);
      els.forEach((el) => el.click());
      continue;
    }

    if (cmdType === 'fill') {
      const els = queryElements(cmd.selector, document);
      els.forEach((el) => {
        el.value = cmd.value;
        el.dispatchEvent(new window.Event('change', { bubbles: true }));
        el.dispatchEvent(new window.Event('input', { bubbles: true }));
      });
      continue;
    }
  }

  function extractAttributeName(cmd) {
    if (cmd.attribute && typeof cmd.attribute === 'string') {
      return cmd.attribute.trim();
    }

    if (typeof cmd.selector !== 'string') {
      return null;
    }

    const matches = [...cmd.selector.matchAll(/\[([^\]]+)\]/g)];
    if (matches.length === 0) {
      return null;
    }

    const lastMatch = matches[matches.length - 1][1];
    if (!lastMatch) {
      return null;
    }

    return lastMatch.split('=')[0].trim();
  }

  function cleanText(value) {
    if (typeof value !== 'string') {
      return value;
    }
    return value.replace(/\s+/g, ' ').trim();
  }

  function normalizeSelectorSegment(segment) {
    if (typeof segment !== 'string') {
      return segment;
    }

    return segment.replace(
      /(^|[^:])(\b[a-zA-Z][a-zA-Z0-9_-]*|\*)\((['"])(.*?)\3\)/g,
      (match, prefix, tagName, quote, text) =>
        `${prefix}${tagName}:has-text(${quote}${text}${quote})`
    );
  }

  function queryElements(selector, contextRoot = document) {
    if (typeof selector !== 'string' || !selector.trim()) {
      return [];
    }

    const segments = selector
      .split('>>')
      .map((part) => part.trim())
      .filter(Boolean);

    let contexts = [contextRoot];

    for (const rawSegment of segments) {
      const segment = rawSegment.trim();
      if (!segment) {
        return [];
      }

      const isNextSibling = segment.toLowerCase().startsWith('next:');
      const selectorBody = normalizeSelectorSegment(
        isNextSibling ? segment.slice('next:'.length).trim() : segment
      );

      const hasTextMatch = selectorBody.match(/:has-text\(("|')(.*?)(\1)\)/i);
      const textNeedle = hasTextMatch?.[2]?.toLowerCase();
      const baseSelector = hasTextMatch
        ? selectorBody.replace(hasTextMatch[0], '').trim() || '*'
        : selectorBody;
      const allowSiblingTextFallback =
        Boolean(textNeedle) && baseSelector.includes('+');

      const nextContexts = [];

      for (const ctx of contexts) {
        if (!ctx) {
          continue;
        }

        const searchRoot = isNextSibling
          ? ctx.nextElementSibling || null
          : ctx;

        if (!searchRoot) {
          continue;
        }

        let matched = [];
        try {
          if (searchRoot.matches && searchRoot.matches(baseSelector)) {
            matched.push(searchRoot);
          }

          if (!isNextSibling) {
            matched.push(...searchRoot.querySelectorAll(baseSelector));
          }
        } catch (error) {
          console.warn(`Invalid selector '${baseSelector}':`, error);
          continue;
        }

        if (textNeedle) {
          const hasText = (el) =>
            (el?.textContent || '').toLowerCase().includes(textNeedle);

          matched = matched.filter((node) => {
            if (allowSiblingTextFallback) {
              const sibling = node.previousElementSibling;
              return Boolean(sibling && hasText(sibling));
            }

            return hasText(node);
          });
        }

        nextContexts.push(...matched);
      }

      contexts = nextContexts;
      if (contexts.length === 0) {
        break;
      }
    }

    return contexts;
  }

  async function buildRecordFromElement(element, cmd) {
    if (!element || !cmd?.name) {
      return undefined;
    }

    const type = cmd.type?.toLowerCase().trim();
    if (type === 'attr') {
      const attrName = extractAttributeName(cmd);
      if (!attrName) {
        return undefined;
      }
      const attrValue = element.getAttribute(attrName);
      if (attrValue == null) {
        return undefined;
      }
      return cleanText(attrValue);
    }

    if (type === 'tag') {
      const textValue = element.textContent;
      if (textValue == null) {
        return undefined;
      }
      return cleanText(textValue);
    }

    if (type === 'html') {
      return element.outerHTML;
    }

    if (type === 'img') {
      const imageValue = await captureImageFromElement(element, cmd);
      return imageValue;
    }

    return undefined;
  }

  function normaliseUrl(rawUrl) {
    if (!rawUrl || typeof rawUrl !== 'string') {
      return null;
    }

    try {
      return new URL(rawUrl, document.location.href).href;
    } catch {
      return rawUrl;
    }
  }

  function arrayBufferToBase64(arrayBuffer) {
    if (!arrayBuffer) {
      return '';
    }

    const bytes = new Uint8Array(arrayBuffer);
    let binary = '';
    const chunkSize = 0x8000;
    for (let i = 0; i < bytes.length; i += chunkSize) {
      const chunk = bytes.subarray(i, i + chunkSize);
      binary += String.fromCharCode.apply(null, chunk);
    }
    return window.btoa(binary);
  }

  async function blobToPngDataUrl(blob) {
    if (!blob) {
      return null;
    }

    const supportOffscreen =
      typeof window.OffscreenCanvas !== 'undefined' &&
      typeof window.OffscreenCanvas.prototype.convertToBlob === 'function';

    if (supportOffscreen && typeof window.createImageBitmap === 'function') {
      try {
        const bitmap = await window.createImageBitmap(blob);
        const canvas = new window.OffscreenCanvas(bitmap.width || 1, bitmap.height || 1);
        const ctx = canvas.getContext('2d');
        ctx.drawImage(bitmap, 0, 0);
        const pngBlob = await canvas.convertToBlob({ type: 'image/png' });
        bitmap.close();
        const arrayBuffer = await pngBlob.arrayBuffer();
        const base64 = arrayBufferToBase64(arrayBuffer);
        return `data:image/png;base64,${base64}`;
      } catch (error) {
        console.warn('Failed to convert image using OffscreenCanvas:', error);
      }
    }

    return new Promise((resolve, reject) => {
      const objectUrl = window.URL.createObjectURL(blob);
      const img = document.createElement('img');

      img.onload = () => {
        try {
          const canvas = document.createElement('canvas');
          const width = img.naturalWidth || img.width || 1;
          const height = img.naturalHeight || img.height || 1;
          canvas.width = width;
          canvas.height = height;
          const ctx = canvas.getContext('2d');
          ctx.drawImage(img, 0, 0, width, height);
          const dataUrl = canvas.toDataURL('image/png');
          resolve(dataUrl);
        } catch (err) {
          reject(err);
        } finally {
          window.URL.revokeObjectURL(objectUrl);
        }
      };

      img.onerror = (err) => {
        window.URL.revokeObjectURL(objectUrl);
        reject(err);
      };

      img.src = objectUrl;
    }).catch((error) => {
      console.warn('Failed to convert image using fallback canvas:', error);
      return null;
    });
  }

  async function blobToOriginalDataUrl(blob) {
    if (!blob) {
      return null;
    }

    try {
      const arrayBuffer = await blob.arrayBuffer();
      const base64 = arrayBufferToBase64(arrayBuffer);
      const mimeType = blob.type || 'application/octet-stream';
      return `data:${mimeType};base64,${base64}`;
    } catch (error) {
      console.warn('Failed to convert image blob to original format:', error);
      return null;
    }
  }

  function inferExtensionFromContentType(contentType) {
    if (!contentType || typeof contentType !== 'string') {
      return null;
    }

    const mime = contentType.split(';')[0].trim().toLowerCase();
    const map = {
      'image/jpeg': 'jpg',
      'image/jpg': 'jpg',
      'image/png': 'png',
      'image/gif': 'gif',
      'image/webp': 'webp',
      'image/svg+xml': 'svg',
      'image/bmp': 'bmp',
      'image/x-icon': 'ico',
      'image/vnd.microsoft.icon': 'ico',
    };

    if (map[mime]) {
      return map[mime];
    }

    if (mime.startsWith('image/')) {
      return mime.split('/')[1];
    }

    return null;
  }

  function inferExtensionFromUrl(rawUrl) {
    if (!rawUrl || typeof rawUrl !== 'string') {
      return null;
    }

    const match = rawUrl.match(/\.([a-z0-9]+)(?:[?#].*)?$/i);
    if (match) {
      return match[1].toLowerCase();
    }

    return null;
  }

  async function captureImageFromElement(element, cmd) {
    if (!element) {
      return undefined;
    }

    const src =
      element.currentSrc || element.src || element.getAttribute('src') || '';
    const absoluteUrl = normaliseUrl(src);
    if (!absoluteUrl) {
      return undefined;
    }

    try {
      const response = await fetch(absoluteUrl, {
        mode: 'cors',
        credentials: 'omit',
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const blob = await response.blob();
      const pngDataUrl = await blobToPngDataUrl(blob);
      const originalDataUrl = await blobToOriginalDataUrl(blob);

      if (!pngDataUrl && !originalDataUrl) {
        return undefined;
      }

      const baseFileName =
        typeof item.id !== 'undefined' && item.id !== null
          ? String(item.id)
          : cmd.name || 'image';

      const imageName = cmd?.name || cmd?.type || 'image';
      const results = [];

      if (pngDataUrl) {
        results.push({
          type: 'img',
          name: imageName,
          dataUrl: pngDataUrl,
          fileName: baseFileName,
          extension: 'png',
          sourceUrl: absoluteUrl,
          contentType: 'image/png',
        });
      }

      if (originalDataUrl) {
        const responseContentType = response.headers.get('content-type');
        const mimeType = blob.type || responseContentType || 'application/octet-stream';
        const inferredExtension =
          inferExtensionFromContentType(mimeType) ||
          inferExtensionFromUrl(absoluteUrl) ||
          'img';

        results.push({
          type: 'img',
          name: imageName,
          dataUrl: originalDataUrl,
          fileName: baseFileName,
          extension: inferredExtension,
          sourceUrl: absoluteUrl,
          contentType: mimeType,
        });
      }

      if (results.length === 1) {
        return results[0];
      }

      return results;
    } catch (error) {
      console.warn('Failed to capture image:', error);
      return undefined;
    }
  }

  async function extractValuesFromContext(contextRoot, commands) {
    if (!commands.length) {
      return [];
    }

    if (typeof contextRoot.querySelectorAll !== 'function') {
      return [];
    }

    const collectedValues = new Map();
    let maxLength = 0;

    for (const extractCmd of commands) {
      if (!extractCmd.selector) {
        continue;
      }

      const elements = queryElements(extractCmd.selector, contextRoot);
      if (!elements || elements.length === 0) {
        continue;
      }

      const fieldName = extractCmd.name || extractCmd.type;
      const values = [];

      for (const element of elements) {
        const value = await buildRecordFromElement(element, extractCmd);
        if (value !== undefined) {
          values.push(value);
        }
      }

      if (values.length === 0) {
        continue;
      }

      collectedValues.set(fieldName, values);
      if (values.length > maxLength) {
        maxLength = values.length;
      }
    }

    if (maxLength === 0) {
      return [];
    }

    const records = [];
    for (let index = 0; index < maxLength; index += 1) {
      const record = {};

      for (const [fieldName, values] of collectedValues.entries()) {
        if (index < values.length) {
          record[fieldName] = values[index];
        }
      }

      if (Object.keys(record).length > 0) {
        records.push(record);
      }
    }

    return records;
  }

  const data = [];

  if (patentCommands.length > 0 && extractionCommands.length > 0) {
    for (const patentCmd of patentCommands) {
      if (!patentCmd.selector) {
        continue;
      }

      const patentElements = queryElements(patentCmd.selector, document);
      for (const patentEl of patentElements) {
        const records = await extractValuesFromContext(
          patentEl,
          extractionCommands
        );

        for (const record of records) {
          if (Object.keys(record).length > 0) {
            data.push(record);
          }
        }
      }
    }
  } else if (extractionCommands.length > 0) {
    const records = await extractValuesFromContext(document, extractionCommands);

    for (const record of records) {
      if (Object.keys(record).length > 0) {
        data.push(record);
      }
    }
  }

  return {
    id: item.id,
    timestamp: new Date().toISOString(),
    url: item.proxifiedUrl || item.url,
    data,
  };
}

function waitMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normaliseValue(value) {
  if (value == null) {
    return '';
  }

  if (typeof value === 'string') {
    return value.replace(/\s+/g, ' ').trim();
  }

  return JSON.stringify(value);
}

function parseCsvLine(line) {
  const cells = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      cells.push(current);
      current = '';
    } else {
      current += char;
    }
  }

  cells.push(current);

  return cells;
}

function loadCsvHeaders(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }

  const content = fs.readFileSync(filePath, 'utf-8');
  const [firstLine] = content.split(/\r?\n/, 1);

  if (!firstLine) {
    return [];
  }

  return parseCsvLine(firstLine);
}

function loadCsvRows(filePath) {
  if (!fs.existsSync(filePath)) {
    return [];
  }

  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split(/\r?\n/).filter((line) => line.length > 0);

  if (lines.length <= 1) {
    return [];
  }

  return lines.slice(1).map((line) => parseCsvLine(line));
}

function escapeCsvCell(value) {
  if (value == null) {
    return '';
  }

  const stringValue = String(value);
  const escaped = stringValue.replace(/"/g, '""');

  return /[",\n]/.test(escaped) ? `"${escaped}"` : escaped;
}

function writeCsvFile(filePath, headers, rows) {
  if (headers.length === 0) {
    return;
  }

  const headerLine = headers.map((header) => escapeCsvCell(header)).join(',');
  const rowLines = rows.map((row) => row.map((cell) => escapeCsvCell(cell)).join(','));
  const csvContent = [headerLine, ...rowLines].join('\n');

  fs.writeFileSync(filePath, `${csvContent}\n`);
}

function sanitizeFileNameSegment(segment) {
  if (!segment) {
    return '';
  }

  return segment.replace(/[\\/:*?"<>|\s]+/g, '_');
}

function ensureImageExtension(fileName, extension) {
  if (!extension) {
    return sanitizeFileNameSegment(fileName);
  }

  const sanitized = sanitizeFileNameSegment(fileName);
  if (new RegExp(`\\.${extension}$`, 'i').test(sanitized)) {
    return sanitized;
  }

  return `${sanitized}.${extension}`;
}

function determineImageExtension(contentType, sourceUrl) {
  if (contentType) {
    const mime = contentType.split(';')[0].trim().toLowerCase();
    const mimeMap = {
      'image/jpeg': 'jpg',
      'image/jpg': 'jpg',
      'image/png': 'png',
      'image/gif': 'gif',
      'image/webp': 'webp',
      'image/svg+xml': 'svg',
      'image/bmp': 'bmp',
      'image/x-icon': 'ico',
      'image/vnd.microsoft.icon': 'ico',
    };

    if (mimeMap[mime]) {
      return mimeMap[mime];
    }
  }

  if (sourceUrl) {
    const match = sourceUrl.match(/\.([a-z0-9]+)(?:[?#].*)?$/i);
    if (match) {
      return match[1].toLowerCase();
    }
  }

  return 'png';
}

function generateUniqueImagePath(baseName, extension) {
  const base = sanitizeFileNameSegment(baseName) || 'image';
  let candidate = ensureImageExtension(base, extension);
  let counter = 1;

  while (fs.existsSync(path.join(IMAGE_FOLDER, candidate))) {
    candidate = ensureImageExtension(`${base}-${counter}`, extension);
    counter += 1;
  }

  return path.join(IMAGE_FOLDER, candidate);
}

async function downloadImageBuffer(imageUrl) {
  if (!imageUrl) {
    return null;
  }

  if (typeof fetch === 'function') {
    try {
      const response = await fetch(imageUrl);

      if (!response.ok) {
        console.warn(`Failed to download image via fetch: HTTP ${response.status}`);
      } else {
        const arrayBuffer = await response.arrayBuffer();
        return {
          buffer: Buffer.from(arrayBuffer),
          contentType: response.headers.get('content-type'),
        };
      }
    } catch (error) {
      console.warn('Error downloading image via fetch:', error);
    }
  }

  try {
    const urlObject = new URL(imageUrl);
    const client = urlObject.protocol === 'https:' ? https : http;

    return await new Promise((resolve, reject) => {
      const request = client.get(urlObject, (response) => {
        const { statusCode, headers } = response;

        if (statusCode && statusCode >= 300 && statusCode < 400 && headers.location) {
          const redirectUrl = new URL(headers.location, urlObject);
          response.resume();
          downloadImageBuffer(redirectUrl.toString()).then(resolve).catch(reject);
          return;
        }

        if (statusCode !== 200) {
          response.resume();
          reject(new Error(`HTTP ${statusCode}`));
          return;
        }

        const chunks = [];
        response.on('data', (chunk) => chunks.push(chunk));
        response.on('end', () => {
          resolve({
            buffer: Buffer.concat(chunks),
            contentType: headers['content-type'],
          });
        });
      });

      request.on('error', reject);
    });
  } catch (error) {
    console.warn('Error downloading image via http/https:', error);
  }

  return null;
}

async function saveImageValue(value, baseId, entryIndex, fieldKey) {
  if (!value || typeof value !== 'object') {
    return null;
  }

  const typeHint = typeof value.type === 'string' ? value.type.trim().toLowerCase() : null;

  if (typeHint !== 'img') {
    return null;
  }

  const preferredName = (() => {
    const fromValue = typeof value.fileName === 'string' && value.fileName.trim()
      ? value.fileName.trim()
      : typeof value.name === 'string' && value.name.trim()
        ? value.name.trim()
        : null;

    if (fromValue) {
      return fromValue;
    }

    if (typeof baseId === 'string' || typeof baseId === 'number') {
      return String(baseId);
    }

    const keySegment = fieldKey ? `-${fieldKey}` : '';
    return `${entryIndex}${keySegment}`;
  })();

  const extensionPreference = typeof value.extension === 'string' && value.extension.trim()
    ? value.extension.trim().toLowerCase()
    : null;

  const dataUrl = typeof value.dataUrl === 'string' ? value.dataUrl : null;
  const directUrl = typeof value.sourceUrl === 'string' && value.sourceUrl.trim()
    ? value.sourceUrl.trim()
    : null;

  if (dataUrl && dataUrl.startsWith('data:')) {
    const match = dataUrl.match(/^data:([^;]+);base64,(.+)$/);
    if (!match) {
      return null;
    }

    const [, mimeType, base64Payload] = match;
    if (!base64Payload) {
      return null;
    }

    const buffer = Buffer.from(base64Payload, 'base64');
    const extension = extensionPreference || determineImageExtension(value.contentType || mimeType, value.sourceUrl);
    const filePath = generateUniqueImagePath(preferredName, extension);

    fs.writeFileSync(filePath, buffer);

    return path.relative('.', filePath);
  }

  if (directUrl) {
    const downloadResult = await downloadImageBuffer(directUrl);

    if (!downloadResult) {
      return null;
    }

    const { buffer, contentType } = downloadResult;
    const extension = extensionPreference || determineImageExtension(value.contentType || contentType, directUrl);
    const filePath = generateUniqueImagePath(preferredName, extension);

    fs.writeFileSync(filePath, buffer);

    return path.relative('.', filePath);
  }

  return null;
}

function isPlainObject(value) {
  return Object.prototype.toString.call(value) === '[object Object]';
}

function isImageDescriptor(value) {
  return (
    value &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    typeof value.type === 'string' &&
    value.type.trim().toLowerCase() === 'img'
  );
}

function isDirectImagePayload(value) {
  if (Array.isArray(value)) {
    return value.length > 0 && value.every((entry) => isDirectImagePayload(entry));
  }

  return isImageDescriptor(value);
}

async function processImageValue(value, baseId, entryIndex, fieldPath) {
  const savedPath = await saveImageValue(value, baseId, entryIndex, fieldPath);
  if (savedPath) {
    return savedPath;
  }

  if (Array.isArray(value)) {
    const results = [];
    for (let idx = 0; idx < value.length; idx += 1) {
      const nextPath = fieldPath ? `${fieldPath}-${idx}` : String(idx);
      results.push(await processImageValue(value[idx], baseId, entryIndex, nextPath));
    }
    return results;
  }

  if (value && typeof value === 'object' && isPlainObject(value)) {
    const entries = {};
    const keys = Object.keys(value);
    for (const key of keys) {
      const nextPath = fieldPath ? `${fieldPath}.${key}` : key;
      entries[key] = await processImageValue(value[key], baseId, entryIndex, nextPath);
    }
    return entries;
  }

  return value;
}

async function processEntryImages(entry, baseId, entryIndex) {
  return processImageValue(entry, baseId, entryIndex, '');
}

async function persistScrapedData(parsedData, originalPayload = parsedData) {
  const baseId = !Array.isArray(parsedData) && parsedData?.id
    ? parsedData.id
    : `data_${Date.now()}`;

  if (isDirectImagePayload(originalPayload)) {
    if (Array.isArray(parsedData)) {
      for (let index = 0; index < parsedData.length; index += 1) {
        await processImageValue(parsedData[index], baseId, index, '');
      }
    } else {
      await processImageValue(parsedData, baseId, 'image', '');
    }

    console.log('Image payload received. Skipped JSON and CSV persistence.');
    return { skippedPersistence: true };
  }

  let payloadArray = [];
  let baseMetadata = {};
  let dataToPersist = parsedData;

  if (Array.isArray(parsedData)) {
    const processedArray = [];
    for (let index = 0; index < parsedData.length; index += 1) {
      processedArray.push(await processEntryImages(parsedData[index], baseId, index));
    }
    dataToPersist = processedArray;

    payloadArray = processedArray.map((entry) => {
      if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
        return { ...entry };
      }

      return { value: normaliseValue(entry) };
    });
  } else if (parsedData && typeof parsedData === 'object') {
    const { data, ...rest } = parsedData;
    baseMetadata = await processEntryImages(rest, baseId, 'meta');

    if (Array.isArray(data)) {
      const processedDataArray = [];
      for (let index = 0; index < data.length; index += 1) {
        processedDataArray.push(await processEntryImages(data[index], baseId, index));
      }
      dataToPersist = { ...baseMetadata, data: processedDataArray };

      payloadArray = processedDataArray.map((entry) => {
        if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
          return { ...baseMetadata, ...entry };
        }

        return { ...baseMetadata, value: normaliseValue(entry) };
      });
    } else if (data !== undefined) {
      const processedDataValue = await processEntryImages(data, baseId, 'data');
      dataToPersist = { ...baseMetadata, data: processedDataValue };

      if (Object.keys(baseMetadata).length > 0) {
        payloadArray = [{ ...baseMetadata }];
      }
    } else {
      dataToPersist = { ...baseMetadata };
      if (Object.keys(baseMetadata).length > 0) {
        payloadArray = [{ ...baseMetadata }];
      }
    }
  }

  if (payloadArray.length === 0) {
    console.warn('No structured data received to persist.');
  } else {
    const existingHeaders = loadCsvHeaders(DATA_FILE_PATH);
    const existingRows = loadCsvRows(DATA_FILE_PATH);

    const headers = Array.isArray(existingHeaders) && existingHeaders.length > 0
      ? [...existingHeaders]
      : [];

    const headerSet = new Set(headers);

    payloadArray.forEach((entry) => {
      if (entry && typeof entry === 'object') {
        Object.keys(entry).forEach((key) => {
          if (!headerSet.has(key)) {
            headerSet.add(key);
            headers.push(key);
          }
        });
      }
    });

    const reconciledExistingRows = existingRows.map((row) => {
      const rowMap = {};

      (existingHeaders || []).forEach((header, index) => {
        rowMap[header] = row[index] ?? '';
      });

      return headers.map((header) => rowMap[header] ?? '');
    });

    const newRows = payloadArray.map((entry) => {
      return headers.map((header) => normaliseValue(entry?.[header]));
    });

    writeCsvFile(DATA_FILE_PATH, headers, [...reconciledExistingRows, ...newRows]);
  }

  const fileName = `${baseId}.json`;
  const filePath = path.join(DATA_FOLDER, fileName);

  fs.writeFileSync(filePath, JSON.stringify(dataToPersist, null, 2));

  console.log(`Saved data to ${fileName}`);

  return { fileName };
}

function startServer() {
  const app = express();

  app.use(express.json({ limit: '50mb' }));
  app.use(express.urlencoded({ limit: '50mb', extended: true }));

  app.post('/scrape_data', async (req, res) => {
    try {
      const { l_scraped_data: scrapedData } = req.body;

      if (!scrapedData) {
        return res.status(400).json({ error: 'Missing l_scraped_data field' });
      }

      const originalPayload = JSON.parse(scrapedData);
      const parsedData = JSON.parse(scrapedData);
      const result = await persistScrapedData(parsedData, originalPayload);

      return res.status(200).json({ success: true, ...result });
    } catch (error) {
      console.error('Error processing request:', error);
      return res.status(500).json({ error: error.toString() });
    }
  });

  const PORT = 3333;
  app.listen(PORT, () => {
    console.log(`Server listening on http://localhost:${PORT}`);
  });
}

const mode = process.argv[2];

if (mode === 'scrape') {
  processQueue().catch((error) => {
    console.error('Failed to run scrape queue:', error);
    process.exitCode = 1;
  });
} else {
  startServer();
}
