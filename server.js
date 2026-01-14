import fs from 'fs';
import path from 'path';
import { JSDOM } from 'jsdom';

const ROOT_DIR = process.cwd();
const LIST_PATH = path.join(ROOT_DIR, 'list.json');
const DATA_DIR = path.join(ROOT_DIR, 'data');
const IMAGES_DIR = path.join(ROOT_DIR, 'images');
const CSV_PATH = path.join(DATA_DIR, 'data.csv');

function ensureDirectories() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.mkdirSync(IMAGES_DIR, { recursive: true });
}

async function loadInstructions() {
  const raw = await fs.promises.readFile(LIST_PATH, 'utf-8');
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error('list.json должен содержать массив объектов');
  }
  return parsed;
}

function waitMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

function queryElements(selector, context) {
  if (typeof selector !== 'string' || !selector.trim()) {
    return [];
  }

  const segments = selector
    .split('>>')
    .map((part) => part.trim())
    .filter(Boolean);

  let contexts = [context];

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
    const allowSiblingTextFallback = Boolean(textNeedle) && baseSelector.includes('+');

    const nextContexts = [];

    for (const ctx of contexts) {
      if (!ctx) {
        continue;
      }

      const searchRoot = isNextSibling ? ctx.nextElementSibling || null : ctx;

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
        const hasText = (el) => (el?.textContent || '').toLowerCase().includes(textNeedle);

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

function normaliseUrl(rawUrl, baseUrl) {
  if (!rawUrl || typeof rawUrl !== 'string') {
    return null;
  }

  try {
    return new URL(rawUrl, baseUrl).href;
  } catch {
    return rawUrl;
  }
}

function inferExtensionFromContentType(contentType) {
  if (!contentType) {
    return null;
  }

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

  return mimeMap[mime] || null;
}

function inferExtensionFromUrl(url) {
  if (!url) {
    return null;
  }

  const match = url.match(/\.([a-z0-9]+)(?:[?#].*)?$/i);
  if (match) {
    return match[1].toLowerCase();
  }

  return null;
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

function generateUniqueImagePath(baseName, extension) {
  const base = sanitizeFileNameSegment(baseName) || 'image';
  let candidate = ensureImageExtension(base, extension);
  let counter = 1;

  while (fs.existsSync(path.join(IMAGES_DIR, candidate))) {
    candidate = ensureImageExtension(`${base}-${counter}`, extension);
    counter += 1;
  }

  return path.join(IMAGES_DIR, candidate);
}

async function downloadImageBuffer(imageUrl) {
  if (!imageUrl) {
    return null;
  }

  try {
    const response = await fetch(imageUrl, { redirect: 'follow' });

    if (!response.ok) {
      console.warn(`Failed to download image via fetch: HTTP ${response.status}`);
      return null;
    }

    const arrayBuffer = await response.arrayBuffer();
    return {
      buffer: Buffer.from(arrayBuffer),
      contentType: response.headers.get('content-type'),
    };
  } catch (error) {
    console.warn('Error downloading image via fetch:', error);
    return null;
  }
}

async function captureImageFromElement(element, cmd, baseUrl, itemId) {
  if (!element) {
    return undefined;
  }

  const src =
    element.currentSrc || element.src || element.getAttribute('src') || '';
  const absoluteUrl = normaliseUrl(src, baseUrl);
  if (!absoluteUrl) {
    return undefined;
  }

  const downloadResult = await downloadImageBuffer(absoluteUrl);
  if (!downloadResult) {
    return undefined;
  }

  const baseFileName =
    typeof itemId !== 'undefined' && itemId !== null
      ? String(itemId)
      : cmd.name || 'image';
  const imageName = cmd?.name || cmd?.type || 'image';
  const extension =
    inferExtensionFromContentType(downloadResult.contentType) ||
    inferExtensionFromUrl(absoluteUrl) ||
    'img';
  const filePath = generateUniqueImagePath(`${baseFileName}-${imageName}`, extension);

  fs.writeFileSync(filePath, downloadResult.buffer);

  return path.relative(ROOT_DIR, filePath);
}

async function buildRecordFromElement(element, cmd, baseUrl, itemId) {
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
    return await captureImageFromElement(element, cmd, baseUrl, itemId);
  }

  return undefined;
}

async function extractValuesFromContext(context, commands, baseUrl, itemId) {
  if (!commands.length) {
    return [];
  }

  if (typeof context.querySelectorAll !== 'function') {
    return [];
  }

  const collectedValues = new Map();
  let maxLength = 0;

  for (const extractCmd of commands) {
    if (!extractCmd.selector) {
      continue;
    }

    const elements = queryElements(extractCmd.selector, context);
    if (!elements || elements.length === 0) {
      continue;
    }

    const fieldName = extractCmd.name || extractCmd.type;
    const values = [];

    for (const element of elements) {
      const value = await buildRecordFromElement(element, extractCmd, baseUrl, itemId);
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

async function scrapeInstruction(instruction) {
  const response = await fetch(instruction.url, { redirect: 'follow' });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${instruction.url}`);
  }

  const html = await response.text();
  const dom = new JSDOM(html);
  const { document } = dom.window;

  const requests = Array.isArray(instruction.requests) ? instruction.requests : [];
  const patentCommands = [];
  const extractionCommands = [];

  for (const cmd of requests) {
    const cmdType = cmd?.type?.toLowerCase().trim();
    if (!cmdType) {
      continue;
    }

    if (cmdType === 'waiter') {
      const waitTime = Number(cmd.time);
      if (!Number.isNaN(waitTime) && waitTime > 0) {
        await waitMs(waitTime);
      }
      continue;
    }

    if (cmdType === 'patent') {
      patentCommands.push(cmd);
      continue;
    }

    if (cmdType === 'attr' || cmdType === 'tag' || cmdType === 'html' || cmdType === 'img') {
      extractionCommands.push(cmd);
    }
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
          extractionCommands,
          instruction.url,
          instruction.id
        );

        for (const record of records) {
          if (Object.keys(record).length > 0) {
            data.push(record);
          }
        }
      }
    }
  } else if (extractionCommands.length > 0) {
    const records = await extractValuesFromContext(
      document,
      extractionCommands,
      instruction.url,
      instruction.id
    );

    for (const record of records) {
      if (Object.keys(record).length > 0) {
        data.push(record);
      }
    }
  }

  return {
    id: instruction.id,
    timestamp: new Date().toISOString(),
    url: instruction.url,
    data,
  };
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

function buildCsvPayload(result) {
  const baseMetadata = {
    id: result.id,
    timestamp: result.timestamp,
    url: result.url,
  };

  if (Array.isArray(result.data) && result.data.length > 0) {
    return result.data.map((entry) => {
      if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
        return { ...baseMetadata, ...entry };
      }

      return { ...baseMetadata, value: normaliseValue(entry) };
    });
  }

  return [baseMetadata];
}

function persistCsvRows(payloadArray) {
  if (!payloadArray.length) {
    return;
  }

  const existingHeaders = loadCsvHeaders(CSV_PATH);
  const existingRows = loadCsvRows(CSV_PATH);

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

  writeCsvFile(CSV_PATH, headers, [...reconciledExistingRows, ...newRows]);
}

async function persistResult(result) {
  ensureDirectories();

  const payloadArray = buildCsvPayload(result);
  persistCsvRows(payloadArray);

  const fileName = `${result.id ?? `data_${Date.now()}`}.json`;
  const filePath = path.join(DATA_DIR, fileName);

  fs.writeFileSync(filePath, JSON.stringify(result, null, 2));
  console.log(`Saved data to ${fileName}`);
}

async function runScrape() {
  ensureDirectories();
  const instructions = await loadInstructions();

  for (const instruction of instructions) {
    try {
      const result = await scrapeInstruction(instruction);
      await persistResult(result);
    } catch (error) {
      console.error(`Ошибка при обработке ${instruction.url}:`, error);
    }
  }
}

const mode = process.argv[2] || 'scrape';

if (mode === 'scrape') {
  runScrape().catch((error) => {
    console.error('Ошибка запуска:', error);
    process.exitCode = 1;
  });
}
