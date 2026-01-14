import express from 'express';
import fs from 'fs';
import http from 'http';
import https from 'https';
import path from 'path';


let cachedInstructions = null;
let isProcessing = false;

async function loadInstructions() {
  if (cachedInstructions) {
    return cachedInstructions;
  }

  const url = chrome.runtime.getURL("list.json");
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Не удалось загрузить list.json: ${response.status}`);
  }

  const instructions = await response.json();
  if (!Array.isArray(instructions)) {
    throw new Error("list.json должен содержать массив объектов");
  }

  cachedInstructions = instructions;
  return instructions;
}

function getTimerDelay() {
  const rawValue = (timerInput?.value || "").trim().replace(",", ".");

  if (!rawValue) {
    return 0;
  }

  const numericValue = Number(rawValue);
  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return 0;
  }

  // Users usually type the delay in seconds. To avoid accidentally treating
  // large values (for example 5000) as seconds, values above one thousand are
  // assumed to be already in milliseconds.
  if (numericValue > 1000) {
    return numericValue;
  }

  return numericValue * 1000;
}

function sendInstructionToBackground(instruction, delayAfterLoad) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendMessage(
      {
        type: "START_SCRAPE",
        payload: { ...instruction, timerDelay: delayAfterLoad },
      },
      (response) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message));
          return;
        }

        if (!response) {
          reject(new Error("Нет ответа от background.js"));
          return;
        }

        if (response.status !== "success") {
          reject(new Error(response.message || "Задача завершилась с ошибкой"));
          return;
        }

        resolve(response);
      }
    );
  });
}

async function processQueue() {
  if (isProcessing) {
    alert("Очередь уже обрабатывается");
    return;
  }

  try {
    isProcessing = true;
    sendButton.disabled = true;

    const instructions = await loadInstructions();
    const delayAfterLoad = getTimerDelay();

    for (const instruction of instructions) {
      await sendInstructionToBackground(instruction, delayAfterLoad);
    }

    alert("Все задания из list.json обработаны");
  } catch (error) {
    console.error("Ошибка при обработке очереди:", error);
    alert(error.message || "Произошла ошибка при запуске задач");
  } finally {
    isProcessing = false;
    sendButton.disabled = false;
  }
}




/*******************************************************
 * handleScrapeInstructions: processes with concurrency=5
 *******************************************************/

async function handleScrapeInstructions(instruction) {
  try {
    const result = await openAndScrape(instruction);
    console.log("✅ Scraping completed:", result);
  } catch (err) {
    console.error("❌ Scraping error:", err);
    throw err; // so it reaches sendResponse({status: 'error', message: ...})
  }
}


/************************************************************
 * openAndScrape(item): Decides proxy usage, opens background
 * tab, injects contentScriptFunction. Waits for result.
 * Also includes a 15s timeout if "complete" isn't reached.
 ************************************************************/
function openAndScrape(item) {
  return new Promise((resolve, reject) => {
    const flags = Array.isArray(item.flags) ? item.flags : [];

    let proxifiedUrl = item.url;
    let decoded;
    decoded = decodeURIComponent(item.url);
    proxifiedUrl = WEB_PROXY_URL + encodeURIComponent(decoded);

    const newItem = { ...item, proxifiedUrl };

    chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
      if (chrome.runtime.lastError) {
        return reject(
          `Failed to retrieve the active tab: ${chrome.runtime.lastError.message}`
        );
      }

      const activeTab = Array.isArray(tabs) ? tabs[0] : null;

      if (!activeTab || !activeTab.id) {
        return reject("No active tab available to load scraping target");
      }

      const tabId = activeTab.id;

      chrome.tabs.update(tabId, { url: proxifiedUrl, active: true }, (tab) => {
        if (chrome.runtime.lastError) {
          return reject(
            `Failed to navigate active tab: ${chrome.runtime.lastError.message}`
          );
        }

        if (!tab) {
          return reject(`Failed to load url in active tab: ${proxifiedUrl}`);
        }

        let didLoad = false;

        const loadTimeout = setTimeout(() => {
          if (!didLoad) {
            console.warn("Page load timeout:", proxifiedUrl);
            chrome.tabs.onUpdated.removeListener(onUpdated);
            reject(`Page load timeout for: ${proxifiedUrl}`);
          }
        }, 100000);

        // This listener waits until tab finishes loading
        const onUpdated = async (updatedTabId, changeInfo) => {
          if (updatedTabId === tabId && changeInfo.status === "complete") {
            didLoad = true;
            chrome.tabs.onUpdated.removeListener(onUpdated);

            const timerDelay = Number(item.timerDelay);
            if (!Number.isNaN(timerDelay) && timerDelay > 0) {
              await waitMs(timerDelay);
            } else if (item.sleep) {
              const sleepDelay = Number(item.sleep);
              if (!Number.isNaN(sleepDelay) && sleepDelay > 0) {
                await waitMs(sleepDelay);
              }
            }

            try {
              const result = await Promise.race([
                new Promise((res, rej) => {
                  chrome.scripting.executeScript(
                    {
                      target: { tabId },
                      func: contentScriptFunction,
                      args: [newItem],
                    },
                    (responses) => {
                      if (chrome.runtime.lastError) {
                        return rej(chrome.runtime.lastError.message);
                      }
                      if (!responses || !responses[0] || responses[0].error) {
                        return rej(
                          responses?.[0]?.error || "No result from content script"
                        );
                      }
                      res(responses[0].result);
                    }
                  );
                })
              ]);

              console.log("Scraped data:", result);

              if (!result) {
                throw new Error("Scraped data is null or undefined.");
              }

              await sendDataToServer(result);

              const waiter = Array.isArray(item.requests)
                ? item.requests.find(
                  (cmd) => cmd.type?.toLowerCase().trim() === "waiter"
                )
                : null;

              if (waiter && !isNaN(waiter.time)) {
                console.log(`Post-script wait for ${waiter.time} ms...`);
                await waitMs(waiter.time);
              }

              clearTimeout(loadTimeout);
              resolve(result);

            } catch (err) {
              console.warn("Error during content script execution:", err);
              chrome.tabs.onUpdated.removeListener(onUpdated);
              clearTimeout(loadTimeout);
              reject(err);
            }
          }
        };

        chrome.tabs.onUpdated.addListener(onUpdated);
      });
    });
  });
}

/***********************************************
 * contentScriptFunction(item): runs in the tab
 ***********************************************/
async function contentScriptFunction(item) {
  const flags = Array.isArray(item.flags) ? item.flags : [];

  if (flags.includes("remove-videos")) {
    document.querySelectorAll("video").forEach((video) => video.remove());
  } else if (flags.includes("pause-videos")) {
    document.querySelectorAll("video").forEach((video) => video.pause());
  }

  if (flags.includes("clear-local-storage")) {
    try {
      localStorage.clear();
    } catch (e) {
      console.warn("Could not clear localStorage:", e);
    }
  }

  if (flags.includes("clear-session-storage")) {
    try {
      sessionStorage.clear();
    } catch (e) {
      console.warn("Could not clear sessionStorage:", e);
    }
  }

  if (flags.includes("clear-cookies")) {
    try {
      document.cookie.split(";").forEach((cookie) => {
        const name = cookie.trim().split("=")[0];
        document.cookie = `${name}=;expires=${new Date(0).toUTCString()};path=/;`;
      });
    } catch (e) {
      console.warn("Could not clear cookies:", e);
    }
  }

  if (flags.includes("disable-animation")) {
    try {
      const styleEl = document.createElement("style");
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
      console.warn("Could not disable animations/transparency:", e);
    }
  }

  if (flags.includes("disable-indexed-db")) {
    try {
      Object.defineProperty(window, "indexedDB", {
        get() {
          console.warn("indexedDB is disabled by script injection.");
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

    if (cmdType === "waiter") {
      const waitTime = Number(cmd.time);
      if (!isNaN(waitTime) && waitTime > 0) {
        console.log(`Waiting for ${waitTime} ms...`);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      }
      continue;
    }

    if (cmdType === "patent") {
      patentCommands.push(cmd);
      continue;
    }

    if (
      cmdType === "attr" ||
      cmdType === "tag" ||
      cmdType === "html" ||
      cmdType === "img"
    ) {
      extractionCommands.push(cmd);
      continue;
    }

    if (cmdType === "click") {
      const els = queryElements(cmd.selector, document);
      els.forEach((el) => el.click());
      continue;
    }

    if (cmdType === "fill") {
      const els = queryElements(cmd.selector, document);
      els.forEach((el) => {
        el.value = cmd.value;
        el.dispatchEvent(new Event("change", { bubbles: true }));
        el.dispatchEvent(new Event("input", { bubbles: true }));
      });
      continue;
    }
  }

  function extractAttributeName(cmd) {
    if (cmd.attribute && typeof cmd.attribute === "string") {
      return cmd.attribute.trim();
    }

    if (typeof cmd.selector !== "string") {
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

    return lastMatch.split("=")[0].trim();
  }

  function cleanText(value) {
    if (typeof value !== "string") {
      return value;
    }
    return value.replace(/\s+/g, " ").trim();
  }

  // Supports basic CSS selectors plus two extensions:
  // 1) :has-text("value") filters matched nodes by inner text containing the
  //    provided value (case-insensitive).
  // 2) Segments separated by " >> " allow step-by-step scoping; segments
  //    prefixed with "next:" switch the search root to the next sibling of the
  //    current context before applying the selector.
  //
  // Example: "#contenu >> h3:has-text('Officers') >> next:div >> p.principal"
  //  - finds the #contenu section
  //  - within it, picks the H3 whose text contains "Officers"
  //  - moves to the next sibling DIV after that H3
  //  - then selects the descendant paragraphs with the .principal class

  function normalizeSelectorSegment(segment) {
    if (typeof segment !== "string") {
      return segment;
    }

    return segment.replace(
      /(^|[^:])(\b[a-zA-Z][a-zA-Z0-9_-]*|\*)\((['"])(.*?)\3\)/g,
      (match, prefix, tagName, quote, text) =>
        `${prefix}${tagName}:has-text(${quote}${text}${quote})`
    );
  }

  function queryElements(selector, context = document) {
    if (typeof selector !== "string" || !selector.trim()) {
      return [];
    }

    const segments = selector
      .split(">>")
      .map((part) => part.trim())
      .filter(Boolean);

    let contexts = [context];

    for (const rawSegment of segments) {
      const segment = rawSegment.trim();
      if (!segment) {
        return [];
      }

      const isNextSibling = segment.toLowerCase().startsWith("next:");
      const selectorBody = normalizeSelectorSegment(
        isNextSibling ? segment.slice("next:".length).trim() : segment
      );

      const hasTextMatch = selectorBody.match(/:has-text\(("|')(.*?)(\1)\)/i);
      const textNeedle = hasTextMatch?.[2]?.toLowerCase();
      const baseSelector = hasTextMatch
        ? selectorBody.replace(hasTextMatch[0], "").trim() || "*"
        : selectorBody;
      const allowSiblingTextFallback =
        Boolean(textNeedle) && baseSelector.includes("+");

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
          // When using the custom "next:" prefix we only want the immediate
          // sibling, not every descendant that also matches the selector. This
          // prevents a segment such as "next:div" from greedily collecting all
          // nested DIVs before the following selector segments are applied.
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
            (el?.textContent || "").toLowerCase().includes(textNeedle);

          matched = matched.filter((node) => {
            if (allowSiblingTextFallback) {
              // For selectors like "label:has-text('Street:') + value" only
              // accept nodes whose immediate previous sibling matches the label
              // text, instead of matching nodes that contain the text themselves.
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
    if (type === "attr") {
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

    if (type === "tag") {
      const textValue = element.textContent;
      if (textValue == null) {
        return undefined;
      }
      return cleanText(textValue);
    }

    if (type === "html") {
      return element.outerHTML;
    }

    if (type === "img") {
      const imageValue = await captureImageFromElement(element, cmd);
      return imageValue;
    }

    return undefined;
  }

  function normaliseUrl(rawUrl) {
    if (!rawUrl || typeof rawUrl !== "string") {
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
      return "";
    }

    const bytes = new Uint8Array(arrayBuffer);
    let binary = "";
    const chunkSize = 0x8000;
    for (let i = 0; i < bytes.length; i += chunkSize) {
      const chunk = bytes.subarray(i, i + chunkSize);
      binary += String.fromCharCode.apply(null, chunk);
    }
    return btoa(binary);
  }

  async function blobToPngDataUrl(blob) {
    if (!blob) {
      return null;
    }

    const supportOffscreen =
      typeof OffscreenCanvas !== "undefined" &&
      typeof OffscreenCanvas.prototype.convertToBlob === "function";

    if (supportOffscreen && typeof createImageBitmap === "function") {
      try {
        const bitmap = await createImageBitmap(blob);
        const canvas = new OffscreenCanvas(bitmap.width || 1, bitmap.height || 1);
        const ctx = canvas.getContext("2d");
        ctx.drawImage(bitmap, 0, 0);
        const pngBlob = await canvas.convertToBlob({ type: "image/png" });
        bitmap.close();
        const arrayBuffer = await pngBlob.arrayBuffer();
        const base64 = arrayBufferToBase64(arrayBuffer);
        return `data:image/png;base64,${base64}`;
      } catch (error) {
        console.warn("Failed to convert image using OffscreenCanvas:", error);
      }
    }

    return new Promise((resolve, reject) => {
      const objectUrl = URL.createObjectURL(blob);
      const img = document.createElement("img");

      img.onload = () => {
        try {
          const canvas = document.createElement("canvas");
          const width = img.naturalWidth || img.width || 1;
          const height = img.naturalHeight || img.height || 1;
          canvas.width = width;
          canvas.height = height;
          const ctx = canvas.getContext("2d");
          ctx.drawImage(img, 0, 0, width, height);
          const dataUrl = canvas.toDataURL("image/png");
          resolve(dataUrl);
        } catch (err) {
          reject(err);
        } finally {
          URL.revokeObjectURL(objectUrl);
        }
      };

      img.onerror = (err) => {
        URL.revokeObjectURL(objectUrl);
        reject(err);
      };

      img.src = objectUrl;
    }).catch((error) => {
      console.warn("Failed to convert image using fallback canvas:", error);
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
      const mimeType = blob.type || "application/octet-stream";
      return `data:${mimeType};base64,${base64}`;
    } catch (error) {
      console.warn("Failed to convert image blob to original format:", error);
      return null;
    }
  }

  function inferExtensionFromContentType(contentType) {
    if (!contentType || typeof contentType !== "string") {
      return null;
    }

    const mime = contentType.split(";")[0].trim().toLowerCase();
    const map = {
      "image/jpeg": "jpg",
      "image/jpg": "jpg",
      "image/png": "png",
      "image/gif": "gif",
      "image/webp": "webp",
      "image/svg+xml": "svg",
      "image/bmp": "bmp",
      "image/x-icon": "ico",
      "image/vnd.microsoft.icon": "ico",
    };

    if (map[mime]) {
      return map[mime];
    }

    if (mime.startsWith("image/")) {
      return mime.split("/")[1];
    }

    return null;
  }

  function inferExtensionFromUrl(rawUrl) {
    if (!rawUrl || typeof rawUrl !== "string") {
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
      element.currentSrc || element.src || element.getAttribute("src") || "";
    const absoluteUrl = normaliseUrl(src);
    if (!absoluteUrl) {
      return undefined;
    }

    try {
      const response = await fetch(absoluteUrl, {
        mode: "cors",
        credentials: "omit",
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
        typeof item.id !== "undefined" && item.id !== null
          ? String(item.id)
          : cmd.name || "image";

      const imageName = cmd?.name || cmd?.type || "image";
      const results = [];

      if (pngDataUrl) {
        results.push({
          type: "img",
          name: imageName,
          dataUrl: pngDataUrl,
          fileName: baseFileName,
          extension: "png",
          sourceUrl: absoluteUrl,
          contentType: "image/png",
        });
      }

      if (originalDataUrl) {
        const responseContentType = response.headers.get("content-type");
        const mimeType = blob.type || responseContentType || "application/octet-stream";
        const inferredExtension =
          inferExtensionFromContentType(mimeType) ||
          inferExtensionFromUrl(absoluteUrl) ||
          "img";

        results.push({
          type: "img",
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
      console.warn("Failed to capture image:", error);
      return undefined;
    }
  }

  async function extractValuesFromContext(context, commands) {
    if (!commands.length) {
      return [];
    }

    if (typeof context.querySelectorAll !== "function") {
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
        const value = await buildRecordFromElement(element, extractCmd);
        if (value !== undefined) {
          values.push(value);
        }
      }

      if (values.length === 0) {
        continue;
      }

      // When a selector matches multiple elements we want to store the collected
      // values in a single field, separated by the "◙" symbol. This keeps all
      // related data together instead of spreading it across multiple records.
      // if (values.length > 1) {
      //   const joinedValue = values
      //     .map((val) => (typeof val === "string" ? val : JSON.stringify(val)))
      //     .join("◙");
      //   values.splice(0, values.length, joinedValue);
      // }

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

/************************************************
 * Simple wait function
 ************************************************/
function waitMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/************************************************
 * Send data to local server
 ************************************************/
async function sendDataToServer(scrapedData) {
const app = express();

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

const folderName = 'data';
if (!fs.existsSync(folderName)) {
  fs.mkdirSync(folderName, { recursive: true });
}

const imageFolderName = 'images';
if (!fs.existsSync(imageFolderName)) {
  fs.mkdirSync(imageFolderName, { recursive: true });
}

const dataFilePath = path.join(folderName, 'data.csv');

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

  while (fs.existsSync(path.join(imageFolderName, candidate))) {
    candidate = ensureImageExtension(`${base}-${counter}`, extension);
    counter += 1;
  }

  return path.join(imageFolderName, candidate);
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

app.post('/scrape_data', async (req, res) => {
  try {
    const { l_scraped_data } = req.body;

    if (!l_scraped_data) {
      return res.status(400).json({ error: 'Missing l_scraped_data field' });
    }

    const originalPayload = JSON.parse(l_scraped_data);
    let parsedData = JSON.parse(l_scraped_data);

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
      return res.status(200).json({ success: true, skippedPersistence: true });
    }

    let payloadArray = [];
    let baseMetadata = {};

    if (Array.isArray(parsedData)) {
      const processedArray = [];
      for (let index = 0; index < parsedData.length; index += 1) {
        processedArray.push(await processEntryImages(parsedData[index], baseId, index));
      }
      parsedData = processedArray;

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
        parsedData = { ...baseMetadata, data: processedDataArray };

        payloadArray = processedDataArray.map((entry) => {
          if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
            return { ...baseMetadata, ...entry };
          }

          return { ...baseMetadata, value: normaliseValue(entry) };
        });
      } else if (data !== undefined) {
        const processedDataValue = await processEntryImages(data, baseId, 'data');
        parsedData = { ...baseMetadata, data: processedDataValue };

        if (Object.keys(baseMetadata).length > 0) {
          payloadArray = [{ ...baseMetadata }];
        }
      } else {
        parsedData = { ...baseMetadata };
        if (Object.keys(baseMetadata).length > 0) {
          payloadArray = [{ ...baseMetadata }];
        }
      }
    }

    if (payloadArray.length === 0) {
      console.warn('No structured data received to persist.');
    } else {
      const existingHeaders = loadCsvHeaders(dataFilePath);
      const existingRows = loadCsvRows(dataFilePath);

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

      writeCsvFile(dataFilePath, headers, [...reconciledExistingRows, ...newRows]);
    }

    const fileName = `${baseId}.json`;
    const filePath = path.join(folderName, fileName);

    fs.writeFileSync(filePath, JSON.stringify(parsedData, null, 2));

    console.log(`Saved data to ${fileName}`);

    return res.status(200).json({ success: true, fileName });
  } catch (error) {
    console.error('Error processing request:', error);
    return res.status(500).json({ error: error.toString() });
  }
});

const PORT = 3021;
app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});

}
