async function loadInstructions() {
  if (cachedInstructions) {
    return cachedInstructions;
  }

  const url = chrome.runtime.getURL("list.json");
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`don't loud list.json: ${response.status}`);
  }

  const instructions = await response.json();
  if (!Array.isArray(instructions)) {
    throw new Error("list.json don't have obackt");
  }

  cachedInstructions = instructions;
  return instructions;
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
  const bodyObj = {
    l_scraped_data: JSON.stringify(scrapedData),
  };

  const resp = await fetch("http://localhost:3021/scrape_data", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(bodyObj),
  });

  if (!resp.ok) {
    throw new Error(`Server responded with ${resp.status}`);
  }

  const respJson = await resp.json();
  console.log("Local server response:", respJson);
}
