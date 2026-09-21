import { applySettings, readSettings } from "./rules.js";

// Dynamic rules persist across restarts and run without waking this worker.
chrome.runtime.onInstalled.addListener(() => {
  readSettings().then(applySettings).catch(console.error);
});
