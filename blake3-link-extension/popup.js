import { applySettings, readSettings } from "./rules.js";

const form = document.querySelector("#settings");
const port = document.querySelector("#port");
const enabled = document.querySelector("#enabled");
const status = document.querySelector("#status");
const save = form.querySelector("button");

try {
  const settings = await readSettings();
  port.value = settings.port;
  enabled.checked = settings.enabled;
  save.disabled = false;
} catch (error) {
  status.textContent = error.message;
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  save.disabled = true;
  try {
    await applySettings({ port: port.valueAsNumber, enabled: enabled.checked });
    status.textContent = enabled.checked ? `Saved: http://127.0.0.1:${port.value}` : "Local redirects disabled.";
  } catch (error) {
    status.textContent = error.message;
  } finally {
    save.disabled = false;
  }
});
