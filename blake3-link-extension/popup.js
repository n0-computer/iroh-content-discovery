import { api, applySettings, HOST_ORIGINS, readSettings } from "./rules.js";

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
  if (!(await api.permissions.contains({ origins: HOST_ORIGINS }))) {
    status.textContent = "Click Save to allow redirects on *.blake3.link.";
  }
} catch (error) {
  status.textContent = error.message;
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  // Firefox lets users withhold host access, and only grants it from a user
  // gesture, so request it before the first await.
  const granted = enabled.checked
    ? api.permissions.request({ origins: HOST_ORIGINS })
    : Promise.resolve(true);
  save.disabled = true;
  try {
    if (!(await granted)) {
      status.textContent = "Redirects need access to *.blake3.link.";
      return;
    }
    await applySettings({ port: port.valueAsNumber, enabled: enabled.checked });
    status.textContent = enabled.checked ? `Saved: http://127.0.0.1:${port.value}` : "Local redirects disabled.";
  } catch (error) {
    status.textContent = error.message;
  } finally {
    save.disabled = false;
  }
});
