export const DEFAULT_SETTINGS = { port: 8080, enabled: true };

// One entry per link domain: the public name, and the localhost suffix the
// gateway serves it under. Rule ids follow this order.
export const DOMAINS = [
  { name: "blake3.net", origin: "blake3.localhost" },
  { name: "pkarr.net", origin: "pkarr.localhost" },
];

// Rule 3 is retired, but stays listed so upgrades remove it.
export const RULE_IDS = [1, 2, 3];
export const HOST_ORIGINS = DOMAINS.flatMap(({ name }) => [
  `http://*.${name}/*`,
  `https://*.${name}/*`,
]);

// Firefox provides `browser`; Chrome and Brave only `chrome`.
export const api = globalThis.browser ?? globalThis.chrome;

export function validateSettings(settings) {
  if (!Number.isInteger(settings.port) || settings.port < 1 || settings.port > 65535) {
    throw new Error("Enter a port from 1 to 65535.");
  }
  if (typeof settings.enabled !== "boolean") {
    throw new Error("Invalid enabled setting.");
  }
  return { port: settings.port, enabled: settings.enabled };
}

export function makeRules(settings) {
  const { port, enabled } = validateSettings(settings);
  if (!enabled) return [];
  const resourceTypes = [
    "main_frame", "sub_frame", "stylesheet", "script", "image", "font",
    "object", "xmlhttprequest", "ping", "csp_report", "media", "other",
  ];
  return DOMAINS.map(({ name, origin }, index) => ({
    id: index + 1,
    priority: 1,
    action: {
      type: "redirect",
      redirect: { regexSubstitution: `http://\\1.${origin}:${port}/\\2` },
    },
    condition: {
      regexFilter: `^https?://([a-z0-9]+)\\.${name.replace(".", "\\.")}(?::[0-9]+)?/(.*)$`,
      isUrlFilterCaseSensitive: true,
      resourceTypes,
    },
  }));
}

export async function readSettings() {
  const { settings } = await api.storage.local.get("settings");
  return validateSettings(settings ?? DEFAULT_SETTINGS);
}

export async function applySettings(settings) {
  settings = validateSettings(settings);
  await api.declarativeNetRequest.updateDynamicRules({
    removeRuleIds: RULE_IDS,
    addRules: makeRules(settings),
  });
  await api.storage.local.set({ settings });
}
