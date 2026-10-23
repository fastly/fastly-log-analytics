// Shared service configuration label maps and retention option tables.

export const SHIELD_MAP: Record<string, string> = {
  "us-east-1": "iad-va-us", // Ashburn, VA
  "us-west": "bfi-wa-us", // Seattle, WA (BFI)
  "us-central-1": "chi-il-us", // Chicago, IL (CHI)
  "eu-central-1": "frankfurt-de", // Frankfurt, Germany
  "eu-south-1": "mxp-milan-it", // Milan, Italy
  "eu-west-1": "mxp-paris-fr", // Paris, France
  "uk-east-1": "london-uk", // London, UK
  "jp-central-1": "nrt-tokyo-jp", // Tokyo, Japan (NRT)
  "au-east-1": "sydney-au", // Sydney, Australia
};

export const REGION_LABELS: Record<string, string> = {
  "us-east-1": "US East (Ashburn)",
  "us-west": "US West (Seattle)",
  "us-central-1": "US Central (Chicago)",
  "eu-central-1": "EU Central (Frankfurt)",
  "eu-south-1": "EU South (Milan)",
  "eu-west-1": "EU West (Paris)",
  "uk-east-1": "UK East (London)",
  "jp-central-1": "JP Central (Tokyo)",
  "au-east-1": "AU East (Sydney)",
};

export const SHIELD_LABELS: Record<string, string> = {
  none: "None",
  "iad-va-us": "IAD (Ashburn)",
  "bfi-wa-us": "BFI (Seattle)",
  "chi-il-us": "CHI (Chicago)",
  "frankfurt-de": "FRA (Frankfurt)",
  "mxp-milan-it": "MXP (Milan)",
  "mxp-paris-fr": "MXP (Paris)",
  "london-uk": "LHR (London)",
  "nrt-tokyo-jp": "NRT (Tokyo)",
  "sydney-au": "SYD (Sydney)",
};

export const PERIOD_LABELS: Record<string, string> = {
  "1": "1 second",
  "5": "5 seconds",
  "10": "10 seconds",
  "20": "20 seconds",
  "30": "30 seconds",
  "60": "1 minute",
  "120": "2 minutes",
  "300": "5 minutes",
  "600": "10 minutes",
  "900": "15 minutes",
};

export const RETENTION_OPTIONS = [
  { value: "1", label: "1 day" },
  { value: "3", label: "3 days" },
  { value: "7", label: "7 days" },
  { value: "14", label: "14 days" },
  { value: "30", label: "30 days" },
  { value: "90", label: "90 days" },
  { value: "180", label: "180 days" },
  { value: "365", label: "365 days" },
  { value: "0", label: "Forever" },
] as const;
