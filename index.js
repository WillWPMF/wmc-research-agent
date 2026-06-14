const express = require('express');
const cron = require('node-cron');
const { Pool } = require('pg');
const Anthropic = require('@anthropic-ai/sdk');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_PUBLIC_URL,
  ssl: process.env.DATABASE_PUBLIC_URL ? { rejectUnauthorized: false } : false,
});

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const HUBSPOT_OWNER_ID = '30543251';
const HUBSPOT_HEADERS = () => ({
  Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`,
  'Content-Type': 'application/json',
});

let lastRun = null;
let currentlyProcessing = null;

// ── DB init ───────────────────────────────────────────────────────────────────

async function initDb() {
  await pool.query(`CREATE TABLE IF NOT EXISTS research_briefs (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(255) UNIQUE NOT NULL,
    contact_id VARCHAR(255),
    contact_name VARCHAR(255),
    company_name VARCHAR(255),
    task_title VARCHAR(255),
    task_due_date TIMESTAMP,
    download_context VARCHAR(255),
    brief_text TEXT,
    hubspot_note_id VARCHAR(255),
    status VARCHAR(50) DEFAULT 'pending',
    error_text TEXT,
    created_at TIMESTAMP DEFAULT NOW()
  )`);
  await pool.query(`ALTER TABLE research_briefs ADD COLUMN IF NOT EXISTS input_tokens INTEGER`);
  await pool.query(`ALTER TABLE research_briefs ADD COLUMN IF NOT EXISTS output_tokens INTEGER`);
  await pool.query(`ALTER TABLE research_briefs ADD COLUMN IF NOT EXISTS estimated_cost_usd DECIMAL(10,4)`);
  await pool.query(`ALTER TABLE research_briefs ADD COLUMN IF NOT EXISTS cache_creation_tokens INTEGER`);
  await pool.query(`ALTER TABLE research_briefs ADD COLUMN IF NOT EXISTS cache_read_tokens INTEGER`);
  await pool.query(`ALTER TABLE research_briefs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`);
  await pool.query(`ALTER TABLE research_briefs ADD COLUMN IF NOT EXISTS skip_reason VARCHAR(255)`);
  await pool.query(`ALTER TABLE research_briefs ADD COLUMN IF NOT EXISTS previous_brief_id INTEGER REFERENCES research_briefs(id)`);
  await pool.query(`UPDATE research_briefs SET status = 'queued', updated_at = NOW() WHERE status = 'processing'`);
  await pool.query(`CREATE TABLE IF NOT EXISTS agent_settings (
    id INTEGER PRIMARY KEY DEFAULT 1,
    enabled BOOLEAN DEFAULT true,
    task_range VARCHAR(50) DEFAULT 'due_today',
    range_days INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT single_row CHECK (id = 1)
  )`);
  await pool.query(`INSERT INTO agent_settings (id, enabled, task_range, range_days)
    VALUES (1, true, 'due_today', 0) ON CONFLICT (id) DO NOTHING`);
  console.log('[Research Agent] Database initialised');
}

async function getSettings() {
  const { rows } = await pool.query('SELECT * FROM agent_settings WHERE id = 1');
  return rows[0] || { enabled: true, task_range: 'due_today', range_days: 0 };
}

// ── Step 1: Fetch tasks to process ───────────────────────────────────────────

async function fetchTasksToProcess(settings) {
  const now = new Date();
  const baseFilters = [
    { propertyName: 'hs_task_status', operator: 'EQ', value: 'NOT_STARTED' },
    { propertyName: 'hubspot_owner_id', operator: 'EQ', value: HUBSPOT_OWNER_ID },
  ];

  let rangeFilters;
  if (settings.task_range === 'overdue_by') {
    const cutoff = new Date(now.getTime() - (settings.range_days || 0) * 24 * 60 * 60 * 1000);
    rangeFilters = [{ propertyName: 'hs_timestamp', operator: 'LTE', value: String(cutoff.getTime()) }];
  } else if (settings.task_range === 'due_within') {
    const cutoff = new Date(now.getTime() + (settings.range_days || 0) * 24 * 60 * 60 * 1000);
    cutoff.setUTCHours(23, 59, 59, 999);
    rangeFilters = [
      { propertyName: 'hs_timestamp', operator: 'GTE', value: String(now.getTime()) },
      { propertyName: 'hs_timestamp', operator: 'LTE', value: String(cutoff.getTime()) },
    ];
  } else {
    const startOfDay = new Date(); startOfDay.setUTCHours(0, 0, 0, 0);
    const endOfDay = new Date(); endOfDay.setUTCHours(23, 59, 59, 999);
    rangeFilters = [
      { propertyName: 'hs_timestamp', operator: 'GTE', value: String(startOfDay.getTime()) },
      { propertyName: 'hs_timestamp', operator: 'LTE', value: String(endOfDay.getTime()) },
    ];
  }

  let resp;
  try {
    resp = await axios.post(
      'https://api.hubapi.com/crm/v3/objects/tasks/search',
      {
        filterGroups: [{ filters: [...baseFilters, ...rangeFilters] }],
        properties: ['hs_task_subject', 'hubspot_owner_id', 'hs_timestamp', 'hs_task_status'],
        limit: 100,
      },
      { headers: HUBSPOT_HEADERS() }
    );
  } catch (error) {
    console.error('[Research Agent] HubSpot task search failed:', error.response?.status, JSON.stringify(error.response?.data));
    throw error;
  }

  return resp.data.results || [];
}

// ── Step 2: Get contact and company ──────────────────────────────────────────

async function getTaskContact(taskId) {
  let assocResults;
  try {
    const assocResp = await axios.get(
      `https://api.hubapi.com/crm/v4/objects/tasks/${taskId}/associations/contacts`,
      { headers: HUBSPOT_HEADERS() }
    );
    assocResults = assocResp.data.results || [];
  } catch (error) {
    console.error('[Research Agent] HubSpot task associations fetch failed:', error.response?.status, JSON.stringify(error.response?.data));
    throw error;
  }

  if (!assocResults.length) return null;

  const contactId = String(assocResults[0].toObjectId);

  try {
    const contactResp = await axios.get(
      `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`,
      {
        params: {
          properties: [
            'firstname', 'lastname', 'email', 'company', 'jobtitle',
            'hs_lead_status', 'recent_conversion_event_name', 'hs_analytics_last_referrer',
            'notes_last_contacted',
          ].join(','),
        },
        headers: HUBSPOT_HEADERS(),
      }
    );
    return { id: contactId, ...contactResp.data.properties };
  } catch (error) {
    console.error('[Research Agent] HubSpot contact fetch failed:', error.response?.status, JSON.stringify(error.response?.data));
    throw error;
  }
}

async function getContactCompany(contactId) {
  let assocResults;
  try {
    const assocResp = await axios.get(
      `https://api.hubapi.com/crm/v4/objects/contacts/${contactId}/associations/companies`,
      { headers: HUBSPOT_HEADERS() }
    );
    assocResults = assocResp.data.results || [];
  } catch (error) {
    console.error('[Research Agent] HubSpot company associations fetch failed:', error.response?.status, JSON.stringify(error.response?.data));
    return null;
  }

  if (!assocResults.length) return null;

  const companyId = String(assocResults[0].toObjectId);

  try {
    const companyResp = await axios.get(
      `https://api.hubapi.com/crm/v3/objects/companies/${companyId}`,
      {
        params: { properties: 'name,industry,numberofemployees,city' },
        headers: HUBSPOT_HEADERS(),
      }
    );
    return companyResp.data.properties;
  } catch (error) {
    console.error('[Research Agent] HubSpot company fetch failed:', error.response?.status, JSON.stringify(error.response?.data));
    return null;
  }
}

async function lookupContactByEmail(email) {
  let resp;
  try {
    resp = await axios.post(
      'https://api.hubapi.com/crm/v3/objects/contacts/search',
      {
        filterGroups: [{ filters: [{ propertyName: 'email', operator: 'EQ', value: email }] }],
        properties: [
          'firstname', 'lastname', 'email', 'company', 'jobtitle',
          'hs_lead_status', 'recent_conversion_event_name',
        ],
        limit: 1,
      },
      { headers: HUBSPOT_HEADERS() }
    );
  } catch (error) {
    console.error('[Research Agent] HubSpot contact search by email failed:', error.response?.status, JSON.stringify(error.response?.data));
    throw error;
  }
  const results = resp.data.results || [];
  if (!results.length) return null;
  return { id: results[0].id, ...results[0].properties };
}

function buildDownloadContext(contact, taskTitle) {
  const parts = [];
  if (taskTitle && taskTitle.toLowerCase().includes('download')) parts.push(taskTitle);
  if (taskTitle && taskTitle.toLowerCase().includes('enquir')) parts.push(taskTitle);
  if (contact.recent_conversion_event_name) parts.push(contact.recent_conversion_event_name);
  if (contact.hs_lead_status) parts.push(`Lead status: ${contact.hs_lead_status}`);
  if (!parts.length && taskTitle) parts.push(taskTitle);
  return parts.join(' | ') || 'Not specified';
}

// ── Step 3: Research company with Claude ─────────────────────────────────────

function buildNoteHtml(data, contactName) {
  const hr = '<hr style="border:none;border-top:1px solid #E8ECF0;margin:16px 0;">';

  function sectionHeader(emoji, title) {
    return `<p style="margin-top:16px;margin-bottom:4px;"><strong style="color:#3B6FE8;">${emoji} ${title}</strong></p>`;
  }

  function srcLink(url, domain) {
    if (!url) return domain || '';
    return `<a href="${url}" target="_blank" style="color:#A0AEC0;">${domain || url}</a>`;
  }

  function sourcesLine(sources) {
    const valid = (sources || []).filter(s => s && (s.url || s.sourceUrl));
    if (!valid.length) return '<p style="font-size:11px;color:#A0AEC0;margin-top:8px;">Sources: none found</p>';
    const links = valid.map(s => srcLink(s.url || s.sourceUrl, s.domain || s.sourceDomain)).join(', ');
    return `<p style="font-size:11px;color:#A0AEC0;margin-top:8px;">Sources: ${links}</p>`;
  }

  function inlineSrc(url, domain) {
    if (!url) return '';
    return ` <span style="font-size:11px;color:#A0AEC0;">[${srcLink(url, domain)}]</span>`;
  }

  let html = '';

  // Suggested opener
  const openerSrc = data.recentNews?.[0] ? [{ url: data.recentNews[0].sourceUrl, domain: data.recentNews[0].sourceDomain }] : [];
  html += sectionHeader('📞', 'SUGGESTED OPENER');
  html += `<p style="font-size:15px;font-style:italic;">"${data.suggestedOpener || 'No opener generated.'}"</p>`;
  html += sourcesLine(openerSrc);
  html += hr;

  // Company snapshot
  html += sectionHeader('🏢', 'COMPANY SNAPSHOT');
  html += `<p>${data.companySnapshot || 'No information found.'}</p>`;
  const snapshotSrc = (data.recentNews || []).slice(0, 2).map(n => ({ url: n.sourceUrl, domain: n.sourceDomain }));
  html += sourcesLine(snapshotSrc);
  html += hr;

  // Recent news
  html += sectionHeader('📰', 'RECENT NEWS &amp; SIGNALS');
  if (data.recentNews && data.recentNews.length > 0) {
    html += '<ul>';
    for (const item of data.recentNews) {
      html += `<li>${item.point}${inlineSrc(item.sourceUrl, item.sourceDomain)}</li>`;
    }
    html += '</ul>';
    html += sourcesLine(data.recentNews);
  } else {
    html += '<p>No recent news found.</p>';
    html += sourcesLine([]);
  }
  html += hr;

  // Wellbeing check
  html += sectionHeader('🔍', 'WELLBEING ACTIVITY &amp; PROVIDER CHECK');
  const wc = data.wellbeingCheck || {};
  if (wc.findings && wc.findings.length > 0) {
    html += '<ul>';
    for (const f of wc.findings) {
      html += `<li>${f.point}${inlineSrc(f.sourceUrl, f.sourceDomain)}</li>`;
    }
    html += '</ul>';
  }
  html += `<p>${wc.summary || (wc.noProviderFound !== false ? 'No public activity found around MHFA, neurodiversity, menopause or wellbeing training — suggests this may be new territory for the organisation.' : '')}</p>`;
  html += sourcesLine(wc.findings || []);
  html += hr;

  // About contact
  const contactLabel = contactName ? `ABOUT ${contactName.toUpperCase()}` : 'ABOUT THE CONTACT';
  html += sectionHeader('👤', contactLabel);
  const ac = data.aboutContact || {};
  html += `<p>${ac.summary || 'No profile found.'}</p>`;
  if (ac.verified === false) {
    html += '<p style="color:#A0AEC0;font-size:12px;">Profile could not be verified as matching both name and company.</p>';
  }
  html += sourcesLine(ac.sources || []);
  html += hr;

  // Recommended angle
  html += sectionHeader('💡', 'RECOMMENDED ANGLE');
  const ra = data.recommendedAngle || {};
  if (ra.situation || ra.opening || ra.ask) {
    if (ra.situation) html += `<p><strong>The situation:</strong> ${ra.situation}</p>`;
    if (ra.opening)   html += `<p><strong>The opening:</strong> ${ra.opening}</p>`;
    if (ra.ask)       html += `<p><strong>The ask:</strong> ${ra.ask}</p>`;
  } else {
    html += '<p>No angle generated.</p>';
  }
  html += sourcesLine(ra.sources || []);

  return html;
}

async function researchCompany({ taskId, contactName, companyName, industry, employeeCount, city, jobTitle, downloadContext }) {
  const currentYear = new Date().getFullYear();
  const contactFirst = (contactName || '').split(' ')[0];
  const contactLast = (contactName || '').split(' ').slice(1).join(' ');

  const systemPrompt = `You are a research assistant for The Workplace Mindfulness Co. (WMC), a UK workplace wellbeing training company. You prepare pre-call research briefs for sales calls. WMC delivers mental health awareness, MHFA training, resilience, menopause, and neurodiversity programmes.

## ACCURACY RULES

- Every factual claim MUST be based on a web search result from this session — never infer, assume, or guess.
- Verify the contact: their result must match BOTH their name AND company. If you cannot verify both, write "Could not verify — results inconclusive."
- Cite a source URL for every factual claim. If you cannot cite it, do not include it.
- If a section has no findings, write "Not found" — do not pad with generic statements.

## OUTPUT FORMAT — JSON only

Return ONLY valid JSON. No markdown, no backticks, no preamble.

{
  "suggestedOpener": "one literal sentence Olly could say to open the call, referencing one specific verified fact",
  "companySnapshot": "2-3 sentences: what the company does, size, sector, recent trajectory",
  "recentNews": [
    { "point": "specific verified fact", "sourceUrl": "string", "sourceDomain": "string" }
  ],
  "wellbeingCheck": {
    "summary": "one sentence conclusion — e.g. already running MHFA, or no public activity found",
    "findings": [
      { "point": "specific finding e.g. EAP provided by Bupa (named on careers page)", "sourceUrl": "string", "sourceDomain": "string" }
    ],
    "noProviderFound": false
  },
  "aboutContact": {
    "summary": "verified role exactly as written on profile, tenure, background. If unverified write: Could not verify.",
    "verified": true,
    "sources": [{ "url": "string", "domain": "string" }]
  },
  "recommendedAngle": {
    "situation": "2-3 sentences on context and trigger",
    "opening": "2-3 sentences connecting their situation to WMC services",
    "ask": "1 sentence on what to propose in this call",
    "sources": [{ "url": "string", "domain": "string" }]
  }
}

## PRIORITISED SEARCH ORDER — use at most 4 searches in this order

1. "${companyName} news ${currentYear}"
2. "${companyName} LinkedIn OR ${companyName} wellbeing OR ${companyName} MHFA"
3. "${contactFirst} ${contactLast} ${companyName} LinkedIn"
4. "${companyName} EAP OR ${companyName} employee assistance OR ${companyName} mental health training"`;

  const userMessage = `Company: ${companyName}
Industry: ${industry || 'Not specified'}
Headcount: ${employeeCount || 'Unknown'}
Location: ${city || 'Unknown'}
Contact: ${contactName || 'Unknown'} — ${jobTitle || 'Unknown'}
What they downloaded / enquired about: ${downloadContext}

Research this company and contact using web search, then return the JSON brief.`;

  console.log('[Research Agent] API key present:', !!process.env.ANTHROPIC_API_KEY);
  console.log('[Research Agent] API key length:', process.env.ANTHROPIC_API_KEY?.length);
  console.log('[Research Agent] API key prefix:', process.env.ANTHROPIC_API_KEY?.substring(0, 15));
  console.log('[Research Agent] API key suffix:', process.env.ANTHROPIC_API_KEY?.substring(process.env.ANTHROPIC_API_KEY.length - 5));

  let response;
  try {
    response = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 2048,
      tools: [{
        type: 'web_search_20250305',
        name: 'web_search',
        max_uses: 4,
      }],
      system: [{ type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }],
      messages: [{ role: 'user', content: userMessage }],
    });
  } catch (error) {
    console.error('[Research Agent] Anthropic research call failed:', error.status, JSON.stringify(error.error ?? error.message));
    throw error;
  }

  if (response.stop_reason === 'max_tokens') {
    console.warn('[Research Agent] Response truncated due to max_tokens for task:', taskId);
  }

  const textBlock = response.content.findLast(b => b.type === 'text');
  const rawResponse = textBlock ? textBlock.text.trim() : '';

  let briefText;
  try {
    const data = JSON.parse(rawResponse);
    briefText = buildNoteHtml(data, contactName);
  } catch (e) {
    console.error('[Research Agent] JSON parse failed, using raw text fallback');
    briefText = rawResponse ? `<p>${rawResponse}</p>` : '<p>Research could not be completed.</p>';
  }

  const usage = response.usage || {};
  const cacheCreationTokens = usage.cache_creation_input_tokens || 0;
  const cacheReadTokens = usage.cache_read_input_tokens || 0;
  const inputTokens = usage.input_tokens || 0;
  const outputTokens = usage.output_tokens || 0;
  const estimatedCostUsd =
    (cacheCreationTokens / 1_000_000 * 3.75) +
    (cacheReadTokens     / 1_000_000 * 0.30) +
    (inputTokens         / 1_000_000 * 3)    +
    (outputTokens        / 1_000_000 * 15);
  return { briefText, inputTokens, outputTokens, estimatedCostUsd, cacheCreationTokens, cacheReadTokens };
}

// ── Step 4: Write HubSpot note ────────────────────────────────────────────────

async function writeHubSpotNote(contactId, briefHtml, taskTitle) {
  const now = new Date();
  const formattedDate = now.toLocaleString('en-GB', {
    day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit', timeZone: 'Europe/London',
  });

  const noteBody = `<p>🔍 <strong>Pre-call Research Brief</strong> — prepared automatically by WMC Research Agent</p><hr>${briefHtml}<hr><p><em>Generated ${formattedDate} · Task: ${taskTitle}</em></p>`;

  let response;
  try {
    response = await axios.post(
      'https://api.hubapi.com/crm/v3/objects/notes',
      {
        properties: {
          hs_note_body: noteBody,
          hs_timestamp: String(now.getTime()),
        },
        associations: [{
          to: { id: contactId },
          types: [{ associationCategory: 'HUBSPOT_DEFINED', associationTypeId: 202 }],
        }],
      },
      { headers: HUBSPOT_HEADERS() }
    );
  } catch (error) {
    console.error('[Research Agent] HubSpot note creation failed:', error.response?.status, JSON.stringify(error.response?.data));
    throw error;
  }

  return response.data.id;
}

// ── Per-task processing (shared by main run and retry) ───────────────────────

async function processTask(taskId, taskTitle) {
  try {
    currentlyProcessing = { taskId, contactName: null, step: 'fetching_contact' };

    await pool.query(
      `UPDATE research_briefs SET status = 'processing', updated_at = NOW() WHERE task_id = $1`,
      [taskId]
    ).catch(() => {});

    const contact = await getTaskContact(taskId);

    if (!contact) {
      await pool.query(
        `UPDATE research_briefs SET status = 'failed', error_text = $1, updated_at = NOW() WHERE task_id = $2`,
        ['No contact associated with this task', taskId]
      );
      return;
    }

    const contactName = [contact.firstname, contact.lastname].filter(Boolean).join(' ') || 'Unknown';

    currentlyProcessing = { taskId, contactName, step: 'fetching_company' };
    const company = await getContactCompany(contact.id);
    const companyName = company?.name || contact.company || 'Unknown';
    const downloadContext = buildDownloadContext(contact, taskTitle);

    await pool.query(
      `UPDATE research_briefs
       SET contact_id = $1, contact_name = $2, company_name = $3, download_context = $4, updated_at = NOW()
       WHERE task_id = $5`,
      [contact.id, contactName, companyName, downloadContext, taskId]
    );

    currentlyProcessing = { taskId, contactName, step: 'researching' };
    const { briefText, inputTokens, outputTokens, estimatedCostUsd, cacheCreationTokens, cacheReadTokens } = await researchCompany({
      taskId,
      contactName,
      companyName,
      industry: company?.industry,
      employeeCount: company?.numberofemployees,
      city: company?.city,
      jobTitle: contact.jobtitle,
      downloadContext,
    });

    currentlyProcessing = { taskId, contactName, step: 'writing_note' };
    const noteId = await writeHubSpotNote(contact.id, briefText, taskTitle);

    await pool.query(
      `UPDATE research_briefs
       SET status = 'completed', brief_text = $1, hubspot_note_id = $2,
           input_tokens = $3, output_tokens = $4, estimated_cost_usd = $5,
           cache_creation_tokens = $6, cache_read_tokens = $7, updated_at = NOW()
       WHERE task_id = $8`,
      [briefText, noteId, inputTokens, outputTokens, estimatedCostUsd, cacheCreationTokens, cacheReadTokens, taskId]
    );
  } finally {
    currentlyProcessing = null;
  }
}

// ── Queue population ─────────────────────────────────────────────────────────

async function populateQueue(settings) {
  let tasks;
  try {
    tasks = await fetchTasksToProcess(settings);
  } catch (err) {
    console.error('[Research Agent] populateQueue: task fetch failed:', err.message);
    return { queued: 0, skipped: 0 };
  }

  const alreadyQueued = new Set();
  const { rows: existingActive } = await pool.query(
    `SELECT DISTINCT contact_id FROM research_briefs WHERE status IN ('queued', 'processing') AND contact_id IS NOT NULL`
  ).catch(() => ({ rows: [] }));
  existingActive.forEach(r => { if (r.contact_id) alreadyQueued.add(r.contact_id); });

  let queued = 0;
  let skipped = 0;

  for (const task of tasks) {
    const taskId = task.id;
    const taskTitle = task.properties.hs_task_subject || 'Untitled task';
    const taskDueDate = task.properties.hs_timestamp
      ? new Date(parseInt(task.properties.hs_timestamp))
      : null;

    const existing = await pool.query(
      'SELECT id FROM research_briefs WHERE task_id = $1', [taskId]
    ).catch(() => ({ rows: [] }));
    if (existing.rows.length) continue;

    let contact;
    try {
      contact = await getTaskContact(taskId);
    } catch (err) {
      console.error(`[Research Agent] populateQueue: contact fetch failed for ${taskId}:`, err.message);
      continue;
    }
    if (!contact) continue;

    const contactId = contact.id;
    const contactName = [contact.firstname, contact.lastname].filter(Boolean).join(' ') || 'Unknown';

    if (alreadyQueued.has(contactId)) {
      await pool.query(
        `INSERT INTO research_briefs (task_id, task_title, task_due_date, contact_id, contact_name, status, skip_reason, updated_at)
         VALUES ($1, $2, $3, $4, $5, 'skipped', 'Contact already in queue', NOW())
         ON CONFLICT (task_id) DO NOTHING`,
        [taskId, taskTitle, taskDueDate, contactId, contactName]
      ).catch(() => {});
      skipped++;
      continue;
    }

    const { rows: recentRows } = await pool.query(
      `SELECT id FROM research_briefs WHERE contact_id = $1 AND status = 'completed'
         AND created_at >= NOW() - INTERVAL '7 days' ORDER BY created_at DESC LIMIT 1`,
      [contactId]
    ).catch(() => ({ rows: [] }));

    if (recentRows.length) {
      await pool.query(
        `INSERT INTO research_briefs (task_id, task_title, task_due_date, contact_id, contact_name, status, skip_reason, previous_brief_id, updated_at)
         VALUES ($1, $2, $3, $4, $5, 'skipped', 'Duplicate within 7 days', $6, NOW())
         ON CONFLICT (task_id) DO NOTHING`,
        [taskId, taskTitle, taskDueDate, contactId, contactName, recentRows[0].id]
      ).catch(() => {});
      skipped++;
      console.log(`[Research Agent] Skipped task ${taskId} — recent brief exists for contact ${contactId}`);
      continue;
    }

    const company = await getContactCompany(contactId);
    const companyName = company?.name || contact.company || 'Unknown';
    const downloadContext = buildDownloadContext(contact, taskTitle);

    await pool.query(
      `INSERT INTO research_briefs (task_id, task_title, task_due_date, contact_id, contact_name, company_name, download_context, status, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'queued', NOW())
       ON CONFLICT (task_id) DO NOTHING`,
      [taskId, taskTitle, taskDueDate, contactId, contactName, companyName, downloadContext]
    ).catch(() => {});

    alreadyQueued.add(contactId);
    queued++;
    console.log(`[Research Agent] Queued: ${contactName} at ${companyName} (task ${taskId})`);
  }

  console.log(`[Research Agent] Queue populated — ${queued} queued, ${skipped} skipped`);
  return { queued, skipped };
}

// ── Main agent ────────────────────────────────────────────────────────────────

async function runResearchAgent() {
  const settings = await getSettings().catch(() => ({ enabled: true, task_range: 'due_today', range_days: 0 }));
  if (!settings.enabled) {
    console.log('[Research Agent] Skipped run — agent disabled');
    return;
  }

  console.log(`[Research Agent] Starting run — ${new Date().toISOString()}`);

  try {
    await populateQueue(settings);
  } catch (popErr) {
    console.error('[Research Agent] Queue population error:', popErr.message);
  }

  let processed = 0;
  try {
    const { rows: queued } = await pool.query(
      `SELECT task_id, task_title FROM research_briefs WHERE status = 'queued'`
    );

    for (const row of queued) {
      try {
        await processTask(row.task_id, row.task_title || 'Untitled task');
        processed++;
      } catch (taskErr) {
        console.error(`[Research Agent] Task ${row.task_id} failed:`, taskErr.message);
        await pool.query(
          `UPDATE research_briefs SET status = 'failed', error_text = $1, updated_at = NOW() WHERE task_id = $2`,
          [taskErr.message.slice(0, 1000), row.task_id]
        ).catch(() => {});
        processed++;
      }
    }
  } catch (outerErr) {
    console.error('[Research Agent] Run failed:', outerErr.message);
  }

  lastRun = new Date().toISOString();
  console.log(`[Research Agent] Run complete — processed ${processed} tasks`);
}

async function retryFailedBriefs() {
  console.log(`[Research Agent] Starting retry of failed briefs — ${new Date().toISOString()}`);
  let retried = 0;

  try {
    const { rows } = await pool.query(
      `SELECT task_id, task_title FROM research_briefs WHERE status = 'failed'`
    );

    console.log(`[Research Agent] Found ${rows.length} failed briefs to retry`);

    for (const row of rows) {
      await pool.query(
        `UPDATE research_briefs SET status = 'queued', error_text = NULL, updated_at = NOW() WHERE task_id = $1`,
        [row.task_id]
      );

      try {
        await processTask(row.task_id, row.task_title || 'Untitled task');
        retried++;
      } catch (taskErr) {
        console.error(`[Research Agent] Retry of task ${row.task_id} failed:`, taskErr.message);
        await pool.query(
          `UPDATE research_briefs SET status = 'failed', error_text = $1, updated_at = NOW() WHERE task_id = $2`,
          [taskErr.message.slice(0, 1000), row.task_id]
        );
        retried++;
      }
    }
  } catch (outerErr) {
    console.error('[Research Agent] Retry run failed:', outerErr.message);
  }

  console.log(`[Research Agent] Retry complete — processed ${retried} briefs`);
}

// ── Routes ────────────────────────────────────────────────────────────────────

app.get('/health', (req, res) => res.json({ status: 'ok', lastRun }));

app.get('/api/status', (req, res) => res.json(currentlyProcessing));

app.get('/api/briefs', async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT * FROM research_briefs ORDER BY created_at DESC LIMIT 50'
    );
    res.json(rows);
  } catch (err) {
    console.error('[/api/briefs]', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/briefs/:taskId', async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT * FROM research_briefs WHERE task_id = $1',
      [req.params.taskId]
    );
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error('[/api/briefs/:taskId]', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/briefs/trigger', (req, res) => {
  const auth = req.headers.authorization;
  if (!auth || auth !== `Bearer ${process.env.AGENT_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  runResearchAgent();
  res.json({ success: true, message: 'Research agent triggered' });
});

app.post('/api/briefs/retry-failed', (req, res) => {
  const auth = req.headers.authorization;
  if (!auth || auth !== `Bearer ${process.env.AGENT_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  retryFailedBriefs();
  res.json({ success: true, message: 'Retry of failed briefs triggered' });
});

app.post('/api/briefs/manual', async (req, res) => {
  const auth = req.headers.authorization;
  if (!auth || auth !== `Bearer ${process.env.AGENT_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'email is required' });

  const timestamp = Date.now();
  currentlyProcessing = { taskId: `manual-${timestamp}`, contactName: null, step: 'fetching_contact' };

  let contact;
  try {
    contact = await lookupContactByEmail(email);
  } catch (err) {
    currentlyProcessing = null;
    return res.status(500).json({ success: false, error: 'Failed to search HubSpot: ' + err.message });
  }

  if (!contact) {
    currentlyProcessing = null;
    return res.status(404).json({ error: 'No HubSpot contact found with that email' });
  }

  const contactId = contact.id;
  const contactName = [contact.firstname, contact.lastname].filter(Boolean).join(' ') || contact.email || 'Unknown';
  const taskId = `manual-${timestamp}-${contactId}`;
  const taskTitle = 'Manual research brief';

  try {
    await pool.query(
      `INSERT INTO research_briefs (task_id, task_title, status) VALUES ($1, $2, 'pending')`,
      [taskId, taskTitle]
    );
  } catch (err) {
    currentlyProcessing = null;
    return res.status(500).json({ success: false, error: 'Database error: ' + err.message });
  }

  try {
    currentlyProcessing = { taskId, contactName, step: 'fetching_company' };
    const company = await getContactCompany(contactId);
    const companyName = company?.name || contact.company || 'Unknown';

    const contextParts = [];
    if (contact.recent_conversion_event_name) contextParts.push(contact.recent_conversion_event_name);
    if (contact.hs_lead_status) contextParts.push(`Lead status: ${contact.hs_lead_status}`);
    if (contact.jobtitle) contextParts.push(`Role: ${contact.jobtitle}`);
    const downloadContext = contextParts.join(' | ') || 'General enquiry — no specific download/enquiry recorded';

    await pool.query(
      `UPDATE research_briefs SET contact_id = $1, contact_name = $2, company_name = $3, download_context = $4 WHERE task_id = $5`,
      [contactId, contactName, companyName, downloadContext, taskId]
    );

    currentlyProcessing = { taskId, contactName, step: 'researching' };
    const { briefText, inputTokens, outputTokens, estimatedCostUsd, cacheCreationTokens, cacheReadTokens } = await researchCompany({
      taskId,
      contactName,
      companyName,
      industry: company?.industry,
      employeeCount: company?.numberofemployees,
      city: company?.city,
      jobTitle: contact.jobtitle,
      downloadContext,
    });

    currentlyProcessing = { taskId, contactName, step: 'writing_note' };
    const noteId = await writeHubSpotNote(contactId, briefText, taskTitle);

    await pool.query(
      `UPDATE research_briefs
       SET status = 'completed', brief_text = $1, hubspot_note_id = $2,
           input_tokens = $3, output_tokens = $4, estimated_cost_usd = $5,
           cache_creation_tokens = $6, cache_read_tokens = $7, updated_at = NOW()
       WHERE task_id = $8`,
      [briefText, noteId, inputTokens, outputTokens, estimatedCostUsd, cacheCreationTokens, cacheReadTokens, taskId]
    );

    const { rows } = await pool.query('SELECT id FROM research_briefs WHERE task_id = $1', [taskId]);
    res.json({ success: true, briefId: rows[0]?.id, contactName, companyName });
  } catch (err) {
    console.error(`[Research Agent] Manual brief for ${email} failed:`, err.message);
    await pool.query(
      `UPDATE research_briefs SET status = 'failed', error_text = $1 WHERE task_id = $2`,
      [err.message.slice(0, 1000), taskId]
    ).catch(() => {});
    res.status(500).json({ success: false, error: err.message });
  } finally {
    currentlyProcessing = null;
  }
});

app.get('/api/settings', async (req, res) => {
  try {
    res.json(await getSettings());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/settings', async (req, res) => {
  const auth = req.headers.authorization;
  if (!auth || auth !== `Bearer ${process.env.AGENT_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const { enabled, task_range, range_days } = req.body;
  const validRanges = ['due_today', 'overdue_by', 'due_within'];
  if (task_range !== undefined && !validRanges.includes(task_range)) {
    return res.status(400).json({ error: 'Invalid task_range' });
  }
  try {
    await pool.query(
      `UPDATE agent_settings SET
         enabled    = COALESCE($1, enabled),
         task_range = COALESCE($2, task_range),
         range_days = COALESCE($3, range_days),
         updated_at = NOW()
       WHERE id = 1`,
      [enabled ?? null, task_range ?? null, range_days ?? null]
    );
    res.json(await getSettings());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/settings/preview', async (req, res) => {
  const auth = req.headers.authorization;
  if (!auth || auth !== `Bearer ${process.env.AGENT_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const { task_range, range_days } = req.body;
  try {
    const tasks = await fetchTasksToProcess({ task_range: task_range || 'due_today', range_days: range_days || 0 });
    res.json({ count: tasks.length });
  } catch (err) {
    console.error('[Research Agent] Settings preview failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/costs', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) AS total_briefs,
        COALESCE(SUM(estimated_cost_usd), 0) AS total_cost_usd,
        COALESCE(AVG(estimated_cost_usd) FILTER (WHERE estimated_cost_usd IS NOT NULL), 0) AS avg_cost_per_brief,
        COALESCE(SUM(estimated_cost_usd) FILTER (WHERE created_at >= CURRENT_DATE), 0) AS today_cost_usd,
        COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE) AS today_briefs
      FROM research_briefs
    `);
    const r = rows[0];
    res.json({
      totalBriefs:      parseInt(r.total_briefs) || 0,
      totalCostUsd:     parseFloat(r.total_cost_usd) || 0,
      avgCostPerBrief:  parseFloat(r.avg_cost_per_brief) || 0,
      todayCostUsd:     parseFloat(r.today_cost_usd) || 0,
      todayBriefs:      parseInt(r.today_briefs) || 0,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/queue', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT rb.id, rb.task_id, rb.contact_id, rb.contact_name, rb.company_name,
             rb.task_title, rb.task_due_date, rb.download_context, rb.status,
             rb.estimated_cost_usd, rb.error_text, rb.skip_reason, rb.previous_brief_id,
             rb.hubspot_note_id, rb.created_at, rb.updated_at,
             pb.created_at AS prev_brief_created_at
      FROM research_briefs rb
      LEFT JOIN research_briefs pb ON rb.previous_brief_id = pb.id
      ORDER BY rb.created_at DESC
      LIMIT 300
    `);
    const grouped = { queued: [], processing: [], completed: [], failed: [], skipped: [] };
    for (const row of rows) {
      if (grouped[row.status]) grouped[row.status].push(row);
    }
    res.json(grouped);
  } catch (err) {
    console.error('[/api/queue]', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/queue/:id', async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT * FROM research_briefs WHERE id = $1',
      [req.params.id]
    );
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/queue/populate', async (req, res) => {
  const auth = req.headers.authorization;
  if (!auth || auth !== `Bearer ${process.env.AGENT_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const settings = await getSettings();
    const result = await populateQueue(settings);
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[Research Agent] Queue populate failed:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Scheduler ─────────────────────────────────────────────────────────────────

cron.schedule('0 * * * *', runResearchAgent);

// ── Start ─────────────────────────────────────────────────────────────────────

initDb()
  .then(() => {
    app.listen(PORT, () => console.log(`WMC Research Agent running on port ${PORT}`));
    setTimeout(runResearchAgent, 5000);
  })
  .catch(err => { console.error('[initDb] Fatal:', err); process.exit(1); });
