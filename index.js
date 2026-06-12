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
  console.log('[Research Agent] Database initialised');
}

// ── Step 1: Fetch today's tasks ───────────────────────────────────────────────

async function fetchTodaysTasks() {
  const startOfDay = new Date();
  startOfDay.setUTCHours(0, 0, 0, 0);
  const endOfDay = new Date();
  endOfDay.setUTCHours(23, 59, 59, 999);

  const resp = await axios.post(
    'https://api.hubapi.com/crm/v3/objects/tasks/search',
    {
      filterGroups: [{
        filters: [
          { propertyName: 'hs_task_status', operator: 'EQ', value: 'NOT_STARTED' },
          { propertyName: 'hubspot_owner_id', operator: 'EQ', value: HUBSPOT_OWNER_ID },
          { propertyName: 'hs_timestamp', operator: 'GTE', value: String(startOfDay.getTime()) },
          { propertyName: 'hs_timestamp', operator: 'LTE', value: String(endOfDay.getTime()) },
        ],
      }],
      properties: ['hs_task_subject', 'hubspot_owner_id', 'hs_timestamp', 'hs_task_status'],
      limit: 100,
    },
    { headers: HUBSPOT_HEADERS() }
  );

  return resp.data.results || [];
}

// ── Step 2: Get contact and company ──────────────────────────────────────────

async function getTaskContact(taskId) {
  const assocResp = await axios.get(
    `https://api.hubapi.com/crm/v4/objects/tasks/${taskId}/associations/contacts`,
    { headers: HUBSPOT_HEADERS() }
  );

  const results = assocResp.data.results || [];
  if (!results.length) return null;

  const contactId = String(results[0].toObjectId);

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
}

async function getContactCompany(contactId) {
  try {
    const assocResp = await axios.get(
      `https://api.hubapi.com/crm/v4/objects/contacts/${contactId}/associations/companies`,
      { headers: HUBSPOT_HEADERS() }
    );

    const results = assocResp.data.results || [];
    if (!results.length) return null;

    const companyId = String(results[0].toObjectId);

    const companyResp = await axios.get(
      `https://api.hubapi.com/crm/v3/objects/companies/${companyId}`,
      {
        params: { properties: 'name,industry,numberofemployees,city' },
        headers: HUBSPOT_HEADERS(),
      }
    );

    return companyResp.data.properties;
  } catch (_) {
    return null;
  }
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

async function researchCompany({ companyName, industry, employeeCount, city, jobTitle, downloadContext }) {
  const systemPrompt = `You are a research assistant for The Workplace Mindfulness Co., a UK-based workplace wellbeing training company. Your job is to research a company before a sales call and produce a concise, numbered research brief that will help our team have a personalised, informed conversation.

We deliver bespoke mental health and wellbeing training, programmes and ongoing support to organisations. Our core offerings include Mental Health Awareness, Mental Health First Aider training, MHFA Refresher, and wellbeing programmes covering resilience, menopause, neurodiversity and more.

Research the company provided and produce a brief of 4-6 numbered points. Each point should be a specific, relevant fact about the company that gives context for why they might benefit from our services or what their current situation is. Focus on:
- Recent growth, funding rounds, restructuring or headcount changes
- Any recent news about employee wellbeing, mental health, HR initiatives
- Industry context relevant to workplace wellbeing
- Size, locations, type of workforce
- Any signals that suggest why they downloaded our guide or enquired about our courses

Be specific and factual. Only include things you can verify. Do not pad with generic statements. Each point should be genuinely useful for a sales conversation.

End the brief with one sentence suggesting the most relevant angle for the call based on what they downloaded or enquired about.`;

  const userMessage = `Company name: ${companyName}
Industry: ${industry || 'Unknown'}
Approximate size: ${employeeCount || 'Unknown'} employees
Location: ${city || 'Unknown'}
Contact role: ${jobTitle || 'Unknown'}
What they downloaded or enquired about: ${downloadContext}

Please research this company and produce the numbered brief.`;

  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 2000,
    tools: [{
      type: 'web_search_20250305',
      name: 'web_search',
      max_uses: 5,
    }],
    system: systemPrompt,
    messages: [{ role: 'user', content: userMessage }],
  });

  const textBlock = response.content.findLast(b => b.type === 'text');
  return textBlock ? textBlock.text.trim() : 'Research could not be completed.';
}

// ── Step 4: Write HubSpot note ────────────────────────────────────────────────

async function writeHubSpotNote(contactId, briefText, taskTitle) {
  const now = new Date();
  const datetime = now.toLocaleString('en-GB', {
    day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit', timeZone: 'Europe/London',
  });

  const noteBody = `🔍 Pre-call Research Brief\n\nPrepared automatically by WMC Research Agent\n\n${briefText}\n\n---\nGenerated ${datetime} · Task: ${taskTitle}`;

  const response = await axios.post(
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

  return response.data.id;
}

// ── Main agent ────────────────────────────────────────────────────────────────

async function runResearchAgent() {
  console.log(`[Research Agent] Starting run — ${new Date().toISOString()}`);
  let processed = 0;

  try {
    const tasks = await fetchTodaysTasks();

    for (const task of tasks) {
      const taskId = task.id;
      const taskTitle = task.properties.hs_task_subject || 'Untitled task';
      const taskDueDate = task.properties.hs_timestamp
        ? new Date(parseInt(task.properties.hs_timestamp))
        : null;

      const existing = await pool.query(
        'SELECT id FROM research_briefs WHERE task_id = $1',
        [taskId]
      );
      if (existing.rows.length) continue;

      await pool.query(
        `INSERT INTO research_briefs (task_id, task_title, task_due_date, status)
         VALUES ($1, $2, $3, 'pending')
         ON CONFLICT (task_id) DO NOTHING`,
        [taskId, taskTitle, taskDueDate]
      );

      try {
        const contact = await getTaskContact(taskId);

        if (!contact) {
          await pool.query(
            `UPDATE research_briefs SET status = 'failed', error_text = $1 WHERE task_id = $2`,
            ['No contact associated with this task', taskId]
          );
          processed++;
          continue;
        }

        const contactName = [contact.firstname, contact.lastname].filter(Boolean).join(' ') || 'Unknown';
        const company = await getContactCompany(contact.id);
        const companyName = company?.name || contact.company || 'Unknown';
        const downloadContext = buildDownloadContext(contact, taskTitle);

        await pool.query(
          `UPDATE research_briefs
           SET contact_id = $1, contact_name = $2, company_name = $3, download_context = $4
           WHERE task_id = $5`,
          [contact.id, contactName, companyName, downloadContext, taskId]
        );

        const briefText = await researchCompany({
          companyName,
          industry: company?.industry,
          employeeCount: company?.numberofemployees,
          city: company?.city,
          jobTitle: contact.jobtitle,
          downloadContext,
        });

        const noteId = await writeHubSpotNote(contact.id, briefText, taskTitle);

        await pool.query(
          `UPDATE research_briefs
           SET status = 'completed', brief_text = $1, hubspot_note_id = $2
           WHERE task_id = $3`,
          [briefText, noteId, taskId]
        );

        processed++;
      } catch (taskErr) {
        console.error(`[Research Agent] Task ${taskId} failed:`, taskErr.message);
        await pool.query(
          `UPDATE research_briefs SET status = 'failed', error_text = $1 WHERE task_id = $2`,
          [taskErr.message.slice(0, 1000), taskId]
        );
        processed++;
      }
    }
  } catch (outerErr) {
    console.error('[Research Agent] Run failed:', outerErr.message);
  }

  lastRun = new Date().toISOString();
  console.log(`[Research Agent] Run complete — processed ${processed} tasks`);
}

// ── Routes ────────────────────────────────────────────────────────────────────

app.get('/health', (req, res) => res.json({ status: 'ok', lastRun }));

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

// ── Scheduler ─────────────────────────────────────────────────────────────────

cron.schedule('0 * * * *', runResearchAgent);

// ── Start ─────────────────────────────────────────────────────────────────────

initDb()
  .then(() => {
    app.listen(PORT, () => console.log(`WMC Research Agent running on port ${PORT}`));
    setTimeout(runResearchAgent, 5000);
  })
  .catch(err => { console.error('[initDb] Fatal:', err); process.exit(1); });
