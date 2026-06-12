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
  console.log('[Research Agent] Database initialised');
}

// ── Step 1: Fetch today's tasks ───────────────────────────────────────────────

async function fetchTodaysTasks() {
  const startOfDay = new Date();
  startOfDay.setUTCHours(0, 0, 0, 0);
  const endOfDay = new Date();
  endOfDay.setUTCHours(23, 59, 59, 999);

  let resp;
  try {
    resp = await axios.post(
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

async function researchCompany({ taskId, contactName, companyName, industry, employeeCount, city, jobTitle, downloadContext }) {
  const systemPrompt = `You are a research assistant for The Workplace Mindfulness Co., a UK-based workplace wellbeing training company. Your job is to prepare a detailed, genuinely useful pre-call research brief before a sales call.

We deliver bespoke mental health and wellbeing training and programmes to organisations. Our core offerings include Mental Health Awareness, Mental Health First Aider (MHFA) training, MHFA Refresher, and wellbeing programmes covering resilience, menopause, neurodiversity and more.

## Research process — run all of these searches before writing

Work through these searches in order. Do not skip any. Try variations if a search returns little.

1. Company news: search "[company name] news 2024 2025" then "[company name] funding growth restructuring layoffs"
2. Company LinkedIn: search "[company name] LinkedIn" — look for their company page, recent posts about culture, hiring, wellbeing, events
3. HR and wellbeing signals: search "[company name] mental health wellbeing EAP HR" and "[company name] careers hiring people"
4. Contact person: search "[contact name] [company name] LinkedIn" then "[contact name] [company name]" — find their role, tenure, background, priorities

## Output format — HTML only

Output ONLY valid HTML. No markdown, no asterisks, no preamble, no explanation outside the tags. Use exactly this structure:

<h3>🏢 Company Snapshot</h3>
<p>[2–3 sentences: what the company does, approximate size, sector, UK/international locations, recent trajectory — growing, stable, restructuring, or in flux]</p>

<h3>📰 Recent News &amp; Signals</h3>
<ul>
  <li>[Specific verifiable fact found via search — e.g. raised Series B funding Jan 2025, opened Manchester office, acquired by X, named in Sunday Times Best Companies 2024]</li>
  <li>[Second specific fact — e.g. CEO announced 150 redundancies March 2025, rebranded after merger]</li>
  <li>[Wellbeing or HR signal if found — e.g. recently appointed a Head of People &amp; Culture, LinkedIn posts about mental health awareness week, signed the Time to Change pledge]</li>
</ul>

<h3>👤 About ${contactName || 'the Contact'}</h3>
<p>[Their role and what it implies about their priorities. Any background found: previous companies, tenure, LinkedIn presence. If little is publicly available, be honest: "Limited public profile found — their [role] suggests they are likely responsible for [reasonable inference based on role]."]</p>

<h3>💡 Recommended Angle</h3>
<p><strong>[One specific, actionable approach for this call — directly connect what they downloaded or enquired about with something concrete discovered in research. Make it specific to this company and contact, not generic. E.g. "Given their recent 40% headcount growth and the contact's HR Business Partner role, lead with the manager mental health training angle — fast-scaling teams consistently produce first-time managers who lack the skills and confidence to handle wellbeing conversations."]</strong></p>

## Rules
- Every fact in Recent News &amp; Signals must be something you found via search — do not invent or assume
- If a section genuinely has nothing after trying multiple searches, write exactly: <em>No specific information found.</em>
- Do not pad with generic industry statements — one real fact is worth more than three generic ones
- The Recommended Angle must be tailored to this specific company and contact
- Output nothing outside the four HTML sections above`;

  const userMessage = `Company name: ${companyName}
Industry: ${industry || 'Not specified'}
Approximate headcount: ${employeeCount || 'Unknown'}
Location: ${city || 'Unknown'}
Contact name: ${contactName || 'Unknown'}
Contact role: ${jobTitle || 'Unknown'}
What they downloaded or enquired about: ${downloadContext}

Research this company and contact thoroughly using web search, then produce the HTML brief.`;

  console.log('[Research Agent] API key present:', !!process.env.ANTHROPIC_API_KEY);
  console.log('[Research Agent] API key length:', process.env.ANTHROPIC_API_KEY?.length);
  console.log('[Research Agent] API key prefix:', process.env.ANTHROPIC_API_KEY?.substring(0, 15));
  console.log('[Research Agent] API key suffix:', process.env.ANTHROPIC_API_KEY?.substring(process.env.ANTHROPIC_API_KEY.length - 5));

  let response;
  try {
    response = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 4096,
      tools: [{
        type: 'web_search_20250305',
        name: 'web_search',
        max_uses: 8,
      }],
      system: systemPrompt,
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
  return textBlock ? textBlock.text.trim() : '<p>Research could not be completed.</p>';
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

    const contact = await getTaskContact(taskId);

    if (!contact) {
      await pool.query(
        `UPDATE research_briefs SET status = 'failed', error_text = $1 WHERE task_id = $2`,
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
       SET contact_id = $1, contact_name = $2, company_name = $3, download_context = $4
       WHERE task_id = $5`,
      [contact.id, contactName, companyName, downloadContext, taskId]
    );

    currentlyProcessing = { taskId, contactName, step: 'researching' };
    const briefText = await researchCompany({
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
       SET status = 'completed', brief_text = $1, hubspot_note_id = $2
       WHERE task_id = $3`,
      [briefText, noteId, taskId]
    );
  } finally {
    currentlyProcessing = null;
  }
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
        await processTask(taskId, taskTitle);
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
        `UPDATE research_briefs SET status = 'pending', error_text = NULL WHERE task_id = $1`,
        [row.task_id]
      );

      try {
        await processTask(row.task_id, row.task_title || 'Untitled task');
        retried++;
      } catch (taskErr) {
        console.error(`[Research Agent] Retry of task ${row.task_id} failed:`, taskErr.message);
        await pool.query(
          `UPDATE research_briefs SET status = 'failed', error_text = $1 WHERE task_id = $2`,
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

// ── Scheduler ─────────────────────────────────────────────────────────────────

cron.schedule('0 * * * *', runResearchAgent);

// ── Start ─────────────────────────────────────────────────────────────────────

initDb()
  .then(() => {
    app.listen(PORT, () => console.log(`WMC Research Agent running on port ${PORT}`));
    setTimeout(runResearchAgent, 5000);
  })
  .catch(err => { console.error('[initDb] Fatal:', err); process.exit(1); });
