# SOC-AI Log Pipeline
 
> **Real-Time AI-Powered Security Log Analysis Framework**
 
---
 
## 🚀 Quick Start — Just Want to Use It? Start Here.
 
> **This section is for you if you just want to plug in your devices and start analysing logs. No need to read anything else first.**
 
### What this does (in plain English)
 
You point your firewall, router, or any network device at this system — including home routers such as Zyxel, TP-Link, ASUS, or any device that supports syslog. It automatically reads the logs, uses AI to understand them, and gives you a chat interface where you can ask things like *"show me failed login attempts"* or *"is there anything suspicious?"* — and get a real-time structured answer.

> 💡 **Multiple devices are fully supported.** You can connect as many devices as you like at the same time — each one sends logs to its own dedicated port and is tracked independently. All logs flow into a single searchable database so you can query across all your devices at once.
 
---
 
### ✅ Step 1 — Things you need before starting
 
- 🐳 **Docker Desktop** installed and running → [download here](https://www.docker.com/products/docker-desktop/)
- 🔑 **An OpenAI API key** → Use the API key provided to you or [get one here](https://platform.openai.com/api-keys) (requires access to `gpt-4.1`)
- 💾 At least **8 GB of free RAM**
- Your network device's syslog feature enabled (you will configure where it sends logs in Step 4)
 
---
 
### 🔑 Step 2 — Set your secret keys
 
In the root of the project folder you will find a file called **`.env`**. Open it in any text editor (Notepad, VS Code, etc.).
 
You will see lines that look like this — **commented out** with a `#`:
 
```
# OPENAI_API_KEY=put your open ai api key here
# OPENSEARCH_PASSWORD=put-your-OPENSEARCH_PASSWORD-here
```
 
**Remove the `#` and fill in your values:**
 
```env
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
OPENSEARCH_PASSWORD=MyStrongPassword123!
```

The file also contains a pre-set default you do not need to change:
 
```env
OPENAI_MODEL=gpt-4.1
```
 
> ⚠️ `OPENAI_API_KEY` is your private OpenAI secret key — never share it or commit it to a public repository.
>
> ⚠️ `OPENSEARCH_PASSWORD` protects your log database. Pick something strong. You will use it to log into the OpenSearch Dashboards at `http://localhost:5601` with username `admin`.
 
**Save the file.**
 
---
 
### 🐳 Step 3 — Start the system
 
Open a terminal in the project folder and run:

```bash
cd otel-langgraph-pipeline
```
 
Then run:

```bash
docker compose up -d
```
 
Wait about **60–90 seconds** for everything to start. Don't worry if it takes more, this happens because it is the first time running the code. Then check:
 
```bash
docker compose ps
```
 
All services should show **`running`** or **`healthy`**. OpenSearch takes the longest — if it's still starting, just wait a bit longer.
 
 The services that start are:
 
| Service | What it does |
|---|---|
| `kafka` | Message broker — buffers raw log data between services |
| `kafka-ui` | Web UI for monitoring Kafka message traffic |
| `fluent-bit` | Listens on all syslog UDP ports and forwards logs |
| `producer` | Receives logs from Fluent Bit and batches them into Kafka |
| `consumer` | Reads from Kafka, parses logs with AI, writes to OpenSearch |
| `opensearch` | Log database — stores and indexes all validated log entries |
| `opensearch-dashboards` | Visual log explorer UI |
| `chatbot` | The SOC AI chatbot — your main interface |
---
 
### 📡 Step 4 — Connect your devices
 
Find your machine's **local IP address** (e.g. `192.168.1.50`). On Windows run `ipconfig`, on Mac/Linux run `ifconfig` or `ip a`. It is the **Default Gateway** under the **Wireless LAN adapter Wi-fi**.
 
Configure your device's syslog output to send UDP traffic to your machine's IP on the port that matches your device:
 
| Port | Device / Brand |
|---|---|
| **514** | ⬅️ Any generic device, or if you're not sure — use this |
| **1514** | Cisco ASA, ASAv, FTD |
| **1515** | Cisco IOS, IOS-XE, IOS-XR (routers / switches) |
| **1516** | Cisco Firepower / FMC |
| **1517** | Cisco Stealthwatch |
| **1518** | Fortinet FortiGate |
| **1519** | Fortinet FortiWeb (WAF) |
| **1520** | Palo Alto PAN-OS (traffic logs) |
| **1521** | Palo Alto PAN-OS (threat logs) |
| **1522** | Juniper SRX / JunOS |
| **1523** | Check Point Firewall-1 / R80+ |
| **1524** | Sophos XG / SFOS |
| **1525** | pfSense / OPNsense |
| **1526** | WatchGuard XTM / Firebox |
| **1527** | Untangle NG / Arista |
| **1528** | Snort / Suricata |
| **1529** | Zeek / Bro |
| **1530** | IPFire / Smoothwall / iptables-based |
| **1531** | Zyxel VMG / NBG / USG |
 
**Quick config examples by vendor:**
 
**Cisco ASA** — paste into your ASA CLI:
```
logging enable
logging host <interface-name> <YOUR_HOST_IP> udp/1514
logging trap informational
```
 
**Fortinet FortiGate** — go to *Log & Report → Log Settings → Remote Logging*:
- Server: <YOUR_HOST_IP>`
- Port: `1518`
- Protocol: UDP
 
**Palo Alto PAN-OS** — go to *Device → Server Profiles → Syslog*, create a profile:
- Server IP: <YOUR_HOST_IP>
- Port: `1520` (traffic) or `1521` (threat)
- Transport: UDP
 
**pfSense / OPNsense** — go to *Status → System Logs → Settings*:
- Enable Remote Logging
- Remote log server: <YOUR_HOST_IP>
 
**Juniper SRX** — in the CLI:
```
set system syslog host <YOUR_HOST_IP> any any
set system syslog host <YOUR_HOST_IP> port 1522
```
 
**Check Point** — in SmartConsole under *Logs & Monitor → Log Servers*, add a new syslog target:
- IP: <YOUR_HOST_IP>
- Port: `1523`
- Protocol: UDP
 
**Sophos XG** — go to *System → Administration → Notification Settings → Syslog Server*:
- IP Address: <YOUR_HOST_IP>
- Port: `1524`
- Facility: Any
 
> 💡 **Not sure which port?** Just use **port 514** — it accepts any standard syslog format and works as a universal fallback.
>
> 🏠 **Home router?** Consumer routers (Zyxel, TP-Link, ASUS, Netgear, etc.) are fully supported. Use port **1531** for Zyxel VMG/NBG/USG devices, or port **514** for any other home router brand.
>
> 🔀 **Multiple devices at once?** Yes — connect as many as you like simultaneously. Each device sends to its own port and is labelled independently in the database. You can query one device or all of them at the same time from the chatbot.
 
---
 
### 💬 Step 5 — Open the SOC Chatbot and start asking questions
 
This is the main product. Once your devices are sending logs, open your browser and go to:
 
> ## 🤖 http://localhost:8000
 
This is the **SOC AI Chatbot** — your analyst interface. Type any question about your network in plain English and the system will search through your device logs and give you a structured, AI-generated answer.
 
**Example questions to try:**
 
| Question | What happens |
|---|---|
| `Show me the latest error logs` | Returns a table of recent ERROR-level events |
| `Are there any failed login attempts?` | Searches for authentication failures |
| `Show me all traffic from IP 10.0.0.5` | Filters logs by a specific source IP |
| `Is there anything suspicious in the last hour?` | Runs a full correlation and threat pattern analysis |
| `How do I block the IP causing the most errors?` | Returns step-by-step remediation instructions |
| `How many logs are there?` | Counts all indexed entries |
| `Show me logs from the firewall service` | Filters by a specific service name |
 
---
 
### 🔎 Optional — Additional monitoring interfaces
 
These are secondary tools for inspecting what's happening under the hood. You don't need them to use the chatbot.
 
| URL | What it is |
|---|---|
| 📊 `http://localhost:5601` | **OpenSearch Dashboards** — visual log explorer and raw search UI (login: `admin` / your `OPENSEARCH_PASSWORD`) |
| 🛠️ `http://localhost:8081` | **Kafka UI** — monitor raw message traffic between services |
 
---
 
### 🛑 Stopping the system
 
```bash
docker compose down
```
 
Your indexed logs are saved in the `outputs/` folder and in the OpenSearch volume — they persist between restarts.
 
---
 
*For the full analytical guide including architecture diagrams, detailed field tables, and extended analysis, see `SOC_AI_Pipeline_Guide.docx` included in this repository.*

---
 
## 🏗️ Architecture
 
### How data flows
 
```
Network Device (syslog UDP)
        │
        ▼
  Fluent Bit  ←── parses + enriches with vendor/device metadata
        │
        ▼  HTTP POST /fluent-bit
   Producer   ←── batches logs (50 per batch, or every 10s)
        │
        ▼  Kafka topic: raw-logs
   Consumer   ←── LangGraph AI pipeline
        │          • start_node     — enriches with ingestion timestamp
        │          • chat_prompt_node — LLM parses raw logs to schema
        │          • conditional_edge — Pydantic validates output
        │          • tools_node     — retries failed entries (up to 3×)
        │          • end_node       — writes to OpenSearch + output files
        │
        ├──► OpenSearch index: otel-logs-validated
        │
        └──► DLQ topic: raw-logs-dlq  (unrecoverable entries)
```
 
### Chatbot query flow
 
```
User question (HTTP POST /chat)
        │
        ▼
  intent_node  ←── classifies question into one of:
        │           retrieval | correlation | remediation
        │           context_aware | off_topic
        │
        ├── off_topic ──► off_topic_node ──► answer
        │
        ├── context_aware ──► analyst_node ──► answer
        │                     (uses chat history, no DB query)
        │
        └── retrieval / correlation / remediation
                │
                ▼
         query_gen_node  ←── generates OpenSearch DSL
                │
                ▼
         opensearch_node ←── executes query (no LLM)
                │
                ▼
          analyst_node   ←── LLM reasons over results
                │
                ▼
             answer
```
 
---
 
## 📁 Output files
 
The consumer writes two files to the `outputs/` folder on your host machine. These persist between restarts.
 
| File | Contents |
|---|---|
| `outputs/validated_logs.txt` | All successfully parsed and validated log entries in JSON format |
| `outputs/invalid_logs_buffer.txt` | Entries that failed Pydantic validation after all retries, including the raw log, last LLM output, and validation error |
 
Entries that land in `invalid_logs_buffer.txt` are also published to the Kafka dead-letter queue (`raw-logs-dlq`) for potential reprocessing.
 
---
 
## 🔌 Chatbot API
 
The chatbot exposes a REST API at `http://localhost:8000` that the web UI uses. You can also call it directly.
 
### `POST /chat`
 
Send a question and receive an AI-generated answer.
 
**Request body:**
```json
{
  "question": "Show me failed login attempts",
  "session_id": "optional-session-id"
}
```
 
**Response:**
```json
{
  "session_id": "abc-123",
  "answer": "...",
  "intent": "retrieval",
  "results_count": 12
}
```
 
Omit `session_id` to start a new conversation. Include the same `session_id` in follow-up messages to maintain conversation history (the chatbot remembers what was already shown and can answer follow-up questions without re-querying the database).
 
### `GET /history/{session_id}`
 
Retrieve the full conversation history for a session.
 
### `DELETE /history/{session_id}`
 
Clear the conversation history for a session.
 
### `GET /health`
 
Returns `{"status": "ok"}` when the service is running.
 
---
 
## ⚙️ Configuration reference
 
All configuration is done via the `.env` file and passed to containers through `docker-compose.yml`. The defaults below work out of the box — only `OPENAI_API_KEY` and `OPENSEARCH_PASSWORD` are required.
 
| Variable | Default | Description |
|---|---|---|
| `OPENAI_API_KEY` | *(required)* | Your OpenAI API key |
| `OPENSEARCH_PASSWORD` | *(required)* | Password for the OpenSearch `admin` user |
| `OPENAI_MODEL` | `gpt-4.1` | OpenAI model used for log parsing and chatbot |
| `BATCH_SIZE` | `50` | Number of log entries per Kafka message |
| `FLUSH_INTERVAL` | `10` | Seconds before a partial batch is flushed |
| `BATCH_POLL_INTERVAL` | `60` | Seconds the consumer waits between Kafka polls |
| `ENVIRONMENT` | `lab` | Stamped on every log record (`lab` or `production`) |