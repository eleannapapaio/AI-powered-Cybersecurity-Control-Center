# SOC-AI Log Pipeline
 
> **Real-Time AI-Powered Security Log Analysis Framework**
 
---
 
## 🚀 Quick Start — Just Want to Use It? Start Here.
 
> **This section is for you if you just want to plug in your devices and start analysing logs. No need to read anything else first.**
 
### What this does (in plain English)
 
You point your firewall, router, or any network device at this system. It automatically reads the logs, uses AI to understand them, and gives you a chat interface where you can ask things like *"show me failed login attempts"* or *"is there anything suspicious?"* — and get a real-time structured answer.
 
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
# OPENSEARCH_PASSWORD=your-password-here
```
 
**Remove the `#` and fill in your values:**
 
```env
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
OPENSEARCH_PASSWORD=MyStrongPassword123!
```
 
> ⚠️ `OPENAI_API_KEY` is your private OpenAI secret key — never share it or commit it to a public repository.
>
> ⚠️ `OPENSEARCH_PASSWORD` protects your log database. It will be issued to you by us. Otherwise, pick something strong. You will use it to log into the OpenSearch Dashboards at `http://localhost:5601` with username `admin`.
 
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