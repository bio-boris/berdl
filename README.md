# berdl

A **JupyterLite** environment for exploring the [KBase MCP API](https://hub.berdl.kbase.us/apis/mcp/docs) interactively in your browser — no local Python installation required.

## 🚀 Live site (GitHub Pages)

Once deployed, the JupyterLite environment is available at:

```
https://<your-github-username>.github.io/berdl/
```

Open `KBase_MCP_API_Explorer.ipynb` from the file browser to get started.

---

## Features

- **Token input widget** – paste your KBase token securely (masked password field).
- **OpenAPI spec loader** – fetches and parses the live OpenAPI spec from the MCP API.
- **Endpoint browser** – renders a colour-coded table of all available API routes.
- **Interactive caller** – pick an endpoint, fill in parameters/body, and fire requests directly from the notebook.
- **Raw helper cell** – a bare-bones code cell for quick one-off calls.
- **Runs entirely in-browser** – powered by [Pyodide](https://pyodide.org/) via JupyterLite; no server needed.

---

## Enabling GitHub Pages

1. Go to **Settings → Pages** in this repository.
2. Under **Source**, select **GitHub Actions**.
3. Push a commit to `main` (or trigger the workflow manually under **Actions → Build and Deploy JupyterLite to GitHub Pages → Run workflow**).
4. After the workflow succeeds, your site is live at the URL shown in the deployment step.

---

## Local development

```bash
# Create a virtual environment (optional but recommended)
python -m venv .venv && source .venv/bin/activate

# Install build dependencies
pip install -r requirements.txt

# Build the site locally
jupyter lite build --contents content --output-dir _output

# Serve locally (optional)
jupyter lite serve --contents content
```

Then open <http://localhost:8000> in your browser.

---

## Repository structure

```
berdl/
├── content/
│   └── KBase_MCP_API_Explorer.ipynb   # Main interactive notebook
├── .github/
│   └── workflows/
│       └── deploy.yml                 # Build + deploy to GitHub Pages
├── jupyter_lite_config.json           # JupyterLite build configuration
├── requirements.txt                   # Build-time Python dependencies
└── README.md
```
