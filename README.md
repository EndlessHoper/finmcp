# finmcp

MCP server that exposes `yfinance` tools over stdio.

## Install

```bash
npm install -g finmcp
# or
npx -y finmcp
```

Python dependencies (required):

```bash
pip install -r mcp/python/requirements.txt
```

## Claude Code

```bash
claude mcp add finmcp -- npx -y finmcp
```

## OpenCode

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "finmcp": {
      "type": "local",
      "command": ["npx", "-y", "finmcp"],
      "enabled": true
    }
  }
}
```

## Notes
- Requires Python 3.10+ with `yfinance` installed.
- Server logs go to stderr; stdout is reserved for MCP transport.
