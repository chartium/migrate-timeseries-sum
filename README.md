# timeseries-migrate-default
A script to migrate from legacy 'default' sum variants to the current format.

1. Install [Bun](https://bun.sh/):
   * Linux/macOS: `curl -fsSL https://bun.sh/install | bash`
   * Windows: `powershell -c "irm bun.sh/install.ps1 | iex"`
2. Download `migrate.ts`
3. In terminal, execute: `bun run ./migrate.ts /path/to/file.db`
