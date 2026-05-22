/**
 * Optional Jest setup file that assembles AUDIT_PG_URL from environment
 * variables (or a password file) before any module import.
 *
 * Use this if you'd rather keep your audit cluster password in a file
 * outside your shell history than export AUDIT_PG_URL by hand every
 * `npm test` invocation. Wire it via:
 *
 *   node --experimental-vm-modules node_modules/jest/bin/jest.js \
 *     --setupFiles=./src/__tests__/audit/audit-env-setup.cjs
 *
 * Or add a script in package.json:
 *
 *   "test:audit": "node --experimental-vm-modules node_modules/jest/bin/jest.js
 *                  --setupFiles=./src/__tests__/audit/audit-env-setup.cjs"
 *
 * Behavior:
 *
 *   1. If AUDIT_PG_URL is already set, do nothing.
 *   2. Otherwise, if AUDIT_PG_PWFILE points to a readable file, read
 *      the password from it (trimmed) and assemble a URL using
 *      AUDIT_PG_HOST / AUDIT_PG_PORT / AUDIT_PG_USER / AUDIT_PG_DATABASE
 *      (defaults: 127.0.0.1, 5432, postgres, postgres).
 *   3. On any failure, do nothing — describeIntegration will skip the
 *      audit suite cleanly.
 *
 * Environment variables read:
 *
 *   AUDIT_PG_URL       Full connection URL. If set, used as-is.
 *   AUDIT_PG_PWFILE    Absolute path to a file containing the password.
 *   AUDIT_PG_HOST      Host. Default: 127.0.0.1
 *   AUDIT_PG_PORT      Port. Default: 5432
 *   AUDIT_PG_USER      User. Default: postgres
 *   AUDIT_PG_DATABASE  Database. Default: postgres
 *
 * This file is dev-only and not referenced by the main test script.
 * It exists for contributors who want a single-config-knob workflow.
 */

'use strict';

const fs = require('fs');

(function assembleAuditPgUrl() {
  if (process.env.AUDIT_PG_URL) {
    return; // Caller already set it — respect their choice.
  }

  const pwfile = process.env.AUDIT_PG_PWFILE;
  if (!pwfile) {
    return; // No password file → no-op; tests will skip integration.
  }

  let password;
  try {
    password = fs.readFileSync(pwfile, 'utf8').trim();
  } catch {
    return; // Unreadable → no-op.
  }
  if (!password) {
    return;
  }

  const host = process.env.AUDIT_PG_HOST || '127.0.0.1';
  const port = process.env.AUDIT_PG_PORT || '5432';
  const user = process.env.AUDIT_PG_USER || 'postgres';
  const database = process.env.AUDIT_PG_DATABASE || 'postgres';

  const encodedUser = encodeURIComponent(user);
  const encodedPassword = encodeURIComponent(password);
  process.env.AUDIT_PG_URL =
    'postgres://' + encodedUser + ':' + encodedPassword +
    '@' + host + ':' + port + '/' + database;
})();
