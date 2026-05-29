const { spawn } = require('child_process');
const path = require('path');

const rootDir = path.resolve(__dirname, '..');
const serverEntry = path.join(rootDir, 'server.js');
const broadcastWorkerEntry = path.join(rootDir, 'workers', 'broadcastWorker.js');

const children = new Map();
let shuttingDown = false;

const spawnProcess = (name, entry, args = []) => {
  const child = spawn(process.execPath, [entry, ...args], {
    cwd: rootDir,
    env: process.env,
    stdio: 'inherit',
    windowsHide: true
  });

  children.set(name, child);

  child.on('exit', (code, signal) => {
    children.delete(name);

    if (shuttingDown) {
      return;
    }

    console.error(
      `[startAll] ${name} exited unexpectedly with code=${code ?? 'null'} signal=${signal ?? 'null'}`
    );
    shutdown(code ?? 1);
  });

  child.on('error', (error) => {
    console.error(`[startAll] failed to start ${name}:`, error.message);
    if (!shuttingDown) {
      shutdown(1);
    }
  });

  return child;
};

const shutdown = async (code = 0) => {
  if (shuttingDown) return;
  shuttingDown = true;

  for (const child of children.values()) {
    try {
      child.kill('SIGTERM');
    } catch {
      // Ignore kill errors during shutdown.
    }
  }

  const deadline = Date.now() + 10000;
  while (children.size > 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  for (const child of children.values()) {
    try {
      child.kill('SIGKILL');
    } catch {
      // Ignore force-kill errors during shutdown.
    }
  }

  process.exit(code);
};

process.on('SIGINT', () => {
  void shutdown(0);
});

process.on('SIGTERM', () => {
  void shutdown(0);
});

spawnProcess('server', serverEntry);
spawnProcess('broadcast-worker', broadcastWorkerEntry, ['--mode=all']);

console.log('[startAll] launched server and broadcast worker');
