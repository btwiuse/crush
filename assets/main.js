const loading = document.getElementById("loading");

function promisify(fn) {
  return (...args) => {
    return new Promise((resolve, reject) => {
      const newArgs = [...args]
      newArgs.push((err, ...results) => {
        if (err) {
          reject(err)
        } else {
          resolve(results)
        }
      })
      fn(...newArgs)
    })
  }
}

/**
 * Extract Go WASM environment variables from URL query parameters.
 *
 * Query parameters prefixed with `env.` are mapped into a plain object
 * suitable for assigning to `go.env` before `go.run(instance)`.
 *
 * Example:
 *
 *   URL:
 *     ?env.TERM=xterm-256color&env.DEBUG=1
 *
 *   Result:
 *     {
 *       TERM: "xterm-256color",
 *       DEBUG: "1",
 *     }
 *
 * Usage:
 *
 *   const go = new Go()
 *   go.env = extractGoEnv()
 *
 * Supported format:
 *
 *   ?env.KEY=value
 *
 * Notes:
 *
 * - Keys must match `/^[A-Z0-9_]+$/i`
 * - Values are automatically URL-decoded by URLSearchParams
 * - Non-`env.*` parameters are ignored
 */
function extractGoEnv(search = window.location.search) {
  const params = new URLSearchParams(search);
  const env = {};

  for (const [key, value] of params.entries()) {
    if (!key.startsWith("env.")) {
      continue;
    }

    const envKey = key.slice(4);

    // optional validation
    if (!/^[A-Z0-9_]+$/i.test(envKey)) {
      continue;
    }

    env[envKey] = value;
  }

  return env;
}

function waitForBridge() {
  return new Promise((resolve) => {
    function check() {
      if (
        globalThis.bubbletea_resize !== undefined &&
        globalThis.bubbletea_read !== undefined &&
        globalThis.bubbletea_write !== undefined
      ) {
        resolve();
      } else {
        console.log("waiting for bubbletea bridge…");
        setTimeout(check, 500);
      }
    }
    check();
  });
}

function initTerminal() {
  const term = new Terminal({
    convertEol: true,
    cursorBlink: true,
    allowTransparency: true,
  });
  const imageAddon = new ImageAddon.ImageAddon();
  term.loadAddon(imageAddon);
  const fitAddon = new FitAddon.FitAddon();
  if (new URLSearchParams(location.search).get("webgl") !== null) {
    const webglAddon = new WebglAddon.WebglAddon();
    try {
      term.loadAddon(webglAddon);
    } catch (e) {
      console.warn(
        "WebGL addon failed to load, falling back to canvas renderer",
        e,
      );
    }
  }
  term.loadAddon(fitAddon);
  term.open(document.getElementById("terminal-container"));

  fitAddon.fit();
  window.addEventListener("resize", () => fitAddon.fit());

  term.focus();

  // Send initial size to Go
  bubbletea_resize(term.cols, term.rows);

  /** Whether the Go program has exited; gate all input after this point. */
  let exited = false;

  // Poll Go output and write to terminal
  const pollInterval = setInterval(() => {
    if (exited) return;
    const data = bubbletea_read();
    if (data && data.length > 0) {
      term.write(data);
    }
  }, 16);

  // Forward resize events to Go
  term.onResize((size) => {
    if (!exited) bubbletea_resize(size.cols, size.rows);
  });

  // Forward key/paste input to Go; reload after exit
  term.onData((data) => {
    if (exited) {
      location.reload();
      return;
    }
    bubbletea_write(data);
  });

  return {
    term,
    pollInterval,
    setExited: (v) => {
      exited = v;
    },
  };
}

async function main() {
  let overlayProgress = 0;
  let progressListeners = [];
  const go = new Go();
  go.env = {
    'CRUSH_DISABLE_PROVIDER_AUTO_UPDATE': '1',
    'CRUSH_VERSION': 'v0.69.1',
    'CRUSH_CORE_UTILS': '1',
    'TERM': 'xterm-256color',
    'USER': 'me',
    'HOME': '/home/me',
    'TMPDIR': '/tmp',
    'GOMODCACHE': '/home/me/.cache/go-mod',
    'GOPROXY': 'https://proxy.golang.org/',
    'GOROOT': '/usr/local/go',
    'PATH': '/bin:/home/me/go/bin:/usr/local/go/bin/js_wasm:/usr/local/go/pkg/tool/js_wasm',
    ...extractGoEnv(),
  };
  const wasmPath = new URLSearchParams(location.search).get("wasm") ||
    "crush.wasm";
  const result = await WebAssembly.instantiateStreaming(
    fetch(wasmPath),
    go.importObject,
  );

  // Start the WASM module (non-blocking); Go registers the bridge globals as it runs
  const runPromise = go.run(result.instance);

  // Wait until go-booba registers the JS bridge globals

  // Setup fs mounts
  const { hackpad, fs } = window
  console.log(`hackpad status: ${hackpad.ready ? 'ready' : 'not ready'}`)

  let mkdir = promisify(fs.mkdir)
  await mkdir("/bin", {mode: 0o700})
  await hackpad.overlayIndexedDB('/bin', {cache: true})
  await hackpad.overlayIndexedDB('/home/me')
  await mkdir("/home/me/.cache", {recursive: true, mode: 0o700})
  await hackpad.overlayIndexedDB('/home/me/.cache', {cache: true})

  await mkdir("/usr/local/go", {recursive: true, mode: 0o700})
  await hackpad.overlayTarGzip('/usr/local/go', '/hackpad/wasm/go.tar.gz', {
    persist: true,
    skipCacheDirs: [
      '/usr/local/go/bin/js_wasm',
      '/usr/local/go/pkg/tool/js_wasm',
    ],
    progress: percentage => {
      overlayProgress = percentage
      progressListeners.forEach(c => c(percentage))
    },
  })

  await waitForBridge();

  // Hide the loading overlay
  loading.classList.add("hidden");

  const { term, pollInterval, setExited } = initTerminal();

  // When the Go program exits, show a restart prompt
  runPromise.then(() => {
    console.log("wasm finished");
    setExited(true);
    clearInterval(pollInterval);
    term.write("\r\n\r\nPress any key to continue...");
  });
}

main().catch(console.error);
