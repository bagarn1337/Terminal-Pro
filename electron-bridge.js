/**
 * Terminal Pro — Electron Bridge
 * Injected into the main app renderer when running inside Electron.
 * Adds: custom titlebar, multi-window toolbar, panel launcher, layout management.
 *
 * Include this script at the BOTTOM of index.html's <body>.
 * It wraps the existing app UI — does NOT require modifying the rest of the file.
 */

(function() {
  if (!window.electronAPI) return;   // only runs inside Electron

  const eAPI = window.electronAPI;
  const isMac = eAPI.platform === 'darwin';

  // ── 1. Inject Custom Titlebar (Windows / Linux only) ──────────────────────
  if (!isMac) {
    injectTitlebar();
  } else {
    // macOS: just add top padding so traffic lights don't overlap content
    document.querySelector('.app-shell')?.style.setProperty('padding-top', '0px');
  }

  // ── 2. Inject Multi-Window Toolbar ────────────────────────────────────────
  injectPanelToolbar();

  // ── 3. Listen for menu events ─────────────────────────────────────────────
  eAPI.on('menu:save-layout',      () => openLayoutDialog('save'));
  eAPI.on('menu:load-layout',      () => openLayoutDialog('load'));
  eAPI.on('menu:command-palette',  () => document.getElementById('cmd-input')?.focus());
  eAPI.on('menu:shortcuts',        () => showShortcutsOverlay());
  eAPI.on('updater:available',     (info) => showUpdateBanner(info, false));
  eAPI.on('updater:downloaded',    (info) => showUpdateBanner(info, true));

  // ── 4. Keyboard: Ctrl+Shift+N → quick open panel picker ──────────────────
  document.addEventListener('keydown', (e) => {
    if (e.ctrlKey && e.shiftKey && e.key === 'N') {
      e.preventDefault();
      openPanelPicker();
    }
  });

  // ── 5. Context-aware ticker sync (open panel for current ticker) ──────────
  hookTickerSync();

  // ─────────────────────────────────────────────────────────────────────────
  // TITLEBAR
  // ─────────────────────────────────────────────────────────────────────────
  function injectTitlebar() {
    const bar = document.createElement('div');
    bar.id = 'el-titlebar';
    bar.innerHTML = `
      <div id="el-tb-drag">
        <div id="el-tb-logo">
          <div class="el-logo-mark">T</div>
          <span class="el-logo-text">TERMINAL PRO</span>
        </div>
      </div>
      <div id="el-tb-controls">
        <button class="el-tb-btn" id="el-btn-min"   title="Minimize">─</button>
        <button class="el-tb-btn" id="el-btn-max"   title="Maximize">▢</button>
        <button class="el-tb-btn el-btn-close" id="el-btn-close" title="Close">✕</button>
      </div>
    `;
    document.body.prepend(bar);

    const style = document.createElement('style');
    style.textContent = `
      #el-titlebar {
        display:flex; align-items:center; justify-content:space-between;
        height:32px; background:#0a0f16; border-bottom:1px solid rgba(255,255,255,0.06);
        position:fixed; top:0; left:0; right:0; z-index:99999;
        -webkit-app-region: drag;
      }
      #el-tb-drag { flex:1; display:flex; align-items:center; height:100%; }
      #el-tb-logo {
        display:flex; align-items:center; gap:8px;
        padding:0 14px; -webkit-app-region:drag;
      }
      .el-logo-mark {
        width:20px; height:20px; background:#00e5a0;
        display:flex; align-items:center; justify-content:center;
        font-size:10px; font-weight:700; color:#000; border-radius:2px;
      }
      .el-logo-text {
        font-family:JetBrains Mono,monospace; font-size:10px;
        font-weight:500; letter-spacing:0.18em; color:#8fa4b8;
      }
      #el-tb-controls {
        display:flex; align-items:stretch; height:100%;
        -webkit-app-region: no-drag;
      }
      .el-tb-btn {
        background:transparent; border:none; color:#4e6478;
        width:46px; font-size:12px; cursor:pointer;
        font-family:JetBrains Mono,monospace; transition:all 0.12s;
        display:flex; align-items:center; justify-content:center;
      }
      .el-tb-btn:hover { background:rgba(255,255,255,0.06); color:#f0f4f8; }
      .el-btn-close:hover { background:#f0455a !important; color:#fff !important; }
      .app-shell { padding-top: 32px !important; }
      .topbar { top: 32px !important; }
      .lbar { top: 76px !important; }
    `;
    document.head.appendChild(style);

    document.getElementById('el-btn-min').onclick   = () => eAPI.minimize();
    document.getElementById('el-btn-max').onclick   = () => eAPI.maximize();
    document.getElementById('el-btn-close').onclick = () => eAPI.closeWindow();
  }

  // ─────────────────────────────────────────────────────────────────────────
  // PANEL TOOLBAR
  // ─────────────────────────────────────────────────────────────────────────
  const PANELS = [
    { type:'chart',     icon:'📈', label:'Chart'      },
    { type:'level2',    icon:'📋', label:'Level 2'    },
    { type:'timesales', icon:'🕐', label:'T&S'        },
    { type:'news',      icon:'📰', label:'News'       },
    { type:'watchlist', icon:'⭐', label:'Watchlist'  },
    { type:'options',   icon:'⚙', label:'Options'    },
    { type:'portfolio', icon:'💼', label:'Portfolio'  },
    { type:'heatmap',   icon:'🌡', label:'Heatmap'   },
    { type:'orderflow', icon:'⚡', label:'Flow'       },
    { type:'risk',      icon:'🛡', label:'Risk'       },
    { type:'screener',  icon:'🔍', label:'Screener'   },
    { type:'macro',     icon:'🌐', label:'Macro'      },
  ];

  function injectPanelToolbar() {
    const toolbar = document.createElement('div');
    toolbar.id = 'el-panel-toolbar';
    toolbar.innerHTML = `
      <div id="el-pt-scroll">
        <span class="el-pt-label">PANELS</span>
        ${PANELS.map(p => `
          <button class="el-pt-btn" data-type="${p.type}" title="Open ${p.label} panel">
            <span class="el-pt-icon">${p.icon}</span>
            <span class="el-pt-text">${p.label}</span>
          </button>
        `).join('')}
        <div class="el-pt-divider"></div>
        <button class="el-pt-action" id="el-pt-tile"  title="Tile all windows">⊞ Tile</button>
        <button class="el-pt-action" id="el-pt-stack" title="Stack windows">⧉ Stack</button>
        <button class="el-pt-action" id="el-pt-save"  title="Save layout">💾 Save</button>
        <button class="el-pt-action" id="el-pt-load"  title="Load layout">📂 Load</button>
        <button class="el-pt-action el-pt-danger" id="el-pt-closeall" title="Close all panels">✕ All</button>
      </div>
      <div id="el-pt-openlist"></div>
    `;
    document.body.appendChild(toolbar);

    const style = document.createElement('style');
    style.textContent = `
      #el-panel-toolbar {
        position:fixed; bottom:0; left:0; right:0; z-index:99998;
        background:#0a0f16; border-top:1px solid rgba(255,255,255,0.06);
        display:flex; flex-direction:column; max-height:40px;
        transition:max-height 0.2s ease; overflow:hidden;
      }
      #el-panel-toolbar:hover { max-height:80px; }
      #el-pt-scroll {
        display:flex; align-items:center; gap:2px;
        padding:0 10px; height:40px; overflow-x:auto; flex-shrink:0;
        scrollbar-width:none;
      }
      #el-pt-scroll::-webkit-scrollbar { display:none; }
      .el-pt-label {
        font-family:JetBrains Mono,monospace; font-size:8px;
        color:#2a3d50; letter-spacing:0.2em; text-transform:uppercase;
        padding-right:8px; flex-shrink:0;
      }
      .el-pt-btn {
        display:flex; flex-direction:column; align-items:center; justify-content:center;
        background:transparent; border:1px solid rgba(255,255,255,0.05);
        color:#8fa4b8; padding:3px 10px; cursor:pointer; border-radius:3px;
        font-family:JetBrains Mono,monospace; font-size:9px; font-weight:500;
        letter-spacing:0.04em; transition:all 0.12s; flex-shrink:0; gap:1px;
        min-width:52px;
      }
      .el-pt-btn:hover {
        background:rgba(0,229,160,0.08); border-color:rgba(0,229,160,0.25);
        color:#00e5a0;
      }
      .el-pt-icon { font-size:12px; line-height:1; }
      .el-pt-text { font-size:8px; letter-spacing:0.06em; }
      .el-pt-divider {
        width:1px; height:24px; background:rgba(255,255,255,0.06);
        margin:0 6px; flex-shrink:0;
      }
      .el-pt-action {
        background:transparent; border:none; color:#4e6478;
        padding:4px 10px; cursor:pointer; border-radius:3px;
        font-family:JetBrains Mono,monospace; font-size:9px; font-weight:600;
        letter-spacing:0.05em; transition:all 0.12s; flex-shrink:0; white-space:nowrap;
      }
      .el-pt-action:hover { color:#f0f4f8; background:rgba(255,255,255,0.04); }
      .el-pt-danger:hover { color:#f0455a !important; }
      #el-pt-openlist {
        display:flex; align-items:center; gap:6px;
        padding:0 10px 6px; flex-wrap:wrap; overflow:hidden;
      }
      .el-open-tag {
        display:flex; align-items:center; gap:5px;
        background:rgba(0,229,160,0.07); border:1px solid rgba(0,229,160,0.18);
        color:#00e5a0; font-family:JetBrains Mono,monospace; font-size:9px;
        padding:2px 8px; border-radius:3px; cursor:pointer; letter-spacing:0.04em;
        transition:all 0.12s;
      }
      .el-open-tag:hover { background:rgba(0,229,160,0.14); }
      .el-open-tag-x { color:#f0455a; font-size:10px; cursor:pointer; margin-left:3px; }
      .el-open-tag-x:hover { opacity:0.7; }
      /* Shrink the existing topbar slightly to fit toolbar */
      .news-slider, #news-bottom-bar { margin-bottom: 40px !important; }
    `;
    document.head.appendChild(style);

    // Panel open buttons
    document.querySelectorAll('.el-pt-btn[data-type]').forEach(btn => {
      btn.onclick = () => {
        const type   = btn.dataset.type;
        const ticker = getCurrentTicker();
        openPanel(type, ticker);
      };
    });

    // Action buttons
    document.getElementById('el-pt-tile').onclick     = () => eAPI.tileAll();
    document.getElementById('el-pt-stack').onclick    = () => eAPI.stackAll();
    document.getElementById('el-pt-closeall').onclick = () => { eAPI.closeAll(); refreshOpenList(); };
    document.getElementById('el-pt-save').onclick     = () => openLayoutDialog('save');
    document.getElementById('el-pt-load').onclick     = () => openLayoutDialog('load');

    // Listen for panel open/close events to update open list
    eAPI.on('wm:panel-opened', refreshOpenList);
    eAPI.on('wm:panel-closed', refreshOpenList);
  }

  async function openPanel(type, ticker = '') {
    await eAPI.openPanel({ type, ticker });
    setTimeout(refreshOpenList, 300);
  }

  async function refreshOpenList() {
    const panels = await eAPI.getPanels().catch(() => []);
    const list   = document.getElementById('el-pt-openlist');
    if (!list) return;
    list.innerHTML = '';
    for (const p of panels) {
      const tag = document.createElement('div');
      tag.className = 'el-open-tag';
      tag.title = `Panel #${p.id}: ${p.type} — click to bring to front`;
      tag.innerHTML = `${p.type.toUpperCase()}${p.ticker ? ' · ' + p.ticker : ''}<span class="el-open-tag-x" title="Close">✕</span>`;
      tag.querySelector('.el-open-tag-x').onclick = (e) => {
        e.stopPropagation();
        eAPI.closePanel(p.id);
        setTimeout(refreshOpenList, 200);
      };
      list.appendChild(tag);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // PANEL PICKER OVERLAY
  // ─────────────────────────────────────────────────────────────────────────
  function openPanelPicker() {
    let overlay = document.getElementById('el-panel-picker');
    if (overlay) { overlay.remove(); return; }

    overlay = document.createElement('div');
    overlay.id = 'el-panel-picker';
    overlay.innerHTML = `
      <div id="el-pp-box">
        <div id="el-pp-header">
          <span>OPEN PANEL</span>
          <span style="color:var(--t4);font-size:9px">Ctrl+Shift+N</span>
        </div>
        <input id="el-pp-search" type="text" placeholder="Search panels…" autocomplete="off">
        <div id="el-pp-grid">
          ${PANELS.map(p => `
            <button class="el-pp-item" data-type="${p.type}">
              <span class="el-pp-icon">${p.icon}</span>
              <span class="el-pp-label">${p.label}</span>
            </button>
          `).join('')}
        </div>
        <div id="el-pp-ticker-row">
          <span style="color:var(--t3);font-size:10px">Symbol:</span>
          <input id="el-pp-ticker" type="text" placeholder="AAPL" value="${getCurrentTicker()}" style="text-transform:uppercase">
        </div>
      </div>
    `;

    const style = document.createElement('style');
    style.id = 'el-pp-style';
    style.textContent = `
      #el-panel-picker {
        position:fixed; inset:0; z-index:199999;
        background:rgba(0,0,0,0.7); backdrop-filter:blur(4px);
        display:flex; align-items:center; justify-content:center;
      }
      #el-pp-box {
        background:#0c1015; border:1px solid rgba(0,229,160,0.25);
        border-radius:8px; padding:20px; width:480px; max-width:90vw;
        box-shadow:0 20px 60px rgba(0,0,0,0.8);
      }
      #el-pp-header {
        display:flex; justify-content:space-between; align-items:center;
        margin-bottom:14px; font-family:JetBrains Mono,monospace;
        font-size:11px; font-weight:600; letter-spacing:0.12em; color:#8fa4b8;
      }
      #el-pp-search {
        width:100%; background:#080b0e; border:1px solid rgba(255,255,255,0.07);
        color:#f0f4f8; font-family:JetBrains Mono,monospace; font-size:12px;
        padding:8px 12px; border-radius:4px; outline:none; margin-bottom:14px;
      }
      #el-pp-search:focus { border-color:rgba(0,229,160,0.4); }
      #el-pp-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:8px; margin-bottom:14px; }
      .el-pp-item {
        display:flex; flex-direction:column; align-items:center; justify-content:center;
        gap:5px; padding:12px 6px; background:#111820; border:1px solid rgba(255,255,255,0.06);
        border-radius:6px; cursor:pointer; font-family:JetBrains Mono,monospace;
        color:#8fa4b8; transition:all 0.12s;
      }
      .el-pp-item:hover { background:rgba(0,229,160,0.08); border-color:rgba(0,229,160,0.25); color:#00e5a0; }
      .el-pp-icon { font-size:20px; }
      .el-pp-label { font-size:9px; letter-spacing:0.06em; }
      #el-pp-ticker-row {
        display:flex; align-items:center; gap:10px;
        font-family:JetBrains Mono,monospace;
      }
      #el-pp-ticker {
        flex:1; background:#080b0e; border:1px solid rgba(255,255,255,0.07);
        color:#00e5a0; font-family:JetBrains Mono,monospace; font-size:13px;
        font-weight:500; padding:6px 10px; border-radius:4px; outline:none;
      }
      #el-pp-ticker:focus { border-color:rgba(0,229,160,0.4); }
    `;
    document.head.appendChild(style);
    document.body.appendChild(overlay);

    const searchInput  = document.getElementById('el-pp-search');
    const tickerInput  = document.getElementById('el-pp-ticker');
    searchInput.focus();

    // Filter panels
    searchInput.addEventListener('input', () => {
      const q = searchInput.value.toLowerCase();
      document.querySelectorAll('.el-pp-item').forEach(btn => {
        btn.style.display = btn.dataset.type.includes(q) ||
          btn.querySelector('.el-pp-label').textContent.toLowerCase().includes(q)
          ? '' : 'none';
      });
    });

    document.querySelectorAll('.el-pp-item').forEach(btn => {
      btn.onclick = () => {
        const type   = btn.dataset.type;
        const ticker = tickerInput.value.trim().toUpperCase() || getCurrentTicker();
        openPanel(type, ticker);
        overlay.remove();
        document.getElementById('el-pp-style')?.remove();
      };
    });

    overlay.onclick = (e) => {
      if (e.target === overlay) { overlay.remove(); document.getElementById('el-pp-style')?.remove(); }
    };

    document.addEventListener('keydown', function esc(e) {
      if (e.key === 'Escape') { overlay.remove(); document.getElementById('el-pp-style')?.remove(); document.removeEventListener('keydown', esc); }
    });
  }

  // ─────────────────────────────────────────────────────────────────────────
  // LAYOUT DIALOG
  // ─────────────────────────────────────────────────────────────────────────
  function openLayoutDialog(mode) {
    if (mode === 'save') {
      const name = prompt('Save layout as:', 'my-layout');
      if (name) eAPI.saveLayout(name).then(() => showToast(`Layout "${name}" saved`));
    } else {
      const name = prompt('Load layout (enter name or leave blank to load from file):', 'my-layout');
      if (name === null) return;
      if (name) {
        eAPI.restoreLayout(name).then(ok => showToast(ok ? `Layout "${name}" loaded` : 'Layout not found'));
      } else {
        eAPI.loadLayoutFile().then(ok => showToast(ok ? 'Layout loaded from file' : 'Cancelled'));
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // HELPERS
  // ─────────────────────────────────────────────────────────────────────────
  function getCurrentTicker() {
    // Try to read the current ticker from the main app's input bar
    const input = document.querySelector('input.ti') || document.querySelector('#ticker-input');
    return input?.value?.trim().toUpperCase() || '';
  }

  function hookTickerSync() {
    // When user navigates to a ticker in the main app, offer a right-click
    // context menu to open it in a panel. We watch the ticker input.
    const input = document.querySelector('input.ti');
    if (!input) return;
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && e.altKey) {
        // Alt+Enter → open current ticker in a chart panel
        const ticker = input.value.trim().toUpperCase();
        if (ticker) openPanel('chart', ticker);
      }
    });
  }

  function showToast(msg) {
    const t = document.createElement('div');
    t.style.cssText = `
      position:fixed; bottom:56px; right:20px; z-index:299999;
      background:#16202a; border:1px solid rgba(0,229,160,0.3);
      color:#00e5a0; font-family:JetBrains Mono,monospace; font-size:11px;
      padding:8px 16px; border-radius:4px; letter-spacing:0.05em;
      box-shadow:0 8px 24px rgba(0,0,0,0.5); transition:opacity 0.3s;
    `;
    t.textContent = msg;
    document.body.appendChild(t);
    setTimeout(() => { t.style.opacity = '0'; setTimeout(() => t.remove(), 350); }, 2000);
  }

  function showUpdateBanner(info, ready) {
    const banner = document.createElement('div');
    banner.style.cssText = `
      position:fixed; top:${isMac ? 0 : 32}px; left:0; right:0; z-index:199998;
      background:${ready ? '#00b07a' : '#16202a'}; color:${ready ? '#000' : '#00e5a0'};
      font-family:JetBrains Mono,monospace; font-size:11px;
      padding:7px 20px; display:flex; align-items:center; gap:12px;
      border-bottom:1px solid rgba(0,229,160,0.3);
    `;
    banner.innerHTML = ready
      ? `<span>✓ Update ready (v${info.version})</span><button onclick="electronAPI.installUpdate()" style="background:#000;color:#00e5a0;border:none;padding:3px 12px;cursor:pointer;font-family:inherit;border-radius:3px;">Restart & Install</button><span onclick="this.parentElement.remove()" style="margin-left:auto;cursor:pointer;opacity:0.6">✕</span>`
      : `<span>Update available: v${info.version}</span><span style="color:#4e6478;font-size:10px">Downloading…</span><span onclick="this.parentElement.remove()" style="margin-left:auto;cursor:pointer;opacity:0.6">✕</span>`;
    document.body.prepend(banner);
  }

  function showShortcutsOverlay() {
    // Keyboard shortcuts reference
    const shortcuts = [
      ['Ctrl+Shift+N',       'Open panel picker'],
      ['Ctrl+N',             'New panel window'],
      ['Ctrl+Shift+C',       'New chart window'],
      ['Ctrl+Shift+L',       'New Level 2 window'],
      ['Ctrl+Shift+T',       'New Time & Sales window'],
      ['Ctrl+K',             'Command palette'],
      ['Ctrl+Shift+W',       'Tile all windows'],
      ['Ctrl+Shift+S',       'Stack all windows'],
      ['Alt+Enter (in bar)', 'Open ticker in chart panel'],
      ['F11',                'Toggle fullscreen'],
      ['Ctrl+0',             'Reset zoom'],
      ['Ctrl+S',             'Save layout'],
      ['Ctrl+O',             'Load layout'],
    ];
    const overlay = document.createElement('div');
    overlay.style.cssText = `
      position:fixed; inset:0; z-index:299999; background:rgba(0,0,0,0.75);
      display:flex; align-items:center; justify-content:center;
    `;
    overlay.innerHTML = `
      <div style="background:#0c1015;border:1px solid rgba(0,229,160,0.25);border-radius:8px;padding:28px 32px;min-width:420px;max-width:90vw;box-shadow:0 20px 60px rgba(0,0,0,0.8)">
        <div style="font-family:JetBrains Mono,monospace;font-size:12px;font-weight:600;letter-spacing:0.14em;color:#8fa4b8;margin-bottom:16px;display:flex;justify-content:space-between;">
          KEYBOARD SHORTCUTS
          <span onclick="this.closest('[style]').parentElement.remove()" style="cursor:pointer;color:#4e6478">✕</span>
        </div>
        ${shortcuts.map(([k, v]) => `
          <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid rgba(255,255,255,0.04);font-family:JetBrains Mono,monospace;font-size:11px;gap:20px">
            <code style="color:#00e5a0;background:rgba(0,229,160,0.06);padding:2px 7px;border-radius:3px;white-space:nowrap">${k}</code>
            <span style="color:#8fa4b8">${v}</span>
          </div>
        `).join('')}
      </div>
    `;
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }

  // Initial panel list refresh
  setTimeout(refreshOpenList, 500);

  console.log('[Terminal Pro] Electron bridge loaded ✓');
})();
