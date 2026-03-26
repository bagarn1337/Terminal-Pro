const { app, BrowserWindow, Menu, ipcMain } = require('electron');
const { spawn } = require('child_process');
const path = require('path');

let backendProcess;
let mainWindow;

function getBackendPath() {
    if (app.isPackaged) {
        return path.join(process.resourcesPath, 'main.exe');
    } else {
        return path.join(__dirname, 'dist', 'main.exe');
    }
}

function startBackend() {
    const backendPath = getBackendPath();
    try {
        backendProcess = spawn(backendPath, [], {
            cwd: path.dirname(backendPath),
            detached: false,
        });
        backendProcess.on('error', (err) => { console.log('Backend:', err.message); });
        backendProcess.stdout && backendProcess.stdout.on('data', (d) => console.log(`${d}`));
        backendProcess.stderr && backendProcess.stderr.on('data', (d) => console.log(`${d}`));
    } catch (err) {
        console.log('Backend start note:', err.message);
    }
}

function createWindow() {
    mainWindow = new BrowserWindow({
        width: 1400,
        height: 900,
        frame: false,
        webPreferences: {
            nodeIntegration: true,
            contextIsolation: false,
        },
    });

    Menu.setApplicationMenu(null);
    mainWindow.loadURL('about:blank');

    setTimeout(() => {
        mainWindow.loadURL('http://localhost:8000');

        mainWindow.webContents.on('did-finish-load', () => {
            // Inject electronAPI to match what your HTML buttons call
            mainWindow.webContents.executeJavaScript(`
                window.electronAPI = {
                    minimize:    () => require('electron').ipcRenderer.send('minimize'),
                    maximize:    () => require('electron').ipcRenderer.send('maximize'),
                    closeWindow: () => require('electron').ipcRenderer.send('close'),
                };
            `);

            // Make the topbar draggable
            mainWindow.webContents.insertCSS(`
                .topbar { -webkit-app-region: drag; }
                .topbar button, .topbar input, .topbar a, .topbar select,
                .topbar .nav-tab, .topbar .topbar-right, .topbar-right * { -webkit-app-region: no-drag; }
            `);
        });

    }, 5000);

    // F5 reload, F12 devtools
    mainWindow.webContents.on('before-input-event', (event, input) => {
        if (input.key === 'F5') mainWindow.webContents.reload();
        if (input.key === 'F12') mainWindow.webContents.toggleDevTools();
    });

    mainWindow.on('closed', () => { mainWindow = null; });
}

// Window control handlers
ipcMain.on('minimize', () => mainWindow && mainWindow.minimize());
ipcMain.on('maximize', () => mainWindow && (mainWindow.isMaximized() ? mainWindow.unmaximize() : mainWindow.maximize()));
ipcMain.on('close',    () => mainWindow && mainWindow.close());

app.on('ready', () => {
    startBackend();
    createWindow();
});

app.on('window-all-closed', () => {
    if (backendProcess) { try { backendProcess.kill(); } catch(e) {} }
    app.quit();
});

app.on('activate', () => { if (mainWindow === null) createWindow(); });
