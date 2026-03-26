const { app, BrowserWindow, Menu, globalShortcut } = require('electron');
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

        backendProcess.on('error', (err) => {
            console.log('Backend spawn note:', err.message);
        });

        backendProcess.stdout && backendProcess.stdout.on('data', (data) => {
            console.log(`Backend: ${data}`);
        });

        backendProcess.stderr && backendProcess.stderr.on('data', (data) => {
            console.log(`Backend: ${data}`);
        });

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

    // Remove top menu bar completely
    Menu.setApplicationMenu(null);

    mainWindow.loadURL('about:blank');

    // Wait for backend to start, then load the app
    setTimeout(() => {
        mainWindow.loadURL('http://localhost:8000');
    }, 5000);

    // Enable F5 to reload and F12 for DevTools
    mainWindow.webContents.on('before-input-event', (event, input) => {
        if (input.key === 'F5') {
            mainWindow.webContents.reload();
        }
        if (input.key === 'F12') {
            mainWindow.webContents.toggleDevTools();
        }
    });

    mainWindow.on('closed', () => {
        mainWindow = null;
    });
}

app.on('ready', () => {
    startBackend();
    createWindow();
});

app.on('window-all-closed', () => {
    if (backendProcess) {
        try { backendProcess.kill(); } catch(e) {}
    }
    app.quit();
});

app.on('activate', () => {
    if (mainWindow === null) createWindow();
});
