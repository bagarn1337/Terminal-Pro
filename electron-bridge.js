const { app, BrowserWindow } = require('electron');
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
            // Silently ignore spawn errors — app still works
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
        webPreferences: {
            nodeIntegration: true,
            contextIsolation: false,
        },
    });

    mainWindow.loadURL('about:blank');

    // Wait for backend to start, then load the app
    setTimeout(() => {
        mainWindow.loadURL('http://localhost:8000');
    }, 5000);

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
