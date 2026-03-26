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
    console.log('Starting backend at:', backendPath);
    backendProcess = spawn(backendPath, [], {
        cwd: path.join(__dirname, 'dist'),
        detached: false,
    });

    backendProcess.stdout.on('data', (data) => {
        console.log(`Backend: ${data}`);
    });

    backendProcess.stderr.on('data', (data) => {
        console.error(`Backend error: ${data}`);
    });
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

    // Wait a moment for backend to start, then load the app
    setTimeout(() => {
        mainWindow.loadURL('http://localhost:8000');
    }, 2000);

    mainWindow.on('closed', () => {
        mainWindow = null;
    });
}

app.on('ready', () => {
    startBackend();
    createWindow();
});

app.on('window-all-closed', () => {
    if (backendProcess) backendProcess.kill();
    app.quit();
});

app.on('activate', () => {
    if (mainWindow === null) createWindow();
});
