function createRecorder (stream: MediaStream, mimeType: string) {
    let recordedChunks: BlobPart[] = []; 
    const SAVE_INTERVAL_MS = 200;
    const mediaRecorder = new MediaRecorder(stream);

    mediaRecorder.ondataavailable = function (e) {
        if (e.data.size > 0) {
        recordedChunks.push(e.data);
        }  
    };
    mediaRecorder.onstop = function () {
        saveFile(recordedChunks, mimeType);
        recordedChunks = [];
    };
    mediaRecorder.start(SAVE_INTERVAL_MS);

    return mediaRecorder;
}

function saveFile(recordedChunks: BlobPart[], mimeType: string) {
    const blob = new Blob(recordedChunks, {
        type: mimeType,
    });
    const filename = +new Date() + '.' + mimeType.split('/')[1];
    const downloadLink = document.createElement('a');
    downloadLink.href = URL.createObjectURL(blob);
    downloadLink.download = filename;

    document.body.appendChild(downloadLink);
    downloadLink.click();

    URL.revokeObjectURL(downloadLink.href); // clear from memory
    document.body.removeChild(downloadLink);
}

async function recordScreen() {
    return await navigator.mediaDevices.getDisplayMedia({
        audio: true, 
        video: true,
        // not supported now
        // video: { advanced: [{ frameRate: 30 }]},
    });
};

export async function screenRecorder() {
    try {
        const stream = await recordScreen();
        const defaultMimeType = 'video/webm';
        const mediaRecorder = createRecorder(stream, defaultMimeType);
        
        return () => mediaRecorder.stop();
    } catch (e) {
        console.log(e)
    }
}

// @ts-ignore
window.recordSceenSample = screenRecorder;

// NOT SUPPORTED:
// macOS:  chrome and edge only support capturing current tab's audio
// windows: chrome and edge supports all audio 
// other: not supported 
