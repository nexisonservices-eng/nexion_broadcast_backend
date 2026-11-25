const path = require('path');
const { spawn } = require('node:child_process');

const META_VOICE_MIME_TYPE = 'audio/ogg';
const META_VOICE_EXTENSION = 'ogg';

const resolveOutputFileName = (originalName = '') => {
  const baseName = path.basename(String(originalName || 'voice-note'));
  const withoutExtension = baseName.replace(/\.[^.]*$/, '').trim() || 'voice-note';
  return `${withoutExtension}.${META_VOICE_EXTENSION}`;
};

const resolveFfmpegPath = () => {
  try {
    // ffmpeg-static ships a Linux binary with the application, so Render does
    // not need a separately installed system package.
    return require('ffmpeg-static');
  } catch (error) {
    const dependencyError = new Error(
      'Voice-note conversion is unavailable because ffmpeg-static is not installed.'
    );
    dependencyError.code = 'VOICE_NOTE_TRANSCODER_UNAVAILABLE';
    dependencyError.status = 503;
    dependencyError.cause = error;
    throw dependencyError;
  }
};

const transcodeVoiceNoteToOggOpus = async (file, options = {}) => {
  if (!file?.buffer?.length) {
    const error = new Error('Voice-note audio buffer is required for conversion.');
    error.code = 'VOICE_NOTE_BUFFER_REQUIRED';
    error.status = 422;
    throw error;
  }

  const ffmpegPath = options.ffmpegPath || resolveFfmpegPath();
  const spawnProcess = options.spawnProcess || spawn;
  const args = [
    '-hide_banner',
    '-loglevel',
    'error',
    '-i',
    'pipe:0',
    '-vn',
    '-ac',
    '1',
    '-c:a',
    'libopus',
    '-b:a',
    '32k',
    '-f',
    'ogg',
    'pipe:1'
  ];

  const output = [];
  const diagnostics = [];

  await new Promise((resolve, reject) => {
    let settled = false;
    const finish = (error) => {
      if (settled) return;
      settled = true;
      if (error) reject(error);
      else resolve();
    };

    let processHandle;
    try {
      processHandle = spawnProcess(ffmpegPath, args, {
        stdio: ['pipe', 'pipe', 'pipe']
      });
    } catch (error) {
      finish(error);
      return;
    }

    processHandle.stdout.on('data', (chunk) => output.push(Buffer.from(chunk)));
    processHandle.stderr.on('data', (chunk) => diagnostics.push(Buffer.from(chunk)));
    processHandle.on('error', (error) => {
      const conversionError = new Error(`Voice-note conversion failed: ${error.message}`);
      conversionError.code = 'VOICE_NOTE_CONVERSION_FAILED';
      conversionError.status = 422;
      conversionError.cause = error;
      finish(conversionError);
    });
    processHandle.on('close', (code, signal) => {
      if (code !== 0) {
        const detail = Buffer.concat(diagnostics).toString('utf8').trim();
        const conversionError = new Error(
          `Voice-note conversion failed${detail ? `: ${detail}` : ` (exit code ${code || signal || 'unknown'})`}`
        );
        conversionError.code = 'VOICE_NOTE_CONVERSION_FAILED';
        conversionError.status = 422;
        finish(conversionError);
        return;
      }
      finish();
    });

    processHandle.stdin.on('error', (error) => {
      if (error.code !== 'EPIPE') finish(error);
    });
    processHandle.stdin.end(file.buffer);
  });

  const buffer = Buffer.concat(output);
  if (!buffer.length) {
    const error = new Error('Voice-note conversion produced an empty audio file.');
    error.code = 'VOICE_NOTE_CONVERSION_EMPTY';
    error.status = 422;
    throw error;
  }

  return {
    ...file,
    buffer,
    size: buffer.length,
    mimetype: META_VOICE_MIME_TYPE,
    originalname: resolveOutputFileName(file.originalname)
  };
};

const prepareOutboundAttachmentFile = (file, isVoiceNote, options = {}) =>
  isVoiceNote ? transcodeVoiceNoteToOggOpus(file, options) : Promise.resolve(file);

module.exports = {
  META_VOICE_MIME_TYPE,
  META_VOICE_EXTENSION,
  transcodeVoiceNoteToOggOpus,
  prepareOutboundAttachmentFile
};
