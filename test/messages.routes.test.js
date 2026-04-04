const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const originalFetch = global.fetch;

const authPath = require.resolve('../middleware/auth');
const planGuardPath = require.resolve('../middleware/planGuard');
const credentialsPath = require.resolve('../middleware/requireWhatsAppCredentials');
const messageModelPath = require.resolve('../models/Message');
const conversationModelPath = require.resolve('../models/Conversation');
const whatsappServicePath = require.resolve('../services/whatsappService');
const inboxMediaServicePath = require.resolve('../services/inboxMediaService');
const messagesRoutePath = require.resolve('../routes/messages');

const originalCacheEntries = new Map(
  [
    authPath,
    planGuardPath,
    credentialsPath,
    messageModelPath,
    conversationModelPath,
    whatsappServicePath,
    inboxMediaServicePath,
    messagesRoutePath
  ].map((path) => [path, require.cache[path]])
);

const buildMockMessageDoc = (payload = {}) => ({
  _id: 'msg-test-1',
  ...payload,
  async populate() {
    return this;
  },
  toObject() {
    const clone = { ...this };
    delete clone.populate;
    delete clone.toObject;
    return clone;
  }
});

const mockState = {
  sendTemplateResult: { success: true, data: { messages: [{ id: 'wamid.test.1' }] } },
  lastSendTemplateArgs: null,
  sendMediaResult: { success: true, data: { messages: [{ id: 'wamid.media.1' }] } },
  lastSendMediaArgs: null,
  uploadMediaResult: { success: true, data: { id: 'meta-media-test-1' } },
  lastUploadMediaArgs: null,
  findOneQueue: [],
  findOneQueries: [],
  updateOneCalls: [],
  lastMessagePayload: null,
  createdMessageDoc: null,
  messageFindResults: [],
  messageFindFilters: null,
  messageFindOneQueue: [],
  messageFindOneFilters: null,
  inboxUploadResult: null,
  inboxSignedResult: null,
  inboxDeleteCalls: [],
  realtimeEvents: []
};

const resetState = () => {
  mockState.sendTemplateResult = { success: true, data: { messages: [{ id: 'wamid.test.1' }] } };
  mockState.lastSendTemplateArgs = null;
  mockState.sendMediaResult = { success: true, data: { messages: [{ id: 'wamid.media.1' }] } };
  mockState.lastSendMediaArgs = null;
  mockState.uploadMediaResult = { success: true, data: { id: 'meta-media-test-1' } };
  mockState.lastUploadMediaArgs = null;
  mockState.findOneQueue = [];
  mockState.findOneQueries = [];
  mockState.updateOneCalls = [];
  mockState.lastMessagePayload = null;
  mockState.createdMessageDoc = null;
  mockState.messageFindResults = [];
  mockState.messageFindFilters = null;
  mockState.messageFindOneQueue = [];
  mockState.messageFindOneFilters = null;
  mockState.inboxUploadResult = null;
  mockState.inboxSignedResult = null;
  mockState.inboxDeleteCalls = [];
  mockState.realtimeEvents = [];
};

const messageModelMock = {
  create: async (payload) => {
    mockState.lastMessagePayload = payload;
    const doc = buildMockMessageDoc(payload);
    doc.save = async () => doc;
    doc.select = () => ({ lean: async () => ({ ...doc }) });
    mockState.createdMessageDoc = doc;
    return doc;
  },
  deleteMany: async () => ({ deletedCount: 0 }),
  find: (filters) => {
    mockState.messageFindFilters = filters;
    return {
      sort: () => ({
        limit: () => ({
          select: () => ({
            lean: async () => mockState.messageFindResults
          })
        })
      })
    };
  },
  findOne: (filters) => {
    mockState.messageFindOneFilters = filters;
    const next =
      mockState.messageFindOneQueue.length > 0
        ? mockState.messageFindOneQueue.shift()
        : null;
    if (!next) return null;
    const doc = buildMockMessageDoc(next);
    doc.save = async () => doc;
    doc.select = () => ({ lean: async () => ({ ...doc }) });
    return doc;
  }
};

const conversationModelMock = {
  findOne: (query) => {
    mockState.findOneQueries.push(query);
    if (mockState.findOneQueue.length > 0) {
      return mockState.findOneQueue.shift();
    }
    return null;
  },
  updateOne: async (filter, update) => {
    mockState.updateOneCalls.push({ filter, update });
    return { acknowledged: true, modifiedCount: 1 };
  }
};

const whatsappServiceMock = {
  sendTemplateMessage: async (...args) => {
    mockState.lastSendTemplateArgs = args;
    return mockState.sendTemplateResult;
  },
  sendMediaMessage: async (...args) => {
    mockState.lastSendMediaArgs = args;
    return mockState.sendMediaResult;
  },
  uploadMediaAsset: async (...args) => {
    mockState.lastUploadMediaArgs = args;
    return mockState.uploadMediaResult;
  }
};

const inboxMediaServiceMock = {
  resolveInboxStorageUsername: () => 'test-user-storage',
  uploadInboxAttachment: async () => {
    if (mockState.inboxUploadResult) return mockState.inboxUploadResult;
    return {
      publicId: 'inbox/test-user-storage/sent/test-file',
      secureUrl: 'https://cdn.example.com/inbox/test-user-storage/sent/test-file',
      fileCategory: 'image',
      originalFileName: 'test.png',
      mimeType: 'image/png',
      bytes: 1234,
      username: 'test-user-storage'
    };
  },
  generateSignedAttachmentUrl: () =>
    mockState.inboxSignedResult || {
      url: 'https://cdn.example.com/inbox/test-user-storage/sent/test-file?signed=1',
      expiresAt: new Date(Date.now() + 300000).toISOString()
    },
  generateAttachmentDownloadUrl: () =>
    mockState.inboxSignedResult || {
      url: 'https://cdn.example.com/inbox/test-user-storage/sent/test-file?download=1',
      expiresAt: new Date(Date.now() + 300000).toISOString()
    },
  isAttachmentPathOwned: () => true,
  deleteInboxAttachment: async ({ attachment }) => {
    mockState.inboxDeleteCalls.push(attachment);
  }
};

require.cache[authPath] = {
  id: authPath,
  filename: authPath,
  loaded: true,
  exports: (req, res, next) => {
    const authHeader = String(req.headers.authorization || '');
    if (!authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, error: 'Unauthorized: token missing' });
    }
    req.user = { id: String(authHeader.slice(7) || '').trim() || 'messages-test-user' };
    req.companyId = 'messages-test-company';
    return next();
  }
};

require.cache[planGuardPath] = {
  id: planGuardPath,
  filename: planGuardPath,
  loaded: true,
  exports: () => (_req, _res, next) => next()
};

require.cache[credentialsPath] = {
  id: credentialsPath,
  filename: credentialsPath,
  loaded: true,
  exports: (req, _res, next) => {
    req.whatsappCredentials = { phoneNumberId: 'test-phone-number-id' };
    return next();
  }
};

require.cache[messageModelPath] = {
  id: messageModelPath,
  filename: messageModelPath,
  loaded: true,
  exports: messageModelMock
};

require.cache[conversationModelPath] = {
  id: conversationModelPath,
  filename: conversationModelPath,
  loaded: true,
  exports: conversationModelMock
};

require.cache[whatsappServicePath] = {
  id: whatsappServicePath,
  filename: whatsappServicePath,
  loaded: true,
  exports: whatsappServiceMock
};

require.cache[inboxMediaServicePath] = {
  id: inboxMediaServicePath,
  filename: inboxMediaServicePath,
  loaded: true,
  exports: inboxMediaServiceMock
};

delete require.cache[messagesRoutePath];
const messagesRouter = require('../routes/messages');

const app = express();
app.use(express.json());
app.locals.sendToUser = (userId, payload) => {
  mockState.realtimeEvents.push({ userId, payload });
};
app.use('/api/messages', messagesRouter);

let server;
let baseUrl = '';

const requestJson = async (method, path, { token = 'messages-route-test-user', body } = {}) => {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: {
      Authorization: token ? `Bearer ${token}` : '',
      'Content-Type': 'application/json'
    },
    body: body ? JSON.stringify(body) : undefined
  });
  const data = await response.json();
  return { status: response.status, data, headers: response.headers };
};

const requestMultipart = async (
  path,
  { token = 'messages-route-test-user', fields = {}, file } = {}
) => {
  const formData = new FormData();
  Object.entries(fields).forEach(([key, value]) => {
    formData.append(key, value);
  });
  if (file) {
    formData.append('file', file, 'test.png');
  }
  const response = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: {
      Authorization: token ? `Bearer ${token}` : ''
    },
    body: formData
  });
  const data = await response.json();
  return { status: response.status, data, headers: response.headers };
};

test.before(async () => {
  await new Promise((resolve) => {
    server = app.listen(0, resolve);
  });
  const { port } = server.address();
  baseUrl = `http://127.0.0.1:${port}`;
});

test.after(async () => {
  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }

  for (const [path, entry] of originalCacheEntries.entries()) {
    if (entry) {
      require.cache[path] = entry;
    } else {
      delete require.cache[path];
    }
  }
});

test.beforeEach(() => {
  resetState();
});

test('POST /send-template returns 400 when required fields are missing', async () => {
  const { status, data } = await requestJson('POST', '/api/messages/send-template', {
    body: {
      to: '+919999999999',
      templateName: 'meeting_details'
    }
  });

  assert.equal(status, 400);
  assert.equal(data.success, false);
  assert.match(String(data.error || ''), /required/i);
});

test('POST /send-template returns provider error when WhatsApp send fails', async () => {
  mockState.findOneQueue = [
    {
      _id: 'conversation-provider-error-1',
      companyId: 'messages-test-company'
    }
  ];

  mockState.sendTemplateResult = {
    success: false,
    error: 'Template is not approved for this account.'
  };

  const { status, data } = await requestJson('POST', '/api/messages/send-template', {
    body: {
      to: '+919999999999',
      conversationId: 'conversation-1',
      templateName: 'meeting_details',
      language: 'en_US',
      variables: ['A', 'B']
    }
  });

  assert.notEqual(status, 404);
  assert.equal(status, 400);
  assert.equal(data.success, false);
  assert.match(String(data.error || ''), /not approved/i);
  assert.ok(Array.isArray(mockState.lastSendTemplateArgs));
});

test('POST /send-template returns 400 when conversation is not found', async () => {
  mockState.findOneQueue = [
    null,
    null,
    {
      sort: async () => null
    }
  ];

  const { status, data } = await requestJson('POST', '/api/messages/send-template', {
    body: {
      to: '+919999999999',
      conversationId: 'conversation-2',
      templateName: 'meeting_details',
      language: 'en_US',
      variables: ['Lead User']
    }
  });

  assert.equal(status, 400);
  assert.equal(data.success, false);
  assert.match(String(data.error || ''), /Conversation not found/i);
  assert.equal(mockState.findOneQueries.length, 3);
});

test('POST /send-template creates outbound message and emits realtime event on success', async () => {
  const conversation = {
    _id: 'conversation-success-1',
    companyId: 'messages-test-company'
  };
  mockState.findOneQueue = [conversation];

  const { status, data } = await requestJson('POST', '/api/messages/send-template', {
    token: 'agent-42',
    body: {
      to: '+919999999999',
      conversationId: 'conversation-success-1',
      templateName: 'meeting_details',
      language: 'en_US',
      variables: ['  Alice  ', '', '  10:30 AM  ']
    }
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(
    mockState.lastMessagePayload?.text,
    'Template: meeting_details (Alice, 10:30 AM)'
  );
  assert.equal(mockState.lastMessagePayload?.status, 'sent');
  assert.equal(mockState.lastMessagePayload?.whatsappMessageId, 'wamid.test.1');

  assert.equal(mockState.updateOneCalls.length, 1);
  assert.equal(mockState.updateOneCalls[0]?.filter?._id, 'conversation-success-1');
  assert.equal(
    mockState.updateOneCalls[0]?.update?.lastMessage,
    'Template: meeting_details (Alice, 10:30 AM)'
  );

  assert.equal(mockState.realtimeEvents.length, 1);
  assert.equal(mockState.realtimeEvents[0]?.userId, 'agent-42');
  assert.equal(mockState.realtimeEvents[0]?.payload?.type, 'message_sent');
});

test('POST /send-attachment returns 400 when file is missing', async () => {
  mockState.findOneQueue = [
    {
      _id: 'conversation-no-file-1',
      companyId: 'messages-test-company'
    }
  ];

  const { status, data } = await requestMultipart('/api/messages/send-attachment', {
    fields: {
      to: '+919999999999',
      conversationId: 'conversation-no-file-1'
    }
  });

  assert.equal(status, 400);
  assert.equal(data.success, false);
  assert.match(String(data.error || ''), /Attachment file is required/i);
});

test('POST /send-attachment uploads, sends media, and stores message', async () => {
  const conversation = {
    _id: 'conversation-attach-1',
    companyId: 'messages-test-company'
  };
  mockState.findOneQueue = [conversation];
  mockState.inboxUploadResult = {
    publicId: 'inbox/test-user-storage/sent/attach-1',
    secureUrl: 'https://cdn.example.com/inbox/test-user-storage/sent/attach-1',
    fileCategory: 'image',
    originalFileName: 'attach.png',
    mimeType: 'image/png',
    bytes: 2048,
    username: 'test-user-storage'
  };

  const file = new Blob(['hello'], { type: 'image/png' });

  const { status, data } = await requestMultipart('/api/messages/send-attachment', {
    fields: {
      to: '+919999999999',
      conversationId: 'conversation-attach-1',
      caption: 'Test image'
    },
    file
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(mockState.lastSendMediaArgs?.[0], '+919999999999');
  assert.equal(mockState.lastMessagePayload?.mediaType, 'image');
  assert.equal(mockState.lastMessagePayload?.mediaUrl, mockState.inboxUploadResult.secureUrl);
  assert.equal(mockState.lastMessagePayload?.text, 'Test image');
});

test('POST /send-attachment passes document filename to WhatsApp media send', async () => {
  const conversation = {
    _id: 'conversation-attach-doc-1',
    companyId: 'messages-test-company'
  };
  mockState.findOneQueue = [conversation];
  mockState.inboxUploadResult = {
    publicId: 'inbox/test-user-storage/sent/attach-doc-1',
    secureUrl: 'https://cdn.example.com/inbox/test-user-storage/sent/attach-doc-1',
    fileCategory: 'document',
    originalFileName: 'Proposal.pdf',
    mimeType: 'application/pdf',
    bytes: 4096,
    username: 'test-user-storage',
    pages: 8
  };

  const file = new Blob(['hello'], { type: 'application/pdf' });

  const { status, data } = await requestMultipart('/api/messages/send-attachment', {
    fields: {
      to: '919999999999',
      conversationId: 'conversation-attach-doc-1'
    },
    file
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(mockState.lastSendMediaArgs?.[0], '919999999999');
  assert.equal(mockState.lastSendMediaArgs?.[1], 'document');
  assert.equal(mockState.lastUploadMediaArgs?.[0]?.mimetype, 'application/pdf');
  assert.equal(mockState.lastSendMediaArgs?.[5]?.fileName, 'Proposal.pdf');
  assert.equal(mockState.lastSendMediaArgs?.[5]?.mediaId, 'meta-media-test-1');
  assert.equal(mockState.lastMessagePayload?.mediaType, 'document');
});

test('POST /send-attachment uploads audio and stores it as an audio message', async () => {
  const conversation = {
    _id: 'conversation-attach-audio-1',
    companyId: 'messages-test-company'
  };
  mockState.findOneQueue = [conversation];
  mockState.inboxUploadResult = {
    publicId: 'inbox/test-user-storage/sent/attach-audio-1',
    secureUrl: 'https://cdn.example.com/inbox/test-user-storage/sent/attach-audio-1',
    fileCategory: 'audio',
    originalFileName: 'voice-note.ogg',
    mimeType: 'audio/ogg',
    bytes: 8192,
    username: 'test-user-storage'
  };

  const file = new Blob(['voice'], { type: 'audio/ogg' });

  const { status, data } = await requestMultipart('/api/messages/send-attachment', {
    fields: {
      to: '+919999999999',
      conversationId: 'conversation-attach-audio-1'
    },
    file
  });

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(mockState.lastUploadMediaArgs?.[0]?.mimetype, 'audio/ogg');
  assert.equal(mockState.lastSendMediaArgs?.[1], 'audio');
  assert.equal(mockState.lastSendMediaArgs?.[5]?.mediaId, 'meta-media-test-1');
  assert.equal(mockState.lastMessagePayload?.mediaType, 'audio');
  assert.equal(mockState.lastMessagePayload?.text, '[Audio]');
});

test('GET /attachments returns attachment summaries', async () => {
  mockState.messageFindResults = [
    {
      _id: 'msg-attach-1',
      conversationId: 'conversation-attach-1',
      sender: 'agent',
      senderName: 'Agent',
      mediaType: 'image',
      mediaCaption: 'Caption',
      mediaUrl: 'https://cdn.example.com/image.png',
      status: 'sent',
      timestamp: new Date().toISOString(),
      attachment: {
        publicId: 'inbox/test-user-storage/sent/attach-1',
        originalFileName: 'attach.png',
        mimeType: 'image/png',
        bytes: 2048
      }
    }
  ];

  const { status, data } = await requestJson('GET', '/api/messages/attachments');

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(Array.isArray(data.data), true);
  assert.equal(data.data[0]?.messageId, 'msg-attach-1');
});

test('GET /attachments/:id/url returns signed url', async () => {
  mockState.messageFindOneQueue = [
    {
      _id: 'msg-attach-url-1',
      mediaUrl: 'https://cdn.example.com/image.png',
      mediaType: 'image',
      attachment: {
        publicId: 'inbox/test-user-storage/sent/attach-1',
        username: 'test-user-storage'
      }
    }
  ];

  const { status, data } = await requestJson(
    'GET',
    '/api/messages/attachments/msg-attach-url-1/url?mode=view'
  );

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.match(String(data?.data?.url || ''), /cdn\.example\.com/);
});

test('GET /attachments/:id/download proxies attachment bytes with download headers', async () => {
  mockState.messageFindOneQueue = [
    {
      _id: 'msg-attach-download-1',
      mediaUrl: 'https://cdn.example.com/file.pdf',
      mediaType: 'document',
      attachment: {
        publicId: 'inbox/test-user-storage/sent/file-1',
        username: 'test-user-storage',
        originalFileName: 'nandhakumar-resume.pdf',
        mimeType: 'application/pdf',
        extension: 'pdf',
        resourceType: 'raw'
      }
    }
  ];
  mockState.inboxSignedResult = {
    url: 'https://cdn.example.com/file-download.pdf?signature=1',
    expiresAt: new Date(Date.now() + 300000).toISOString()
  };
  let fetchedDownloadUrl = '';

  global.fetch = async (url, options) => {
    if (String(url || '').startsWith(baseUrl)) {
      return originalFetch(url, options);
    }

    fetchedDownloadUrl = String(url || '');

    return new Response(Buffer.from('pdf-bytes'), {
      status: 200,
      headers: {
        'content-type': 'application/pdf',
        'content-length': String(Buffer.byteLength('pdf-bytes'))
      }
    });
  };

  try {
    const response = await originalFetch(
      `${baseUrl}/api/messages/attachments/msg-attach-download-1/download`,
      {
        method: 'GET',
        headers: {
          Authorization: 'Bearer messages-route-test-user'
        }
      }
    );
    const buffer = Buffer.from(await response.arrayBuffer());

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-type'), 'application/pdf');
    assert.equal(fetchedDownloadUrl, 'https://cdn.example.com/file-download.pdf?signature=1');
    assert.match(
      String(response.headers.get('content-disposition') || ''),
      /nandhakumar-resume\.pdf/
    );
    assert.equal(buffer.toString(), 'pdf-bytes');
  } finally {
    global.fetch = originalFetch;
    mockState.inboxSignedResult = null;
  }
});

test('DELETE /attachments/:id marks attachment deleted', async () => {
  mockState.messageFindOneQueue = [
    {
      _id: 'msg-attach-del-1',
      conversationId: 'conversation-attach-1',
      mediaUrl: 'https://cdn.example.com/image.png',
      attachment: {
        publicId: 'inbox/test-user-storage/sent/attach-1',
        username: 'test-user-storage'
      }
    }
  ];

  const { status, data } = await requestJson('DELETE', '/api/messages/attachments/msg-attach-del-1');

  assert.equal(status, 200);
  assert.equal(data.success, true);
  assert.equal(mockState.inboxDeleteCalls.length, 1);
});
