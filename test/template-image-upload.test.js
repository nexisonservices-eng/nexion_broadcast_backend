const test = require('node:test');
const assert = require('node:assert/strict');
const axios = require('axios');
const service = require('../services/whatsappService');
const controller = require('../controllers/templateController');
const Template = require('../models/Template');

const image = { buffer: Buffer.from('sample-image'), mimetype: 'image/png', originalname: 'sample.png' };
const credentials = { accessToken: 'tenant-token', businessAccountId: 'waba', phoneNumberId: 'phone' };
const mockResponse = () => ({
  statusCode: 200,
  status(code) { this.statusCode = code; return this; },
  json(body) { this.body = body; return this; }
});

test('template upload sends raw image bytes and returns the resumable handle', async (t) => {
  const calls = [];
  t.mock.method(axios, 'post', async (...args) => {
    calls.push(args);
    return { data: calls.length === 1 ? { id: 'upload:session?sig=abc' } : { h: '4:valid-handle' } };
  });
  const result = await service.uploadTemplateImage(image, credentials);
  assert.deepEqual(result, { success: true, data: { headerHandle: '4:valid-handle' } });
  assert.equal(calls[0][0], `${service.apiUrl}/app/uploads`);
  assert.deepEqual(calls[0][2].params, { file_name: 'sample.png', file_length: image.buffer.length, file_type: 'image/png' });
  assert.equal(calls[1][0], `${service.apiUrl}/upload:session?sig=abc`);
  assert.equal(calls[1][1], image.buffer);
  assert.equal(calls[1][2].headers.file_offset, '0');
  assert.equal(calls[1][2].headers.Authorization, 'OAuth tenant-token');
  assert.equal(calls[1][2].headers['Content-Type'], 'image/png');
});

test('template upload retains tenant credentials while another request initializes the service', async (t) => {
  const authorizations = [];
  t.mock.method(axios, 'post', async (url, data, options) => {
    authorizations.push(options.headers.Authorization);
    if (url.endsWith('/app/uploads')) {
      service.initialize({ accessToken: 'other-tenant' });
      return { data: { id: 'upload:session' } };
    }
    return { data: { h: '4:valid-handle' } };
  });
  assert.equal((await service.uploadTemplateImage(image, credentials)).success, true);
  assert.deepEqual(authorizations, ['Bearer tenant-token', 'OAuth tenant-token']);
});

test('missing, unsupported, and oversized images never reach Meta', async (t) => {
  const post = t.mock.method(axios, 'post', async () => { throw new Error('Unexpected request'); });
  for (const file of [null, { ...image, mimetype: 'image/gif' }, { ...image, buffer: Buffer.alloc(5 * 1024 * 1024 + 1) }]) {
    assert.equal((await service.uploadTemplateImage(file, credentials)).success, false);
  }
  assert.equal(post.mock.callCount(), 0);
});

test('Meta upload rejection preserves its actionable message', async (t) => {
  t.mock.method(axios, 'post', async () => {
    throw Object.assign(new Error('Request failed'), {
      response: { data: { error: { message: 'Invalid parameter', error_user_msg: 'Reconnect WhatsApp.' } } }
    });
  });
  const result = await service.uploadTemplateImage(image, credentials);
  assert.equal(result.success, false);
  assert.equal(result.error, 'Reconnect WhatsApp.');
});

test('missing upload session or handle fails the upload', async (t) => {
  const post = t.mock.method(axios, 'post', async () => ({ data: {} }));
  assert.equal((await service.uploadTemplateImage(image, credentials)).success, false);
  post.mock.mockImplementation(async (url) => ({ data: url.endsWith('/app/uploads') ? { id: 'upload:session' } : {} }));
  assert.equal((await service.uploadTemplateImage(image, credentials)).success, false);
});

test('placeholder and numeric media IDs are rejected before saving a template', async (t) => {
  const create = t.mock.method(Template, 'create', async () => { throw new Error('Unexpected save'); });
  for (const handle of ['example', '123456', 'https://example.com/image.png', '']) {
    const res = mockResponse();
    await controller.createTemplate({ body: {
      name: 'test_image', components: [
        { type: 'HEADER', format: 'IMAGE', example: { header_handle: [handle] } },
        { type: 'BODY', text: 'Test' }
      ]
    } }, res);
    assert.equal(res.statusCode, 400);
    assert.match(res.body.error, /Upload a JPEG or PNG/);
  }
  assert.equal(create.mock.callCount(), 0);
});

test('image template submission keeps its preview URL separate from the Meta handle', async (t) => {
  let saved;
  let submitted;
  t.mock.method(Template, 'findOne', async () => null);
  t.mock.method(Template, 'create', async (data) => {
    saved = data;
    return { ...data, _id: 'local-template' };
  });
  t.mock.method(Template, 'findOneAndUpdate', async (filter, data) => ({ ...saved, ...data }));
  t.mock.method(service, 'createTemplate', async (data) => {
    submitted = data;
    return { success: true, data: { id: 'meta-template' } };
  });
  const res = mockResponse();
  await controller.createTemplate({
    user: { id: 'user' }, companyId: 'company', whatsappCredentials: credentials,
    body: {
      name: 'test_image',
      content: { header: { type: 'image', mediaUrl: 'https://example.com/image.png' } },
      components: [
        { type: 'HEADER', format: 'IMAGE', example: { header_handle: ['4:valid-handle'] } },
        { type: 'BODY', text: 'Test' }
      ]
    }
  }, res);
  assert.equal(res.body.success, true);
  assert.equal(saved.content.header.mediaUrl, 'https://example.com/image.png');
  assert.equal(saved.content.header.mediaHandle, '4:valid-handle');
  assert.deepEqual(submitted.components[0].example.header_handle, ['4:valid-handle']);
});
