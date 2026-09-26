const test = require('node:test');
const assert = require('node:assert/strict');
const { createCreative, uploadCreativeAsset } = require('../services/metaCreativeService');

const metaError = Object.assign(new Error('Meta creative error'), {
    response: { status: 400, data: { error: { message: 'Invalid creative parameter', code: 100 } } }
});
const dependencies = {
    graphRequest: async () => { throw metaError; },
    buildAdAccountPath: (id, edge) => `${id}/${edge}`,
    extractApiErrorMessage: (error) => error.response.data.error.message,
    buildStageErrorWithDetails: (stage, message, details, status) => Object.assign(new Error(message), { stage, details, status })
};

test('video creative with a description publishes without the unsupported video_data field', async () => {
    const result = await createCreative({
        ...dependencies,
        campaignName: 'Video campaign',
        configuredPageId: 'page',
        destinationUrl: 'https://example.com',
        creative: { mediaType: 'video', primaryText: 'Watch this', headline: 'Headline', description: 'Saved description' },
        creativeUpload: { videoId: 'video' },
        graphRequest: async ({ data }) => {
            const video = data.object_story_spec.video_data;
            assert.equal(Object.hasOwn(video, 'description'), false);
            assert.equal(video.video_id, 'video');
            assert.equal(video.message, 'Watch this');
            assert.equal(video.title, 'Headline');
            return { data: { id: 'creative' } };
        }
    });
    assert.equal(result.id, 'creative');
});

test('image creatives retain their link description', async () => {
    await createCreative({
        ...dependencies, campaignName: 'Image campaign',
        creative: { mediaType: 'image', description: 'Image description' },
        creativeUpload: { mediaHash: 'image' },
        graphRequest: async ({ data }) => {
            assert.equal(data.object_story_spec.link_data.description, 'Image description');
            return { data: { id: 'creative' } };
        }
    });
});

test('creative failure preserves Meta error when page context is absent', async () => {
    await assert.rejects(createCreative({
        ...dependencies, campaignName: 'Test', creative: { mediaType: 'video' },
        creativeUpload: { videoId: 'video' }, configuredPageId: 'page', destinationUrl: 'https://example.com'
    }), (error) => {
        assert.equal(error.message, 'Invalid creative parameter');
        assert.equal(error.details.error.code, 100);
        assert.equal(error.details.requestedPageId, 'page');
        assert.equal(error.stage, 'Creative creation');
        return true;
    });
});

test('upload failure preserves original error when access context is resolved internally', async () => {
    await assert.rejects(uploadCreativeAsset({
        ...dependencies, mediaUrl: 'https://example.com/video.mp4', mediaType: 'video',
        shouldUseMockMode: () => false,
        getAccessContextForUser: async () => ({ source: 'user', accessToken: 'test', connection: { selectedAdAccountId: 'account' } })
    }), (error) => {
        assert.equal(error.message, 'Invalid creative parameter');
        assert.equal(error.details.tokenSource, 'user');
        assert.equal(error.stage, 'Creative upload');
        return true;
    });
});
