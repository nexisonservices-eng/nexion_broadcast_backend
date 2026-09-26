const test = require('node:test');
const assert = require('node:assert/strict');
const { createCreative, uploadCreativeAsset } = require('../services/metaCreativeService');

const metaError = Object.assign(new Error('Meta creative error'), {
    response: { status: 400, data: { error: { message: 'Invalid creative parameter', code: 100 } } }
});
const dependencies = {
    graphRequest: async () => { throw metaError; },
    buildAdAccountPath: (id, edge) => `${id}/${edge}`,
    extractApiErrorMessage: (error) => error.response?.data?.error?.message || error.message,
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
        graphRequest: async ({ path, data }) => {
            if (path === 'video/thumbnails') return { data: [
                { uri: 'https://example.com/first.jpg' },
                { uri: 'https://example.com/preferred.jpg', is_preferred: true }
            ] };
            const video = data.object_story_spec.video_data;
            assert.equal(video.image_url, 'https://example.com/preferred.jpg');
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

test('Cloudinary videos supply a frame image without polling Meta thumbnails', async () => {
    let posts = 0;
    await createCreative({
        ...dependencies, creative: { mediaType: 'video' },
        creativeUpload: { videoId: 'video', mediaUrl: 'https://res.cloudinary.com/demo/video/upload/v123/company/ad.mp4' },
        graphRequest: async ({ method, data }) => {
            assert.equal(method, 'POST');
            assert.equal(data.object_story_spec.video_data.image_url,
                'https://res.cloudinary.com/demo/video/upload/so_0/v123/company/ad.jpg');
            posts += 1;
            return { data: { id: 'creative' } };
        }
    });
    assert.equal(posts, 1);
});

test('video upload result carries its source URL into the creative thumbnail request', async () => {
    const mediaUrl = 'https://res.cloudinary.com/demo/video/upload/v123/company/ad.mp4';
    const requests = [];
    const graphRequest = async (request) => {
        requests.push(request);
        if (request.path === 'account/advideos') {
            assert.equal(request.data.file_url, mediaUrl);
            return { id: 'uploaded-video' };
        }
        assert.equal(request.path, 'account/adcreatives');
        assert.equal(request.data.object_story_spec.video_data.video_id, 'uploaded-video');
        assert.equal(request.data.object_story_spec.video_data.image_url,
            'https://res.cloudinary.com/demo/video/upload/so_0/v123/company/ad.jpg');
        return { data: { id: 'creative' } };
    };
    const creativeUpload = await uploadCreativeAsset({
        ...dependencies, graphRequest, mediaUrl, mediaType: 'video', adAccountId: 'account',
        shouldUseMockMode: () => false,
        getAccessContextForUser: async () => ({ accessToken: 'test-token', connection: { selectedAdAccountId: 'account' } })
    });
    const result = await createCreative({
        ...dependencies, graphRequest, creativeUpload, creative: { mediaType: 'video' },
        adAccountId: 'account', configuredPageId: 'page', accessToken: 'test-token'
    });
    assert.equal(result.id, 'creative');
    assert.equal(requests.length, 2);
});

test('external video URLs use Meta thumbnails without rewriting the video URL', async () => {
    let reads = 0;
    await createCreative({
        ...dependencies, creative: { mediaType: 'video' },
        creativeUpload: { videoId: 'video', mediaUrl: 'https://example.com/ad.mp4' },
        graphRequest: async ({ path, data }) => {
            if (path === 'video/thumbnails') {
                reads += 1;
                return { data: [{ uri: 'https://example.com/poster.jpg' }] };
            }
            assert.equal(data.object_story_spec.video_data.image_url, 'https://example.com/poster.jpg');
            return { data: { id: 'creative' } };
        }
    });
    assert.equal(reads, 1);
});

test('creative failure preserves Meta error when page context is absent', async () => {
    await assert.rejects(createCreative({
        ...dependencies, campaignName: 'Test', creative: { mediaType: 'video' },
        creativeUpload: { videoId: 'video' }, configuredPageId: 'page', destinationUrl: 'https://example.com',
        graphRequest: async ({ path }) => {
            if (path === 'video/thumbnails') return { data: [{ uri: 'https://example.com/thumbnail.jpg' }] };
            throw metaError;
        }
    }), (error) => {
        assert.equal(error.message, 'Invalid creative parameter');
        assert.equal(error.details.error.code, 100);
        assert.equal(error.details.requestedPageId, 'page');
        assert.equal(error.stage, 'Creative creation');
        return true;
    });
});

test('waits for a video thumbnail before posting the creative', async () => {
    let reads = 0;
    let posts = 0;
    await createCreative({
        ...dependencies, creative: { mediaType: 'video' }, creativeUpload: { videoId: 'video' },
        waitForRetry: async () => {},
        graphRequest: async ({ path, data }) => {
            if (path === 'video/thumbnails') {
                reads += 1;
                return { data: reads === 1 ? [] : [{ uri: 'https://example.com/thumbnail.jpg' }] };
            }
            posts += 1;
            assert.equal(data.object_story_spec.video_data.image_url, 'https://example.com/thumbnail.jpg');
            return { data: { id: 'creative' } };
        }
    });
    assert.equal(reads, 2);
    assert.equal(posts, 1);
});

test('never posts an invalid creative when thumbnails remain unavailable', async () => {
    let reads = 0;
    await assert.rejects(createCreative({
        ...dependencies, creative: { mediaType: 'video' }, creativeUpload: { videoId: 'video' },
        waitForRetry: async () => {},
        graphRequest: async ({ path }) => {
            assert.equal(path, 'video/thumbnails');
            reads += 1;
            return { data: [] };
        }
    }), /thumbnail is still processing/);
    assert.equal(reads, 20);
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
