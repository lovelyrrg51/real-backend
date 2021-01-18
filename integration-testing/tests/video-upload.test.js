const fs = require('fs')
const got = require('got')
const moment = require('moment')
const path = require('path')
const tough = require('tough-cookie')
const uuidv4 = require('uuid/v4')

const cognito = require('../utils/cognito')
const misc = require('../utils/misc')
const {mutations, queries} = require('../schema')

const videoData = fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'sample.mov'))
const videoHeaders = {'Content-Type': 'video/quicktime'}
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test(
  'Upload video basic success',
  async () => {
    const {client, userId} = await loginCache.getCleanLogin()

    // add a pending video post
    const postId = uuidv4()
    let variables = {postId, postType: 'VIDEO'}
    let resp = await client.mutate({mutation: mutations.addPost, variables})
    expect(resp.data.addPost.postId).toBe(postId)
    expect(resp.data.addPost.postType).toBe('VIDEO')
    expect(resp.data.addPost.postStatus).toBe('PENDING')
    const videoUploadUrl = resp.data.addPost.videoUploadUrl
    expect(videoUploadUrl).toBeTruthy()

    // upload our video to that url
    await got.put(videoUploadUrl, {headers: videoHeaders, body: videoData})
    await misc.sleepUntilPostProcessed(client, postId, {maxWaitMs: 60 * 1000, pollingIntervalMs: 5 * 1000})

    // verify the basic parts of the post is as we expect
    resp = await client.query({query: queries.post, variables: {postId}})
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.postStatus).toBe('COMPLETED')
    expect(resp.data.post.videoUploadUrl).toBeNull()

    // verify the image urls exist, and we can access them
    const image = resp.data.post.image
    expect(image.url).toBeTruthy()
    expect(image.url4k).toBeTruthy()
    expect(image.url1080p).toBeTruthy()
    expect(image.url480p).toBeTruthy()
    expect(image.url64p).toBeTruthy()
    await got.head(image.url)
    await got.head(image.url4k)
    await got.head(image.url1080p)
    await got.head(image.url480p)
    await got.head(image.url64p)

    // verify the video part of the post is all good
    const videoUrl = resp.data.post.video.urlMasterM3U8
    expect(videoUrl).toContain(userId)
    expect(videoUrl).toContain(postId)
    expect(videoUrl).toContain('hls')
    expect(videoUrl).toContain('video')
    expect(videoUrl).toContain('.m3u8')
    const cookies = resp.data.post.video.accessCookies
    expect(cookies.domain).toBeTruthy()
    expect(cookies.path).toBeTruthy()
    expect(cookies.expiresAt).toBeTruthy()
    expect(cookies.policy).toBeTruthy()
    expect(cookies.signature).toBeTruthy()
    expect(cookies.keyPairId).toBeTruthy()
    expect(videoUrl).toContain(cookies.domain)
    expect(videoUrl).toContain(cookies.path)

    // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Set-Cookie
    const cookieJar = new tough.CookieJar()
    const expires = moment(cookies.expiresAt).toDate().toUTCString()
    const cookieProps = `Secure; Domain=${cookies.domain}; Path=${cookies.path}; Expires=${expires}`
    cookieJar.setCookie(`CloudFront-Policy=${cookies.policy}; ${cookieProps}`, videoUrl)
    cookieJar.setCookie(`CloudFront-Signature=${cookies.signature}; ${cookieProps}`, videoUrl)
    cookieJar.setCookie(`CloudFront-Key-Pair-Id=${cookies.keyPairId}; ${cookieProps}`, videoUrl)

    // will error out if it fails
    await got.get(videoUrl, {cookieJar})

    // make sure the cookies work for other urls that will be needed to play the video
    // note that the exact path here is dependent on the AWS MediaConvert settings
    const anotherUrl = videoUrl.replace(
      'video.m3u8',
      'video_Ott_Hls_Ts_Avc_Aac_16x9_1280x720p_30Hz_3500Kbps.m3u8',
    )
    await got.get(anotherUrl, {cookieJar})
  },
  90 * 1000,
)

test(
  'Create video post in album, move in and out',
  async () => {
    const {client} = await loginCache.getCleanLogin()

    // we add an album
    const albumId = uuidv4()
    let variables = {albumId, name: 'first'}
    let resp = await client.mutate({mutation: mutations.addAlbum, variables})
    expect(resp.data.addAlbum.albumId).toBe(albumId)
    expect(resp.data.addAlbum.postCount).toBe(0)
    expect(resp.data.addAlbum.postsLastUpdatedAt).toBeNull()
    expect(resp.data.addAlbum.posts.items).toHaveLength(0)
    let placeholderAlbumArt = resp.data.addAlbum.art

    // add a pending video post to that album
    const postId = uuidv4()
    variables = {postId, postType: 'VIDEO', albumId}
    resp = await client.mutate({mutation: mutations.addPost, variables})
    expect(resp.data.addPost.postId).toBe(postId)
    expect(resp.data.addPost.postType).toBe('VIDEO')
    expect(resp.data.addPost.postStatus).toBe('PENDING')
    expect(resp.data.addPost.album.albumId).toBe(albumId)
    const videoUploadUrl = resp.data.addPost.videoUploadUrl
    expect(videoUploadUrl).toBeTruthy()

    // upload our video to that url
    await got.put(videoUploadUrl, {headers: videoHeaders, body: videoData})
    await misc.sleepUntilPostProcessed(client, postId, {maxWaitMs: 60 * 1000, pollingIntervalMs: 5 * 1000})

    // verify the appears as we expect
    resp = await client.query({query: queries.post, variables: {postId}})
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.postStatus).toBe('COMPLETED')
    expect(resp.data.post.videoUploadUrl).toBeNull()
    expect(resp.data.post.image.url).toBeTruthy()

    // check the album
    await misc.sleep(2000)
    resp = await client.query({query: queries.album, variables: {albumId}})
    expect(resp.data.album.albumId).toBe(albumId)
    expect(resp.data.album.postCount).toBe(1)
    expect(resp.data.album.posts.items).toHaveLength(1)
    expect(resp.data.album.posts.items[0].postId).toBe(postId)
    let postAlbumArt = resp.data.album.art

    // verify album art urls have changed
    expect(placeholderAlbumArt.url.split('?')[0]).not.toBe(postAlbumArt.url.split('?')[0])
    expect(placeholderAlbumArt.url4k.split('?')[0]).not.toBe(postAlbumArt.url4k.split('?')[0])
    expect(placeholderAlbumArt.url1080p.split('?')[0]).not.toBe(postAlbumArt.url1080p.split('?')[0])
    expect(placeholderAlbumArt.url480p.split('?')[0]).not.toBe(postAlbumArt.url480p.split('?')[0])
    expect(placeholderAlbumArt.url64p.split('?')[0]).not.toBe(postAlbumArt.url64p.split('?')[0])

    // remove the post from the album
    resp = await client.mutate({mutation: mutations.editPostAlbum, variables: {postId}})
    expect(resp.data.editPostAlbum.postId).toBe(postId)
    expect(resp.data.editPostAlbum.album).toBeNull()

    // check the album
    await misc.sleep(2000)
    resp = await client.query({query: queries.album, variables: {albumId}})
    expect(resp.data.album.albumId).toBe(albumId)
    expect(resp.data.album.postCount).toBe(0)
    expect(resp.data.album.posts.items).toHaveLength(0)
    let albumArt = resp.data.album.art

    // verify album art urls are back to placeholders
    expect(placeholderAlbumArt.url.split('?')[0]).toBe(albumArt.url.split('?')[0])
    expect(placeholderAlbumArt.url4k.split('?')[0]).toBe(albumArt.url4k.split('?')[0])
    expect(placeholderAlbumArt.url1080p.split('?')[0]).toBe(albumArt.url1080p.split('?')[0])
    expect(placeholderAlbumArt.url480p.split('?')[0]).toBe(albumArt.url480p.split('?')[0])
    expect(placeholderAlbumArt.url64p.split('?')[0]).toBe(albumArt.url64p.split('?')[0])

    // add the post from the album
    resp = await client.mutate({mutation: mutations.editPostAlbum, variables: {postId, albumId}})
    expect(resp.data.editPostAlbum.postId).toBe(postId)
    expect(resp.data.editPostAlbum.album.albumId).toBe(albumId)

    // check the album
    await misc.sleep(2000)
    resp = await client.query({query: queries.album, variables: {albumId}})
    expect(resp.data.album.albumId).toBe(albumId)
    expect(resp.data.album.postCount).toBe(1)
    expect(resp.data.album.posts.items).toHaveLength(1)
    expect(resp.data.album.posts.items[0].postId).toBe(postId)
    albumArt = resp.data.album.art

    // verify album art urls are back to those with the posts
    expect(postAlbumArt.url.split('?')[0]).toBe(albumArt.url.split('?')[0])
    expect(postAlbumArt.url4k.split('?')[0]).toBe(albumArt.url4k.split('?')[0])
    expect(postAlbumArt.url1080p.split('?')[0]).toBe(albumArt.url1080p.split('?')[0])
    expect(postAlbumArt.url480p.split('?')[0]).toBe(albumArt.url480p.split('?')[0])
    expect(postAlbumArt.url64p.split('?')[0]).toBe(albumArt.url64p.split('?')[0])
  },
  90 * 1000,
)
