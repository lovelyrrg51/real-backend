const fs = require('fs')
const got = require('got')
const path = require('path')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const imageBytes = fs.readFileSync(path.join(__dirname, '..', '..', 'fixtures', 'grant.jpg'))
const imageData = new Buffer.from(imageBytes).toString('base64')
const imageHeaders = {'Content-Type': 'image/jpeg'}
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Archive an image post', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we upload an image post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId)
      expect(post.image).toBeTruthy()
    })

  // check we see that post in the feed and in the posts
  await misc.sleep(2000)
  await ourClient.query({query: queries.selfFeed}).then(({data: {self: user}}) => {
    expect(user.feed.items).toHaveLength(1)
    expect(user.feed.items[0].postId).toBe(postId)
    expect(user.feed.items[0].image.url).toBeTruthy()
    expect(user.feed.items[0].imageUploadUrl).toBeNull()
  })
  await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.posts.items).toHaveLength(1)
    expect(user.posts.items[0].postId).toBe(postId)
  })

  // archive the post
  await ourClient
    .mutate({mutation: mutations.archivePost, variables: {postId}})
    .then(({data: {archivePost: post}}) => expect(post.postStatus).toBe('ARCHIVED'))

  // post should be gone from the normal queries - feed, posts
  await misc.sleep(2000)
  await ourClient
    .query({query: queries.selfFeed})
    .then(({data: {self: user}}) => expect(user.feed.items).toHaveLength(0))
  await ourClient
    .query({query: queries.userPosts, variables: {userId: ourUserId}})
    .then(({data: {user}}) => expect(user.posts.items).toHaveLength(0))

  // post should be visible when specifically requesting archived posts
  await ourClient
    .query({query: queries.userPosts, variables: {userId: ourUserId, postStatus: 'ARCHIVED'}})
    .then(({data: {user}}) => {
      expect(user.posts.items).toHaveLength(1)
      expect(user.posts.items[0].postId).toBe(postId)
    })
})

test('Cant archive a post in PENDING status', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // we create a post, leave it with pending status
  const postId = uuidv4()
  await ourClient.mutate({mutation: mutations.addPost, variables: {postId}}).then(({data: {addPost: post}}) => {
    expect(post.postId).toBe(postId)
    expect(post.postStatus).toBe('PENDING')
  })

  // verify we can't archive that post
  await expect(ourClient.mutate({mutation: mutations.archivePost, variables: {postId}})).rejects.toThrow(
    /ClientError: Cannot archive post with status /,
  )
})

test('Cant archive a post or restore an archived post if we are disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we create a post
  const postId = uuidv4()
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // we archive that post
  resp = await ourClient.mutate({mutation: mutations.archivePost, variables: {postId}})
  expect(resp.data.archivePost.postId).toBe(postId)
  expect(resp.data.archivePost.postStatus).toBe('ARCHIVED')

  // we create a second post
  const postId2 = uuidv4()
  resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId: postId2, imageData}})
  expect(resp.data.addPost.postId).toBe(postId2)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // we disable ourselves
  resp = await ourClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(ourUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify we can't archive the second post
  await expect(ourClient.mutate({mutation: mutations.archivePost, variables: {postId: postId2}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )

  // verify we can't restore the first post
  await expect(ourClient.mutate({mutation: mutations.restoreArchivedPost, variables: {postId}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )
})

test('Archiving an image post does not affect image urls', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // we uplaod an image post
  const postId = uuidv4()
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)
  const image = resp.data.addPost.image
  expect(image.url).toBeTruthy()

  // archive the post
  resp = await ourClient.mutate({mutation: mutations.archivePost, variables: {postId}})
  expect(resp.data.archivePost.postStatus).toBe('ARCHIVED')
  expect(resp.data.archivePost.imageUploadUrl).toBeNull()
  expect(resp.data.archivePost.image.url).toBeTruthy()

  // check the url bases have not changed
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.postId).toBe(postId)
  expect(resp.data.post.postStatus).toBe('ARCHIVED')
  const newImage = resp.data.post.image
  expect(image.url.split('?')[0]).toBe(newImage.url.split('?')[0])
  expect(image.url4k.split('?')[0]).toBe(newImage.url4k.split('?')[0])
  expect(image.url1080p.split('?')[0]).toBe(newImage.url1080p.split('?')[0])
  expect(image.url480p.split('?')[0]).toBe(newImage.url480p.split('?')[0])
  expect(image.url64p.split('?')[0]).toBe(newImage.url64p.split('?')[0])
})

test('Restoring an archived image post', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we upload an image post
  const postId = uuidv4()
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.image).toBeTruthy()

  // archive the post
  resp = await ourClient.mutate({mutation: mutations.archivePost, variables: {postId}})
  expect(resp.data.archivePost.postStatus).toBe('ARCHIVED')
  expect(resp.data.archivePost.image).toBeTruthy()

  // restore the post
  resp = await ourClient.mutate({mutation: mutations.restoreArchivedPost, variables: {postId}})
  expect(resp.data.restoreArchivedPost.postStatus).toBe('COMPLETED')
  expect(resp.data.restoreArchivedPost.image).toBeTruthy()

  // check we see that post in the feed and in the posts
  await misc.sleep(2000)
  resp = await ourClient.query({query: queries.selfFeed})
  expect(resp.data.self.feed.items).toHaveLength(1)
  expect(resp.data.self.feed.items[0].postId).toBe(postId)
  expect(resp.data.self.feed.items[0].imageUploadUrl).toBeNull()
  expect(resp.data.self.feed.items[0].image.url).toBeTruthy()

  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId}})
  expect(resp.data.user.posts.items).toHaveLength(1)
  expect(resp.data.user.posts.items[0].postId).toBe(postId)

  // post should not be visible when specifically requesting archived posts
  resp = await ourClient.query({query: queries.userPosts, variables: {userId: ourUserId, postStatus: 'ARCHIVED'}})
  expect(resp.data.user.posts.items).toHaveLength(0)
})

test('Attempts to restore invalid posts', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const postId = uuidv4()

  // verify can't restore a post that doens't exist
  await expect(
    ourClient.mutate({
      mutation: mutations.restoreArchivedPost,
      variables: {postId},
    }),
  ).rejects.toThrow('does not exist')

  // create a post
  let variables = {postId, imageData}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)

  // verify can't restore that non-archived post
  await expect(
    ourClient.mutate({
      mutation: mutations.restoreArchivedPost,
      variables: {postId},
    }),
  ).rejects.toThrow('is not archived')

  // archive the post
  resp = await ourClient.mutate({mutation: mutations.archivePost, variables: {postId}})
  expect(resp.data.archivePost.postStatus).toBe('ARCHIVED')

  // verify another user can't restore our archived our post
  const {client: theirClient} = await loginCache.getCleanLogin()
  await expect(
    theirClient.mutate({
      mutation: mutations.restoreArchivedPost,
      variables: {postId},
    }),
  ).rejects.toThrow("another User's post")

  // verify we can restore our archvied post
  resp = await ourClient.mutate({mutation: mutations.restoreArchivedPost, variables: {postId}})
  expect(resp.data.restoreArchivedPost.postStatus).toBe('COMPLETED')
})

test('Post count reacts to user archiving posts', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // verify count starts at zero
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => expect(user.postCount).toBe(0))

  // add image post with direct image data upload, verify post count goes up
  const postId1 = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, imageData}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('COMPLETED')
    })
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => expect(user.postCount).toBe(1))

  // add a image post, verify count does not go up immediately
  const postId2 = uuidv4()
  const uploadUrl = await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('PENDING')
      return post.imageUploadUrl
    })
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => expect(user.postCount).toBe(1))

  // upload the image for the post, verify post completes and count now goes up
  await got.put(uploadUrl, {headers: imageHeaders, body: imageBytes})
  await misc.sleepUntilPostProcessed(ourClient, postId2)
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId: postId2}}).then(({data: {post}}) => {
    expect(post.postStatus).toBe('COMPLETED')
    expect(post.postedBy.postCount).toBe(2) // count has incremented
  })
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => expect(user.postCount).toBe(2))

  // archive that post, verify count goes down
  await ourClient
    .mutate({mutation: mutations.archivePost, variables: {postId: postId2}})
    .then(({data: {archivePost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('ARCHIVED')
    })
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => expect(user.postCount).toBe(1))

  // cant test an expiring post is removed from the count yet,
  // because that is done in a cron-like job
  // add a way for the test suite to artificially trigger that job?
})

test('Cant archive a post that is not ours', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // they add a post
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // verify we cannot archive that post for them
  await expect(
    ourClient.mutate({
      mutation: mutations.archivePost,
      variables: {postId},
    }),
  ).rejects.toThrow("Cannot archive another User's post")
})

test('When a post is archived, any likes of it disappear', async () => {
  // us and them, they add a post
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()
  const postId = uuidv4()
  let variables = {postId, imageData}
  let resp = await theirClient.mutate({mutation: mutations.addPost, variables})

  // we onymously like it
  resp = await ourClient.mutate({mutation: mutations.onymouslyLikePost, variables: {postId}})

  // they anonymously like it
  resp = await theirClient.mutate({mutation: mutations.anonymouslyLikePost, variables: {postId}})

  // verify the post is now in the like lists
  resp = await theirClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post.onymouslyLikedBy.items).toHaveLength(1)
  expect(resp.data.post.onymouslyLikedBy.items[0].userId).toBe(ourUserId)

  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.onymouslyLikedPosts.items[0].postId).toBe(postId)

  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(1)
  expect(resp.data.self.anonymouslyLikedPosts.items[0].postId).toBe(postId)

  // archive the post
  resp = await theirClient.mutate({mutation: mutations.archivePost, variables: {postId}})
  expect(resp.data.archivePost.postStatus).toBe('ARCHIVED')

  // verify we can no longer see the post
  resp = await ourClient.query({query: queries.post, variables: {postId}})
  expect(resp.data.post).toBeNull()

  // verify the post has disappeared from the like lists
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.onymouslyLikedPosts.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.anonymouslyLikedPosts.items).toHaveLength(0)
})
