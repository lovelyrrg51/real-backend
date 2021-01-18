const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Only post owner can use viewedStatus with Post.commentsCount, others see null', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, postType: 'TEXT_ONLY', text: 'lore ipsum'}})
    .then((resp) => {
      expect(resp.data.addPost.postId).toBe(postId)
    })

  // check we see viewed/unviewed comment counts
  await ourClient.query({query: queries.post, variables: {postId}}).then((resp) => {
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.commentsCount).toBe(0)
    expect(resp.data.post.commentsViewedCount).toBe(0)
    expect(resp.data.post.commentsUnviewedCount).toBe(0)
  })

  // check they do not see viewed/unviewed comment counts
  await theirClient.query({query: queries.post, variables: {postId}}).then((resp) => {
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.commentsCount).toBe(0)
    expect(resp.data.post.commentsViewedCount).toBeNull()
    expect(resp.data.post.commentsUnviewedCount).toBeNull()
  })
})

test('Adding comments: Post owners comments always viewed, others comments are unviewed', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, postType: 'TEXT_ONLY', text: 'lore ipsum'}})
    .then((resp) => {
      expect(resp.data.addPost.postId).toBe(postId)
    })

  // check starting state
  await ourClient.query({query: queries.post, variables: {postId}}).then((resp) => {
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.commentsCount).toBe(0)
    expect(resp.data.post.commentsViewedCount).toBe(0)
    expect(resp.data.post.commentsUnviewedCount).toBe(0)
  })

  // we comment on the post
  await ourClient
    .mutate({mutation: mutations.addComment, variables: {commentId: uuidv4(), postId, text: 'lore? ipsum!'}})
    .then((resp) => {
      expect(resp.data.addComment.commentId).toBeTruthy()
    })

  // check that comment was counted viewed
  await misc.sleep(1000)
  await ourClient.query({query: queries.post, variables: {postId}}).then((resp) => {
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.commentsCount).toBe(1)
    expect(resp.data.post.commentsViewedCount).toBe(1)
    expect(resp.data.post.commentsUnviewedCount).toBe(0)
  })

  // they comment on the post
  await theirClient
    .mutate({mutation: mutations.addComment, variables: {commentId: uuidv4(), postId, text: 'lore? ipsum!'}})
    .then((resp) => {
      expect(resp.data.addComment.commentId).toBeTruthy()
    })

  // check that comment was counted unviewed
  await misc.sleep(1000)
  await ourClient.query({query: queries.post, variables: {postId}}).then((resp) => {
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.commentsCount).toBe(2)
    expect(resp.data.post.commentsViewedCount).toBe(1)
    expect(resp.data.post.commentsUnviewedCount).toBe(1)
  })
})

test('Viewing posts: Post owners views clear the unviewed comment counter, others dont', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, postType: 'TEXT_ONLY', text: 'lore ipsum'}})
    .then((resp) => expect(resp.data.addPost.postId).toBe(postId))

  // they comment on the post
  await theirClient
    .mutate({mutation: mutations.addComment, variables: {commentId: uuidv4(), postId, text: 'lore? ipsum!'}})
    .then((resp) => expect(resp.data.addComment.commentId).toBeTruthy())

  // check viewed/unviewed counts
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId}}).then((resp) => {
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.commentsCount).toBe(1)
    expect(resp.data.post.commentsViewedCount).toBe(0)
    expect(resp.data.post.commentsUnviewedCount).toBe(1)
  })

  // they report a post view
  await theirClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})

  // check viewed/unviewed counts - no change
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId}}).then((resp) => {
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.commentsCount).toBe(1)
    expect(resp.data.post.commentsViewedCount).toBe(0)
    expect(resp.data.post.commentsUnviewedCount).toBe(1)
  })

  // we report a post view
  await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})

  // check viewed/unviewed counts - unviewed have become viewed
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId}}).then((resp) => {
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.commentsCount).toBe(1)
    expect(resp.data.post.commentsViewedCount).toBe(1)
    expect(resp.data.post.commentsUnviewedCount).toBe(0)
  })

  // they comment on the post again
  await theirClient
    .mutate({mutation: mutations.addComment, variables: {commentId: uuidv4(), postId, text: 'lore? ipsum!'}})
    .then((resp) => expect(resp.data.addComment.commentId).toBeTruthy())

  // check viewed/unviewed counts - should have a new unviewed
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId}}).then((resp) => {
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.commentsCount).toBe(2)
    expect(resp.data.post.commentsViewedCount).toBe(1)
    expect(resp.data.post.commentsUnviewedCount).toBe(1)
  })

  // we report a post view
  await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})

  // check viewed/unviewed counts - unviewed have become viewed again
  await misc.sleep(2000)
  await ourClient.query({query: queries.post, variables: {postId}}).then((resp) => {
    expect(resp.data.post.postId).toBe(postId)
    expect(resp.data.post.commentsCount).toBe(2)
    expect(resp.data.post.commentsViewedCount).toBe(2)
    expect(resp.data.post.commentsUnviewedCount).toBe(0)
  })
})
