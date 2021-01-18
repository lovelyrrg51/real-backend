const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const imageData = misc.generateRandomJpeg(8, 8)
const imageDataB64 = new Buffer.from(imageData).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('PostRepost card generation and format, fullfilling card', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId, username: theirUsername} = await loginCache.getCleanLogin()

  // we add an image post
  const originalPostId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: originalPostId, imageData: imageDataB64}})
    .then(({data}) => expect(data.addPost.postId).toBe(originalPostId))

  // check we have no cards
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })

  // they add an image post, same image as ours - ie a repost
  const postId = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData: imageDataB64}})
    .then(({data}) => expect(data.addPost.postId).toBe(postId))

  // verify a card was generated for us, check format
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    let card = user.cards.items[0]
    expect(card.cardId).toBeTruthy()
    expect(card.title).toMatch(RegExp('^@.* reposted one of your posts'))
    expect(card.title).toContain(theirUsername)
    expect(card.subTitle).toBeNull()
    expect(card.action).toMatch(RegExp('^https://real.app/user/.*/post/.*'))
    expect(card.action).toContain(theirUserId)
    expect(card.action).toContain(postId)
    expect(card.thumbnail).toBeTruthy()
    expect(card.thumbnail.url64p).toMatch(RegExp('^https://.*.jpg'))
    expect(card.thumbnail.url480p).toMatch(RegExp('^https://.*.jpg'))
    expect(card.thumbnail.url1080p).toMatch(RegExp('^https://.*.jpg'))
    expect(card.thumbnail.url4k).toMatch(RegExp('^https://.*.jpg'))
    expect(card.thumbnail.url).toMatch(RegExp('^https://.*.jpg'))
    expect(card.thumbnail.url64p).toContain(postId)
    expect(card.thumbnail.url480p).toContain(postId)
    expect(card.thumbnail.url1080p).toContain(postId)
    expect(card.thumbnail.url4k).toContain(postId)
    expect(card.thumbnail.url).toContain(postId)
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })

  // we view our post, verify no change to cards
  await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [originalPostId]}})
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(2)
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })

  // we view their post, verify card disappears
  await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })
})

test('PostRepost card deleted when post deleted', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add an image post
  const originalPostId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: originalPostId, imageData: imageDataB64}})
    .then(({data}) => expect(data.addPost.postId).toBe(originalPostId))

  // they add an image post, same image as ours - ie a repost
  const postId = uuidv4()
  await misc.sleep(2000)
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData: imageDataB64}})
    .then(({data}) => expect(data.addPost.postId).toBe(postId))

  // verify a card was generated for us
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(2)
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })

  // they delete their post
  await theirClient
    .mutate({mutation: mutations.deletePost, variables: {postId}})
    .then(({data}) => expect(data.deletePost.postId).toBe(postId))

  // verify our card disappears
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })
})
