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
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('PostMention card generation and format for image post, fullfilling and dismissing card', async () => {
  const {client: ourClient, userId: ourUserId, username: ourUsername} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId, username: other1Username} = await loginCache.getCleanLogin()
  const {client: other2Client, userId: other2UserId, username: other2Username} = await loginCache.getCleanLogin()

  // we add an image post and tag both users
  const postId = uuidv4()
  const text = `hey @${other1Username} and @${other2Username}, what's up?`
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData: imageDataB64, text}})
    .then(({data}) => expect(data.addPost.postId).toBe(postId))

  // verify a card was generated for other1, check format
  await misc.sleep(2000)
  const cardId1 = await other1Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(other1UserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    let card = user.cards.items[0]
    expect(card.cardId).toBeTruthy()
    expect(card.title).toMatch(RegExp('^@.* tagged you in a post'))
    expect(card.title).toContain(ourUsername)
    expect(card.subTitle).toBeNull()
    expect(card.action).toMatch(RegExp('^https://real.app/user/.*/post/.*'))
    expect(card.action).toContain(ourUserId)
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
    return card.cardId
  })

  // verify a card was generated for other2, check format
  await other2Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(other2UserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    let card = user.cards.items[0]
    expect(card.cardId).toBeTruthy()
    expect(card.title).toMatch(RegExp('^@.* tagged you in a post'))
    expect(card.title).toContain(ourUsername)
    expect(card.subTitle).toBeNull()
    expect(card.action).toMatch(RegExp('^https://real.app/user/.*/post/.*'))
    expect(card.action).toContain(ourUserId)
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
  await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})
  await misc.sleep(2000)
  await other1Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(2)
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })
  await other2Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(2)
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })

  // other1 dismisses the card, verify gone
  await other1Client.mutate({mutation: mutations.deleteCard, variables: {cardId: cardId1}}).then(({data}) => {
    expect(data.deleteCard.cardId).toBe(cardId1)
  })
  await misc.sleep(2000)
  await other1Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })

  // other2 views the post, verify card disappears
  await other2Client.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})
  await misc.sleep(2000)
  await other1Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })
})

test('PostMention card generation for editing text-only post, post deletion', async () => {
  const {client: ourClient, userId: ourUserId, username: ourUsername} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId, username: other1Username} = await loginCache.getCleanLogin()
  const {client: other2Client, userId: other2UserId, username: other2Username} = await loginCache.getCleanLogin()

  // we add a text-only post and tag one user
  const postId = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.addPost,
      variables: {postId, postType: 'TEXT_ONLY', text: `hey @${other1Username}, what's up?`},
    })
    .then(({data}) => expect(data.addPost.text).toContain(other1Username))

  // verify a card was generated for only tagged user, check format
  await misc.sleep(2000)
  await other1Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(other1UserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    let card = user.cards.items[0]
    expect(card.cardId).toBeTruthy()
    expect(card.title).toMatch(RegExp('^@.* tagged you in a post'))
    expect(card.title).toContain(ourUsername)
    expect(card.subTitle).toBeNull()
    expect(card.action).toMatch(RegExp('^https://real.app/user/.*/post/.*'))
    expect(card.action).toContain(ourUserId)
    expect(card.action).toContain(postId)
    expect(card.thumbnail).toBeNull()
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })
  await other2Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(1)
    expect(user.cards.items).toHaveLength(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })

  // we edit the text on the post to now tag the other user
  await ourClient
    .mutate({
      mutation: mutations.editPost,
      variables: {postId, text: `hey @${other2Username}, what's up?`},
    })
    .then(({data}) => expect(data.editPost.text).toContain(other2Username))

  // verify first card still exists, and a card was generated for other2, check format
  await misc.sleep(2000)
  await other1Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(2)
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })
  await other2Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(other2UserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    let card = user.cards.items[0]
    expect(card.cardId).toBeTruthy()
    expect(card.title).toMatch(RegExp('^@.* tagged you in a post'))
    expect(card.title).toContain(ourUsername)
    expect(card.subTitle).toBeNull()
    expect(card.action).toMatch(RegExp('^https://real.app/user/.*/post/.*'))
    expect(card.action).toContain(ourUserId)
    expect(card.action).toContain(postId)
    expect(card.thumbnail).toBeNull()
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })

  // we delete our post, verify the two cards disappear
  await ourClient
    .mutate({mutation: mutations.deletePost, variables: {postId}})
    .then(({data}) => expect(data.deletePost.postStatus).toBe('DELETING'))
  await misc.sleep(2000)
  await other1Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })
  await other2Client.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.cardCount).toBe(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })
})
