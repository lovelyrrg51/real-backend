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

test('CommenttMention card generation and format, fullfilling and dismissing card', async () => {
  const {client: ourClient, userId: ourUserId, username: ourUsername} = await loginCache.getCleanLogin()
  const {client: theirClient, username: theirUsername} = await loginCache.getCleanLogin()
  const {client: otherClient, userId: otherUserId, username: otherUsername} = await loginCache.getCleanLogin()

  // we add a post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData: imageDataB64}})
    .then(({data}) => expect(data.addPost.postId).toBe(postId))

  // they comment on our post and tag us and other
  const commentId = uuidv4()
  const text = `hey @${ourUsername} and @${otherUsername}, como va?`
  await theirClient
    .mutate({mutation: mutations.addComment, variables: {commentId, postId, text}})
    .then(({data}) => expect(data.addComment.commentId).toBe(commentId))

  // verify a card was generated for us, check format
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(3) // one of these is for the comment, another is for the mention
    expect(user.cards.items).toHaveLength(3)
    let card = user.cards.items[0].cardId.includes('COMMENT_MENTION') ? user.cards.items[0] : user.cards.items[1]
    expect(card.cardId).toBeTruthy()
    expect(card.title).toMatch(RegExp('^@.* mentioned you in a comment'))
    expect(card.title).toContain(theirUsername)
    expect(card.subTitle).toBeNull()
    expect(card.action).toMatch(RegExp('^https://real.app/user/.*/post/.*/comments/.*'))
    expect(card.action).toContain(ourUserId)
    expect(card.action).toContain(postId)
    expect(card.action).toContain(commentId)
    expect(card.thumbnail).toBeTruthy() // we get the post's thumbnail here
    // third card is the 'Add a profile photo'
    expect(user.cards.items[2].title).toBe('Add a profile photo')
    return card.cardId
  })

  // verify a card was generated for other, check format
  const cardId2 = await otherClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(otherUserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    let card = user.cards.items[0]
    expect(card.cardId).toBeTruthy()
    expect(card.title).toMatch(RegExp('^@.* mentioned you in a comment'))
    expect(card.title).toContain(theirUsername)
    expect(card.subTitle).toBeNull()
    expect(card.action).toMatch(RegExp('^https://real.app/user/.*/post/.*/comments/.*'))
    expect(card.action).toContain(ourUserId)
    expect(card.action).toContain(postId)
    expect(card.action).toContain(commentId)
    expect(card.thumbnail).toBeTruthy() // we get the post's thumbnail here
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
    return card.cardId
  })

  // they view the post, verify no change to cards
  await theirClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => expect(user.cardCount).toBe(3))
  await otherClient.query({query: queries.self}).then(({data: {self: user}}) => expect(user.cardCount).toBe(2))

  // other dismisses their card, verify gone
  await otherClient.mutate({mutation: mutations.deleteCard, variables: {cardId: cardId2}}).then(({data}) => {
    expect(data.deleteCard.cardId).toBe(cardId2)
  })
  await misc.sleep(2000)
  await otherClient.query({query: queries.self}).then(({data: {self: user}}) => expect(user.cardCount).toBe(1))

  // we view the post, verify both our cards disappear
  await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId]}})
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => expect(user.cardCount).toBe(1))
})

test('CommenttMention card deletion on comment deletion', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: other1Client, username: other1Username} = await loginCache.getCleanLogin()
  const {client: other2Client, username: other2Username} = await loginCache.getCleanLogin()

  // we add a text-only post
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, postType: 'TEXT_ONLY', text: 'lore ipsum'}})
    .then(({data}) => expect(data.addPost.postId).toBe(postId))

  // we comment on the post, tagging the other users
  const commentId = uuidv4()
  const text = `hey @${other1Username} and @${other2Username}, como va?`
  await ourClient
    .mutate({mutation: mutations.addComment, variables: {commentId, postId, text}})
    .then(({data}) => expect(data.addComment.commentId).toBe(commentId))

  // verify both users see cards
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

  // we delete the comment
  await ourClient
    .mutate({mutation: mutations.deleteComment, variables: {commentId}})
    .then(({data}) => expect(data.deleteComment.commentId).toBe(commentId))

  // verify both cards have disappeared
  await misc.sleep(2000)
  await other1Client.query({query: queries.self}).then(({data: {self: user}}) => expect(user.cardCount).toBe(1))
  await other2Client.query({query: queries.self}).then(({data: {self: user}}) => expect(user.cardCount).toBe(1))
})
