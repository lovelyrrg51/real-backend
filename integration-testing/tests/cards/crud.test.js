const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const {mutations, queries} = require('../../schema')
const misc = require('../../utils/misc')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Cards are private to user themselves', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // verify we see our zero cards and count on self
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(1)
    expect(user.cards.items).toHaveLength(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })

  // verify we see our zero cards and count on user
  await ourClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(1)
    expect(user.cards.items).toHaveLength(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })

  // verify they don't see our zero cards and count
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBeNull()
    expect(user.cards).toBeNull()
  })
})

test('List cards', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // verify list & count for no cards
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(1)
    expect(user.cards.items).toHaveLength(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })

  // they start a direct chat with us
  const chatId = uuidv4()
  let variables = {userId: ourUserId, chatId, messageId: uuidv4(), messageText: 'lore ipsum'}
  await theirClient
    .mutate({mutation: mutations.createDirectChat, variables})
    .then(({data: {createDirectChat}}) => {
      expect(createDirectChat.chatId).toBe(chatId)
    })

  // verify list & count that one card
  await misc.sleep(2000) // dynamo
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })

  // we add a post
  const postId = uuidv4()
  variables = {postId, postType: 'TEXT_ONLY', text: 'lore ipsum'}
  await ourClient.mutate({mutation: mutations.addPost, variables}).then(({data: {addPost}}) => {
    expect(addPost.postId).toBe(postId)
  })

  // they comment on our post
  variables = {commentId: uuidv4(), postId, text: 'nice post'}
  await theirClient.mutate({mutation: mutations.addComment, variables})

  // verify list & count for those two cards, including order (most recent first)
  await misc.sleep(2000) // dynamo
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(3)
    expect(user.cards.items).toHaveLength(3)
    expect(user.cards.items[0].action).toContain('https://real.app/')
    expect(user.cards.items[1].action).toContain('https://real.app/')
    // third card is the 'Add a profile photo'
    expect(user.cards.items[2].title).toBe('Add a profile photo')
  })
})

test('Delete card, generate new card after deleting', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // verify can't delete card that doesn't exist
  await expect(ourClient.mutate({mutation: mutations.deleteCard, variables: {cardId: uuidv4()}})).rejects.toThrow(
    /ClientError: No card .* found/,
  )

  // they start a direct chat with us, verify generates us a card
  const chatId = uuidv4()
  await theirClient
    .mutate({
      mutation: mutations.createDirectChat,
      variables: {userId: ourUserId, chatId, messageId: uuidv4(), messageText: 'lore ipsum'},
    })
    .then(({data}) => expect(data.createDirectChat.chatId).toBe(chatId))
  await misc.sleep(2000) // dynamo
  const card = await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    expect(user.cards.items[0].cardId).toBeTruthy()
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
    return user.cards.items[0]
  })

  // verify they can't delete our card
  await expect(
    theirClient.mutate({mutation: mutations.deleteCard, variables: {cardId: card.cardId}}),
  ).rejects.toThrow(/ClientError: Caller.* does not own Card /)

  // verify we can delete our card
  await ourClient
    .mutate({mutation: mutations.deleteCard, variables: {cardId: card.cardId}})
    .then(({data}) => expect(data.deleteCard).toEqual(card))
  await misc.sleep(2000) // dynamo
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(1)
    expect(user.cards.items).toHaveLength(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })

  // they add a message to a chat that already has new messages - verify no new card generated
  await theirClient
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: uuidv4(), text: 'lore ipsum'}})
    .then(({data}) => expect(data.addChatMessage.messageId).toBeTruthy())
  await misc.sleep(2000) // dynamo
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(1)
    expect(user.cards.items).toHaveLength(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })

  // they open up a group chat with us, verify card generated same as old one with a different title
  await theirClient
    .mutate({
      mutation: mutations.createGroupChat,
      variables: {chatId: uuidv4(), userIds: [ourUserId], messageId: uuidv4(), messageText: 'm1'},
    })
    .then(({data}) => expect(data.createGroupChat.chatId).toBeTruthy())
  await misc.sleep(3000) // dynamo
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    expect(user.cards.items[0].cardId).toBe(card.cardId)
    expect(user.cards.items[0].title).not.toBe(card.title)
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })
})
