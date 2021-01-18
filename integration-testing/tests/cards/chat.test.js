const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries, subscriptions} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Unread chat message card with correct format', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we subscribe to our cards
  const handlers = []
  const sub = await ourClient
    .subscribe({query: subscriptions.onCardNotification, variables: {userId: ourUserId}})
    .subscribe({
      next: ({data: {onCardNotification: notification}}) => {
        const handler = handlers.shift()
        expect(handler).toBeDefined()
        handler(notification)
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  const subInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscription initialize
  let nextNotification = new Promise((resolve) => handlers.push(resolve))

  // we start a direct chat with them, verify no card generated for the chat we created or that first message
  const chatId = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.createDirectChat,
      variables: {userId: theirUserId, chatId, messageId: uuidv4(), messageText: 'lore ipsum'},
    })
    .then(({data}) => expect(data.createDirectChat.chatId).toBe(chatId))
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(1)
    expect(user.cards.items).toHaveLength(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })

  // they add a message to the chat, verify a card was generated for their chat message, has correct format
  await theirClient
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: uuidv4(), text: 'lore ipsum'}})
    .then(({data}) => expect(data.addChatMessage.messageId).toBeTruthy())
  await misc.sleep(2000)
  const card1 = await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    const card = user.cards.items[0]
    expect(card.cardId).toBeTruthy()
    expect(card.title).toBe('You have 1 chat with new messages')
    expect(card.subTitle).toBeNull()
    expect(card.action).toBe('https://real.app/chat/')
    expect(card.thumbnail).toBeNull()
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
    return card
  })
  const {thumbnail: card1Thumbnail, ...card1ExcludingThumbnail} = card1
  expect(card1Thumbnail).toBeNull()

  // verify subscription fired correctly with that new card
  await nextNotification.then((notification) => {
    expect(notification.userId).toBe(ourUserId)
    expect(notification.type).toBe('ADDED')
    expect(notification.card).toEqual(card1ExcludingThumbnail)
  })
  nextNotification = new Promise((resolve) => handlers.push(resolve))

  // they add another message to the chat, verify card title has not changed
  await ourClient
    .mutate({
      mutation: mutations.addChatMessage,
      variables: {chatId, messageId: uuidv4(), text: 'lore ipsum'},
    })
    .then(({data}) => expect(data.addChatMessage.messageId).toBeTruthy())
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    expect(user.cards.items[0].title).toBe('You have 1 chat with new messages')
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })

  // they open up a group chat with us, verify our card title changes
  const chatId2 = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.createGroupChat,
      variables: {chatId: chatId2, userIds: [ourUserId, theirUserId], messageId: uuidv4(), messageText: 'm1'},
    })
    .then(({data}) => expect(data.createGroupChat.chatId).toBe(chatId2))
  await misc.sleep(2000)
  const card2 = await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    const card = user.cards.items[0]
    expect(card.title).toBe('You have 2 chats with new messages')
    const {title: cardTitle, ...cardOtherFields} = card
    const {title: card1Title, ...card1OtherFields} = card1
    expect(cardTitle).not.toBe(card1Title)
    expect(cardOtherFields).toEqual(card1OtherFields)
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
    return card
  })
  const {thumbnail: card2Thumbnail, ...card2ExcludingThumbnail} = card2
  expect(card2Thumbnail).toBeNull()

  // verify subscription fired correctly with that changed card
  await nextNotification.then((notification) => {
    expect(notification.userId).toBe(ourUserId)
    expect(notification.type).toBe('EDITED')
    expect(notification.card).toEqual(card2ExcludingThumbnail)
  })
  nextNotification = new Promise((resolve) => handlers.push(resolve))

  // we report to have viewed one of the chats, verify our card title has changed back to original
  await ourClient.mutate({mutation: mutations.reportChatViews, variables: {chatIds: [chatId]}})
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    expect(user.cards.items[0]).toEqual(card1)
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })

  // verify subscription fired correctly with that changed card
  await nextNotification.then((notification) => {
    expect(notification.userId).toBe(ourUserId)
    expect(notification.type).toBe('EDITED')
    expect(notification.card).toEqual(card1ExcludingThumbnail)
  })
  nextNotification = new Promise((resolve) => handlers.push(resolve))

  // we report to have viewed the other chat, verify card has dissapeared
  await ourClient.mutate({mutation: mutations.reportChatViews, variables: {chatIds: [chatId2]}})
  // verify the card has disappeared
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(1)
    expect(user.cards.items).toHaveLength(1)
    // first card is the 'Add a profile photo'
    expect(user.cards.items[0].title).toBe('Add a profile photo')
  })

  // verify subscription fired correctly for card deletion
  await nextNotification.then((notification) => {
    expect(notification.userId).toBe(ourUserId)
    expect(notification.type).toBe('DELETED')
    expect(notification.card).toEqual(card1ExcludingThumbnail)
  })

  // shut down the subscription
  sub.unsubscribe()
  await subInitTimeout
})
