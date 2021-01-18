const uuidv4 = require('uuid/v4')
// the aws-appsync-subscription-link pacakge expects WebSocket to be globaly defined, like in the browser
global.WebSocket = require('ws')

const cognito = require('../utils/cognito')
const misc = require('../utils/misc')
const {mutations, subscriptions} = require('../schema')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Notification trigger cannot be called from external graphql client', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // create a well-formed valid chat notification object
  const mutation = mutations.triggerNotification
  const variables = {
    input: {
      userId: ourUserId,
      type: 'USER_CHATS_WITH_UNVIEWED_MESSAGES_COUNT_CHANGED',
      userChatsWithUnviewedMessagesCount: 2,
    },
  }

  // verify niether of us can call the trigger method, even with valid input
  await expect(ourClient.mutate({mutation, variables})).rejects.toThrow(/ClientError: Access denied/)
  await expect(theirClient.mutate({mutation, variables})).rejects.toThrow(/ClientError: Access denied/)
})

test('Cannot subscribe to other users notifications', async () => {
  // Note: there doesn't seem to be any error thrown at the time of subscription, it's just that
  // the subscription next() method is never triggered

  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // We both listen to notifictions on **our** user object
  const ourHandlers = []
  const ourSub = await ourClient
    .subscribe({query: subscriptions.onNotification, variables: {userId: ourUserId}})
    .subscribe({
      next: ({data: {onNotification: notification}}) => {
        if (notification.type === 'USER_CHATS_WITH_UNVIEWED_MESSAGES_COUNT_CHANGED') {
          const handler = ourHandlers.shift()
          expect(handler).toBeDefined()
          handler(notification)
        }
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  await theirClient.subscribe({query: subscriptions.onNotification, variables: {userId: ourUserId}}).subscribe({
    next: (resp) => expect(`Should not be called: ${resp}`).toBeNull(),
    error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
  })
  const subInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscription initialize

  // they open up a chat with us
  const chatId = uuidv4()
  await theirClient
    .mutate({
      mutation: mutations.createDirectChat,
      variables: {userId: ourUserId, chatId, messageId: uuidv4(), messageText: 'lore'},
    })
    .then(({data: {createDirectChat: chat}}) => expect(chat.chatId).toBe(chatId))

  // they send a messsage to the chat, which will increment our User.chatsWithUnviewedMessagesCount
  let nextNotification = new Promise((resolve) => ourHandlers.push(resolve))
  await theirClient
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: uuidv4(), text: 'ipsum'}})
    .then(({data: {addChatMessage: message}}) => expect(message.chat.chatId).toBe(chatId))
  await nextNotification.then((notification) => expect(notification.userId).toBe(ourUserId))

  // we don't unsubscribe from the subscription because
  //  - it's not actually active, although I have yet to find a way to expect() that
  //  - unsubcribing results in the AWS SDK throwing errors
  ourSub.unsubscribe()
  await subInitTimeout
})
