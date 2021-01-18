const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Flag chat failures', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId} = await loginCache.getCleanLogin()
  const {client: other2Client} = await loginCache.getCleanLogin()

  // we create a group chat with us and other1 in it
  const chatId = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.createGroupChat,
      variables: {chatId, userIds: [other1UserId], messageId: uuidv4(), messageText: 'm1'},
    })
    .then(({data: {createGroupChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.flagStatus).toBe('NOT_FLAGGED')
    })

  // can't flag a chat that DNE
  await expect(
    other1Client.mutate({mutation: mutations.flagChat, variables: {chatId: uuidv4()}}),
  ).rejects.toThrow(/Chat .* does not exist/)

  // can't flag a chat user is not in
  await expect(other2Client.mutate({mutation: mutations.flagChat, variables: {chatId}})).rejects.toThrow(
    /User is not part of chat/,
  )

  // can't flag a message if we're disabled
  await other1Client.mutate({mutation: mutations.disableUser}).then(({data: {disableUser: user}}) => {
    expect(user.userId).toBe(other1UserId)
    expect(user.userStatus).toBe('DISABLED')
  })
  await expect(other1Client.mutate({mutation: mutations.flagChat, variables: {chatId}})).rejects.toThrow(
    /User .* is not ACTIVE/,
  )

  // would be nice to test that we can't double-flag a chat but without at least
  // 10 users in a chat it'll be deleted right away upon someone flagging it
})

test('Flag chat success', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we create a direct chat with them
  const chatId = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.createDirectChat,
      variables: {userId: theirUserId, chatId, messageId: uuidv4(), messageText: 'lore ipsum'},
    })
    .then(({data: {createDirectChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.flagStatus).toBe('NOT_FLAGGED')
    })

  // check they see the chat as unflagged
  await theirClient.query({query: queries.chat, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
    expect(chat.flagStatus).toBe('NOT_FLAGGED')
  })

  // they flag the chat
  await theirClient
    .mutate({mutation: mutations.flagChat, variables: {chatId}})
    .then(({data: {flagChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.flagStatus).toBe('FLAGGED')
    })

  // over 10% of participants in chat have flagged it, so check it was auto-deleted
  await misc.sleep(2000)
  await theirClient
    .query({query: queries.chat, variables: {chatId}})
    .then(({data: {chat}}) => expect(chat).toBeNull())
  await ourClient
    .query({query: queries.chat, variables: {chatId}})
    .then(({data: {chat}}) => expect(chat).toBeNull())

  // It would be nice to check the case when the chat is not auto-deleted,
  // (less than 10% of participants in chat have flagged it)
  // but that would require a chat with at least 10 users, and running that integration
  // test would be too slow to be worth it. Already covered by unit tests anyway.
})
