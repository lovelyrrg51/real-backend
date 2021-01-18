const moment = require('moment')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const {mutations, queries} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

let anonClient, anonUserId
beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())
afterEach(async () => {
  if (anonClient) await anonClient.mutate({mutation: mutations.deleteUser})
  anonClient = null
})

test('Create a direct chat', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  const {client: randoClient} = await loginCache.getCleanLogin()

  // check we have no direct chat between us
  let resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.directChat).toBeNull()
  expect(resp.data.self.chatCount).toBe(0)
  expect(resp.data.self.chats.items).toHaveLength(0)

  resp = await theirClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.directChat).toBeNull()
  expect(resp.data.user.chatCount).toBeNull()
  expect(resp.data.user.chats).toBeNull()

  resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.directChat).toBeNull()
  expect(resp.data.user.chatCount).toBeNull()
  expect(resp.data.user.chats).toBeNull()

  // we open up a direct chat with them
  const [chatId, messageId] = [uuidv4(), uuidv4()]
  const messageText = 'lore ipsum'
  let variables = {userId: theirUserId, chatId, messageId, messageText}
  let before = moment().toISOString()
  resp = await ourClient.mutate({mutation: mutations.createDirectChat, variables})
  let after = moment().toISOString()
  let chat = resp.data.createDirectChat
  expect(chat.chatId).toBe(chatId)
  expect(chat.chatType).toBe('DIRECT')
  expect(chat.name).toBeNull()
  expect(before <= chat.createdAt).toBe(true)
  expect(after >= chat.createdAt).toBe(true)
  const chatCreatedAt = chat.createdAt
  expect(chat.userCount).toBe(2)
  expect(chat.usersCount).toBe(2)
  expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, theirUserId].sort())
  expect(chat.messages.items).toHaveLength(1)
  expect(chat.messages.items[0].messageId).toBe(messageId)
  expect(chat.messages.items[0].text).toBe(messageText)
  expect(chat.messages.items[0].textTaggedUsers).toEqual([])
  expect(chat.messages.items[0].createdAt).toBe(chatCreatedAt)
  expect(chat.messages.items[0].lastEditedAt).toBeNull()
  expect(chat.messages.items[0].chat.chatId).toBe(chatId)
  expect(chat.messages.items[0].author.userId).toBe(ourUserId)
  expect(chat.messages.items[0].viewedStatus).toBe('VIEWED')

  // check we can see that direct chat when looking at their profile
  resp = await ourClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.directChat.chatId).toBe(chatId)
  expect(resp.data.user.directChat.lastMessageActivityAt).toBe(chatCreatedAt)
  expect(resp.data.user.directChat.messageCount).toBe(1)
  expect(resp.data.user.directChat.messagesCount).toBe(1)
  expect(resp.data.user.chatCount).toBeNull()
  expect(resp.data.user.chats).toBeNull()

  // check they can see that direct chat when looking at our profile
  resp = await theirClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.directChat.chatId).toBe(chatId)
  expect(resp.data.user.chatCount).toBeNull()
  expect(resp.data.user.chats).toBeNull()

  // check we see the chat in our list of chats
  resp = await ourClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.directChat).toBeNull()
  expect(resp.data.user.chatCount).toBe(1)
  expect(resp.data.user.chats.items).toHaveLength(1)
  expect(resp.data.user.chats.items[0].chatId).toBe(chatId)

  // check they see the chat in their list of chats
  resp = await theirClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.directChat).toBeNull()
  expect(resp.data.user.chatCount).toBe(1)
  expect(resp.data.user.chats.items).toHaveLength(1)
  expect(resp.data.user.chats.items[0].chatId).toBe(chatId)

  // check we can both see the chat directly
  resp = await ourClient.query({query: queries.chat, variables: {chatId}})
  expect(resp.data.chat.chatId).toBe(chatId)

  resp = await theirClient.query({query: queries.chat, variables: {chatId}})
  expect(resp.data.chat.chatId).toBe(chatId)

  // check that another rando can't see either the chat either by looking at either of us or direct access
  resp = await randoClient.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.directChat).toBeNull()
  expect(resp.data.user.chatCount).toBeNull()
  expect(resp.data.user.chats).toBeNull()

  resp = await randoClient.query({query: queries.user, variables: {userId: theirUserId}})
  expect(resp.data.user.directChat).toBeNull()
  expect(resp.data.user.chatCount).toBeNull()
  expect(resp.data.user.chats).toBeNull()

  resp = await randoClient.query({query: queries.chat, variables: {chatId}})
  expect(resp.data.chat).toBeNull()
})

test('Cannot create a direct chat if one already exists', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // they open up a direct chat with us
  const chatId = uuidv4()
  let variables = {userId: ourUserId, chatId, messageId: uuidv4(), messageText: 'lore ipsum'}
  let resp = await theirClient.mutate({mutation: mutations.createDirectChat, variables})
  expect(resp.data.createDirectChat.chatId).toBe(chatId)

  // verify we cannot open up another direct chat with them
  variables = {userId: theirUserId, chatId: uuidv4(), messageId: uuidv4(), messageText: 'lore ipsum'}
  await expect(ourClient.mutate({mutation: mutations.createDirectChat, variables})).rejects.toThrow(
    /ClientError: Chat already exists /,
  )

  // verify they cannot open up another direct chat with us
  variables = {userId: ourUserId, chatId: uuidv4(), messageId: uuidv4(), messageText: 'lore ipsum'}
  await expect(theirClient.mutate({mutation: mutations.createDirectChat, variables})).rejects.toThrow(
    /ClientError: Chat already exists /,
  )
})

test('Cannot create a direct chat if we are disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()

  // we disable ourselves
  let resp = await ourClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(ourUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify we cannot open up another direct chat with them
  let variables = {userId: theirUserId, chatId: uuidv4(), messageId: uuidv4(), messageText: 'lore ipsum'}
  await expect(ourClient.mutate({mutation: mutations.createDirectChat, variables})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )
})

test('Anonymous users cannot create direct chats nor be added to one', async () => {
  ;({client: anonClient, userId: anonUserId} = await cognito.getAnonymousAppSyncLogin())
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // check anon user can't create direct chat
  await expect(
    anonClient.mutate({
      mutation: mutations.createDirectChat,
      variables: {userId: theirUserId, chatId: uuidv4(), messageId: uuidv4(), messageText: 'lore ipsum'},
    }),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)

  // check normal user can't create direct chat with anon user
  await expect(
    theirClient.mutate({
      mutation: mutations.createDirectChat,
      variables: {userId: anonUserId, chatId: uuidv4(), messageId: uuidv4(), messageText: 'lore ipsum'},
    }),
  ).rejects.toThrow(/ClientError: Cannot open direct chat with user with status `ANONYMOUS`/)
})

test('Cannot open direct chat with self', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  const chatId = uuidv4()
  let variables = {userId: ourUserId, chatId, messageId: uuidv4(), messageText: 'lore ipsum'}
  await expect(ourClient.mutate({mutation: mutations.createDirectChat, variables})).rejects.toThrow(
    /ClientError: .* cannot open direct chat with themselves/,
  )
})

test('Create multiple direct chats', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: other1Client} = await loginCache.getCleanLogin()
  const {client: other2Client} = await loginCache.getCleanLogin()

  // check we have no chats
  let resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.chatCount).toBe(0)
  expect(resp.data.self.chats.items).toHaveLength(0)

  // other1 opens up a direct chat with us
  const [chatId1, messageId1] = [uuidv4(), uuidv4()]
  const messageText1 = 'heyya! from other 1'
  let variables = {userId: ourUserId, chatId: chatId1, messageId: messageId1, messageText: messageText1}
  resp = await other1Client.mutate({mutation: mutations.createDirectChat, variables})
  expect(resp.data.createDirectChat.chatId).toBe(chatId1)
  expect(resp.data.createDirectChat.messages.items).toHaveLength(1)
  expect(resp.data.createDirectChat.messages.items[0].messageId).toBe(messageId1)
  expect(resp.data.createDirectChat.messages.items[0].text).toBe(messageText1)

  // other2 opens up a direct chat with us
  const [chatId2, messageId2] = [uuidv4(), uuidv4()]
  const messageText2 = 'heyya! from other 2'
  variables = {userId: ourUserId, chatId: chatId2, messageId: messageId2, messageText: messageText2}
  resp = await other2Client.mutate({mutation: mutations.createDirectChat, variables})
  expect(resp.data.createDirectChat.chatId).toBe(chatId2)
  expect(resp.data.createDirectChat.messages.items).toHaveLength(1)
  expect(resp.data.createDirectChat.messages.items[0].messageId).toBe(messageId2)
  expect(resp.data.createDirectChat.messages.items[0].text).toBe(messageText2)

  // check we see both chats
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.chatCount).toBe(2)
  expect(resp.data.self.chats.items).toHaveLength(2)
  expect(resp.data.self.chats.items[0].chatId).toBe(chatId2)
  expect(resp.data.self.chats.items[1].chatId).toBe(chatId1)

  // check other1 sees the direct chat with us
  resp = await other1Client.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.directChat.chatId).toBe(chatId1)

  // check other2 sees the direct chat with us
  resp = await other2Client.query({query: queries.user, variables: {userId: ourUserId}})
  expect(resp.data.user.directChat.chatId).toBe(chatId2)

  // check other1 cannot see other2's chat
  resp = await other1Client.query({query: queries.chat, variables: {chatId: chatId2}})
  expect(resp.data.chat).toBeNull()

  // check other2 cannot see other1's chat
  resp = await other2Client.query({query: queries.chat, variables: {chatId: chatId1}})
  expect(resp.data.chat).toBeNull()
})
