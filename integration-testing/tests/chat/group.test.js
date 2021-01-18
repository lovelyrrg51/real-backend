const moment = require('moment')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const {mutations, queries} = require('../../schema')
const misc = require('../../utils/misc')

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

test('Create and edit a group chat', async () => {
  const {client: ourClient, userId: ourUserId, username: ourUsername} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId, username: other1Username} = await loginCache.getCleanLogin()
  const {client: other2Client, userId: other2UserId, username: other2Username} = await loginCache.getCleanLogin()

  // we create a group chat with all of us in it, check details are correct
  const [chatId, messageId1] = [uuidv4(), uuidv4()]
  let variables = {
    chatId,
    name: 'x',
    userIds: [other1UserId, other2UserId],
    messageId: messageId1,
    messageText: 'm',
  }
  let before = moment().toISOString()
  let resp = await ourClient.mutate({mutation: mutations.createGroupChat, variables})
  let after = moment().toISOString()
  let chat = resp.data.createGroupChat
  expect(chat.chatId).toBe(chatId)
  expect(chat.chatType).toBe('GROUP')
  expect(chat.name).toBe('x')
  expect(before <= chat.createdAt).toBe(true)
  expect(after >= chat.createdAt).toBe(true)
  expect(chat.userCount).toBe(3)
  expect(chat.usersCount).toBe(3)
  expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, other1UserId, other2UserId].sort())
  expect(chat.messages.items).toHaveLength(3)
  expect(chat.messages.items[0].text).toContain(ourUsername)
  expect(chat.messages.items[0].text).toContain('created the group')
  expect(chat.messages.items[0].text).toContain('x')
  expect(chat.messages.items[0].textTaggedUsers).toHaveLength(1)
  expect(chat.messages.items[0].textTaggedUsers[0].tag).toBe(`@${ourUsername}`)
  expect(chat.messages.items[0].textTaggedUsers[0].user.userId).toBe(ourUserId)
  expect(chat.messages.items[1].text).toContain(ourUsername)
  expect(chat.messages.items[1].text).toContain('added')
  expect(chat.messages.items[1].text).toContain('to the group')
  expect(chat.messages.items[1].text).toContain(other1Username)
  expect(chat.messages.items[1].text).toContain(other2Username)
  expect(chat.messages.items[1].textTaggedUsers).toHaveLength(3)
  expect(chat.messages.items[1].textTaggedUsers.map((t) => t.tag).sort()).toEqual(
    [`@${ourUsername}`, `@${other1Username}`, `@${other2Username}`].sort(),
  )
  expect(chat.messages.items[1].textTaggedUsers.map((t) => t.user.userId).sort()).toEqual(
    [ourUserId, other1UserId, other2UserId].sort(),
  )
  expect(chat.messages.items[2].messageId).toBe(messageId1)
  expect(chat.messages.items[2].text).toBe('m')
  const messageIdSystem0 = chat.messages.items[0].messageId
  const messageIdSystem1 = chat.messages.items[1].messageId

  // check we have the chat
  await misc.sleep(2000)
  resp = await ourClient.query({query: queries.self})
  expect(resp.data.self.userId).toBe(ourUserId)
  expect(resp.data.self.chatCount).toBe(1)
  expect(resp.data.self.chats.items).toHaveLength(1)
  expect(resp.data.self.chats.items[0].chatId).toBe(chatId)
  expect(resp.data.self.chats.items[0].messageCount).toBe(3)
  expect(resp.data.self.chats.items[0].messagesCount).toBe(3)
  expect(resp.data.self.chats.items[0].createdAt < resp.data.self.chats.items[0].lastMessageActivityAt).toBe(true)

  // check other1 has the chat
  resp = await other1Client.query({query: queries.self})
  expect(resp.data.self.userId).toBe(other1UserId)
  expect(resp.data.self.chatCount).toBe(1)
  expect(resp.data.self.chats.items).toHaveLength(1)
  expect(resp.data.self.chats.items[0].chatId).toBe(chatId)

  // we add a message
  const messageId2 = uuidv4()
  variables = {chatId, messageId: messageId2, text: 'm2'}
  resp = await ourClient.mutate({mutation: mutations.addChatMessage, variables})
  expect(resp.data.addChatMessage.messageId).toBe(messageId2)

  // other1 adds a message
  const messageId3 = uuidv4()
  variables = {chatId, messageId: messageId3, text: 'm3'}
  resp = await other1Client.mutate({mutation: mutations.addChatMessage, variables})
  expect(resp.data.addChatMessage.messageId).toBe(messageId3)

  // check other2 sees both those messages
  await misc.sleep(2000)
  resp = await other2Client.query({query: queries.chat, variables: {chatId}})
  expect(resp.data.chat.chatId).toBe(chatId)
  expect(resp.data.chat.messageCount).toBe(5)
  expect(resp.data.chat.messagesCount).toBe(5)
  expect(resp.data.chat.messages.items).toHaveLength(5)
  expect(resp.data.chat.messages.items[0].messageId).toBe(messageIdSystem0)
  expect(resp.data.chat.messages.items[1].messageId).toBe(messageIdSystem1)
  expect(resp.data.chat.messages.items[2].messageId).toBe(messageId1)
  expect(resp.data.chat.messages.items[3].messageId).toBe(messageId2)
  expect(resp.data.chat.messages.items[4].messageId).toBe(messageId3)

  // other2 edits the name of the group chat
  variables = {chatId, name: 'new name'}
  resp = await other2Client.mutate({mutation: mutations.editGroupChat, variables})
  expect(resp.data.editGroupChat.chatId).toBe(chatId)
  expect(resp.data.editGroupChat.name).toBe('new name')

  // check we see the updated name and the messages
  await misc.sleep(2000)
  resp = await ourClient.query({query: queries.chat, variables: {chatId}})
  expect(resp.data.chat.chatId).toBe(chatId)
  expect(resp.data.chat.name).toBe('new name')
  expect(resp.data.chat.messageCount).toBe(6)
  expect(resp.data.chat.messagesCount).toBe(6)
  expect(resp.data.chat.messages.items).toHaveLength(6)
  expect(resp.data.chat.messages.items[0].messageId).toBe(messageIdSystem0)
  expect(resp.data.chat.messages.items[1].messageId).toBe(messageIdSystem1)
  expect(resp.data.chat.messages.items[2].messageId).toBe(messageId1)
  expect(resp.data.chat.messages.items[3].messageId).toBe(messageId2)
  expect(resp.data.chat.messages.items[4].messageId).toBe(messageId3)
  expect(resp.data.chat.messages.items[5].text).toContain(other2Username)
  expect(resp.data.chat.messages.items[5].text).toContain('changed the name of the group')
  expect(resp.data.chat.messages.items[5].text).toContain('new name')
  expect(resp.data.chat.messages.items[5].textTaggedUsers).toHaveLength(1)
  expect(resp.data.chat.messages.items[5].textTaggedUsers[0].tag).toBe(`@${other2Username}`)
  expect(resp.data.chat.messages.items[5].textTaggedUsers[0].user.userId).toBe(other2UserId)
  const messageIdSystem3 = resp.data.chat.messages.items[5].messageId

  // we delete the name of the group chat
  variables = {chatId, name: ''}
  resp = await ourClient.mutate({mutation: mutations.editGroupChat, variables})
  expect(resp.data.editGroupChat.chatId).toBe(chatId)
  expect(resp.data.editGroupChat.name).toBeNull()

  // check other1 sees the updated name
  await misc.sleep(2000)
  resp = await other1Client.query({query: queries.chat, variables: {chatId}})
  expect(resp.data.chat.chatId).toBe(chatId)
  expect(resp.data.chat.name).toBeNull()
  expect(resp.data.chat.messageCount).toBe(7)
  expect(resp.data.chat.messagesCount).toBe(7)
  expect(resp.data.chat.messages.items).toHaveLength(7)
  expect(resp.data.chat.messages.items[0].messageId).toBe(messageIdSystem0)
  expect(resp.data.chat.messages.items[1].messageId).toBe(messageIdSystem1)
  expect(resp.data.chat.messages.items[2].messageId).toBe(messageId1)
  expect(resp.data.chat.messages.items[3].messageId).toBe(messageId2)
  expect(resp.data.chat.messages.items[4].messageId).toBe(messageId3)
  expect(resp.data.chat.messages.items[5].messageId).toBe(messageIdSystem3)
  expect(resp.data.chat.messages.items[6].text).toContain(ourUsername)
  expect(resp.data.chat.messages.items[6].text).toContain('deleted the name of the group')
  expect(resp.data.chat.messages.items[6].textTaggedUsers).toHaveLength(1)
  expect(resp.data.chat.messages.items[6].textTaggedUsers[0].tag).toBe(`@${ourUsername}`)
  expect(resp.data.chat.messages.items[6].textTaggedUsers[0].user.userId).toBe(ourUserId)
})

test('Creating a group chat with our userId in the listed userIds has no affect', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()

  // we create a group chat with the two of us in it, and we uncessarily add our user Id to the userIds
  const [chatId, messageId] = [uuidv4(), uuidv4()]
  let variables = {chatId, userIds: [ourUserId, theirUserId], messageId, messageText: 'm1'}
  let resp = await ourClient.mutate({mutation: mutations.createGroupChat, variables})
  expect(resp.data.createGroupChat.chatId).toBe(chatId)
  expect(resp.data.createGroupChat.name).toBeNull()
  expect(resp.data.createGroupChat.userCount).toBe(2)
  expect(resp.data.createGroupChat.usersCount).toBe(2)
  expect(resp.data.createGroupChat.users.items.map((u) => u.userId).sort()).toEqual(
    [ourUserId, theirUserId].sort(),
  )
})

test('Cannot create, edit, add others to or leave a group chat if we are disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()

  // we create a group chat with just us in it
  const chatId = uuidv4()
  let variables = {chatId, userIds: [], messageId: uuidv4(), messageText: 'm1'}
  let resp = await ourClient.mutate({mutation: mutations.createGroupChat, variables})
  expect(resp.data.createGroupChat.chatId).toBe(chatId)

  // we disable ourselves
  resp = await ourClient.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(ourUserId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify we cannot create another group chat
  variables = {chatId: uuidv4(), userIds: [], messageId: uuidv4(), messageText: 'm1'}
  await expect(ourClient.mutate({mutation: mutations.createGroupChat, variables})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )

  // verify we cannot add someone else to our existing group chat
  await expect(
    ourClient.mutate({mutation: mutations.addToGroupChat, variables: {chatId, userIds: [theirUserId]}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)

  // verify we cannot edit our existing group chat
  await expect(
    ourClient.mutate({mutation: mutations.editGroupChat, variables: {chatId, name: 'new'}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)

  // verify we cannot leave our existing group chat
  await expect(ourClient.mutate({mutation: mutations.leaveGroupChat, variables: {chatId}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )
})

test('Anonymous users cannot create nor get added to a group chat', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()
  const {userId: otherUserId} = await loginCache.getCleanLogin()
  ;({client: anonClient, userId: anonUserId} = await cognito.getAnonymousAppSyncLogin())

  // verify anonymous user can't create group chat
  const chatId = uuidv4()
  await expect(
    anonClient.mutate({
      mutation: mutations.createGroupChat,
      variables: {chatId, userIds: [], messageId: uuidv4(), messageText: 'm1'},
    }),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)

  // verify if we create a group chat with an anonymous user, they actually don't get added
  await ourClient
    .mutate({
      mutation: mutations.createGroupChat,
      variables: {chatId, userIds: [anonUserId, theirUserId], messageId: uuidv4(), messageText: 'm2'},
    })
    .then(({data: {createGroupChat: chat}}) => expect(chat.chatId).toBe(chatId))
  await misc.sleep(2000)
  await ourClient.query({query: queries.chatUsers, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
    expect(chat.usersCount).toBe(2)
    expect(chat.users.items).toHaveLength(2)
    expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, theirUserId].sort())
  })

  // verify if we try to add the anonymous user to a group chat, they don't get added
  await ourClient.mutate({
    mutation: mutations.addToGroupChat,
    variables: {chatId, userIds: [anonUserId, otherUserId]},
  })
  await misc.sleep(2000)
  await ourClient.query({query: queries.chatUsers, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
    expect(chat.usersCount).toBe(3)
    expect(chat.users.items).toHaveLength(3)
    expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, theirUserId, otherUserId].sort())
  })
})

test('Exclude users from list of users in a chat', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()

  // we create a group chat with the two of us in it
  const [chatId, messageId] = [uuidv4(), uuidv4()]
  let variables = {chatId, userIds: [theirUserId], messageId, messageText: 'm1'}
  let resp = await ourClient.mutate({mutation: mutations.createGroupChat, variables})
  expect(resp.data.createGroupChat.chatId).toBe(chatId)
  expect(resp.data.createGroupChat.name).toBeNull()
  expect(resp.data.createGroupChat.userCount).toBe(2)
  expect(resp.data.createGroupChat.usersCount).toBe(2)
  expect(resp.data.createGroupChat.users.items.map((u) => u.userId).sort()).toEqual(
    [ourUserId, theirUserId].sort(),
  )

  // check chat users, all included
  resp = await ourClient.query({query: queries.chatUsers, variables: {chatId}})
  expect(resp.data.chat.chatId).toBe(chatId)
  expect(resp.data.chat.users.items).toHaveLength(2)
  expect(resp.data.chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, theirUserId].sort())

  // exclude ourselves
  resp = await ourClient.query({query: queries.chatUsers, variables: {chatId, excludeUserId: ourUserId}})
  expect(resp.data.chat.chatId).toBe(chatId)
  expect(resp.data.chat.users.items).toHaveLength(1)
  expect(resp.data.chat.users.items[0].userId).toBe(theirUserId)

  // exclude them
  resp = await ourClient.query({query: queries.chatUsers, variables: {chatId, excludeUserId: theirUserId}})
  expect(resp.data.chat.chatId).toBe(chatId)
  expect(resp.data.chat.users.items).toHaveLength(1)
  expect(resp.data.chat.users.items[0].userId).toBe(ourUserId)
})

test('Create a group chat with just us and without a name, add people to it and leave from it', async () => {
  const {client: ourClient, userId: ourUserId, username: ourUsername} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId, username: theirUsername} = await loginCache.getCleanLogin()
  const {client: otherClient, userId: otherUserId, username: otherUsername} = await loginCache.getCleanLogin()

  // we create a group chat with no name and just us in it
  const [chatId, messageId1] = [uuidv4(), uuidv4()]
  let variables = {chatId, userIds: [], messageId: messageId1, messageText: 'm1'}
  let resp = await ourClient.mutate({mutation: mutations.createGroupChat, variables})
  expect(resp.data.createGroupChat.chatId).toBe(chatId)
  expect(resp.data.createGroupChat.name).toBeNull()
  expect(resp.data.createGroupChat.userCount).toBe(1)
  expect(resp.data.createGroupChat.usersCount).toBe(1)
  expect(resp.data.createGroupChat.users.items).toHaveLength(1)
  expect(resp.data.createGroupChat.users.items[0].userId).toBe(ourUserId)

  // check they can't access the chat
  resp = await theirClient.query({query: queries.chat, variables: {chatId}})
  expect(resp.data.chat).toBeNull()

  // we add them and other to the chat
  variables = {chatId, userIds: [theirUserId, otherUserId]}
  resp = await ourClient.mutate({mutation: mutations.addToGroupChat, variables})
  let chat = resp.data.addToGroupChat
  expect(chat.chatId).toBe(chatId)
  expect(chat.userCount).toBe(3)
  expect(chat.usersCount).toBe(3)
  expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, theirUserId, otherUserId].sort())
  expect(chat.messages.items).toHaveLength(3)
  expect(chat.messages.items[1].messageId).toBe(messageId1)
  expect(chat.messages.items[2].text).toContain(ourUsername)
  expect(chat.messages.items[2].text).toContain(theirUsername)
  expect(chat.messages.items[2].text).toContain(otherUsername)
  expect(chat.messages.items[2].text).toContain('added')
  expect(chat.messages.items[2].text).toContain('to the group')
  expect(chat.messages.items[2].textTaggedUsers).toHaveLength(3)
  expect(chat.messages.items[2].textTaggedUsers.map((t) => t.tag).sort()).toEqual(
    [`@${ourUsername}`, `@${theirUsername}`, `@${otherUsername}`].sort(),
  )
  expect(chat.messages.items[2].textTaggedUsers.map((t) => t.user.userId).sort()).toEqual(
    [ourUserId, theirUserId, otherUserId].sort(),
  )

  // check they have the chat now
  resp = await theirClient.query({query: queries.self})
  expect(resp.data.self.userId).toBe(theirUserId)
  expect(resp.data.self.chatCount).toBe(1)
  expect(resp.data.self.chats.items).toHaveLength(1)
  expect(resp.data.self.chats.items[0].chatId).toBe(chatId)
  expect(resp.data.self.chats.items[0].messageCount).toBe(3)
  expect(resp.data.self.chats.items[0].messagesCount).toBe(3)

  // check other can directly access the chat, and they see the system message from adding a user
  resp = await otherClient.query({query: queries.chat, variables: {chatId}})
  expect(resp.data.chat.chatId).toBe(chatId)

  // they add a message to the chat
  const messageId2 = uuidv4()
  variables = {chatId, messageId: messageId2, text: 'lore'}
  resp = await theirClient.mutate({mutation: mutations.addChatMessage, variables})
  expect(resp.data.addChatMessage.messageId).toBe(messageId2)

  // they leave the chat
  resp = await theirClient.mutate({mutation: mutations.leaveGroupChat, variables: {chatId}})
  expect(resp.data.leaveGroupChat.chatId).toBe(chatId)
  expect(resp.data.leaveGroupChat.userCount).toBe(2)
  expect(resp.data.leaveGroupChat.usersCount).toBe(2)

  // check we see their message, we don't see them in the chat
  resp = await ourClient.query({query: queries.chat, variables: {chatId}})
  chat = resp.data.chat
  expect(chat.chatId).toBe(chatId)
  expect(chat.userCount).toBe(2)
  expect(chat.usersCount).toBe(2)
  expect(chat.users.items).toHaveLength(2)
  expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, otherUserId].sort())
  expect(chat.messageCount).toBe(5)
  expect(chat.messagesCount).toBe(5)
  expect(chat.messages.items).toHaveLength(5)
  expect(chat.messages.items[1].messageId).toBe(messageId1)
  expect(chat.messages.items[3].messageId).toBe(messageId2)
  expect(chat.messages.items[3].messageId).toBe(messageId2)
  expect(chat.messages.items[4].text).toContain(theirUsername)
  expect(chat.messages.items[4].text).toContain('left the group')
  expect(chat.messages.items[4].textTaggedUsers).toHaveLength(1)
  expect(chat.messages.items[4].textTaggedUsers[0].tag).toBe(`@${theirUsername}`)
  expect(chat.messages.items[4].textTaggedUsers[0].user.userId).toBe(theirUserId)

  // we leave the chat
  resp = await ourClient.mutate({mutation: mutations.leaveGroupChat, variables: {chatId}})
  expect(resp.data.leaveGroupChat.chatId).toBe(chatId)
  expect(resp.data.leaveGroupChat.userCount).toBe(1)
  expect(resp.data.leaveGroupChat.usersCount).toBe(1)

  // check we can no longer access the chat
  resp = await ourClient.query({query: queries.chat, variables: {chatId}})
  expect(resp.data.chat).toBeNull()
})

test('Cant add a users that does not exist to a group', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {userId: theirUserId} = await loginCache.getCleanLogin()

  // we create a group chat with us and another non-existent user in it,
  // should skip over the non-existent user
  const [chatId, messageId] = [uuidv4(), uuidv4()]
  let variables = {chatId, userIds: ['uid-dne'], messageId, messageText: 'm1'}
  let resp = await ourClient.mutate({mutation: mutations.createGroupChat, variables})
  expect(resp.data.createGroupChat.chatId).toBe(chatId)
  expect(resp.data.createGroupChat.userCount).toBe(1)
  expect(resp.data.createGroupChat.usersCount).toBe(1)
  expect(resp.data.createGroupChat.users.items[0].userId).toBe(ourUserId)
  expect(resp.data.createGroupChat.messages.items).toHaveLength(2)
  expect(resp.data.createGroupChat.messages.items[1].messageId).toBe(messageId)

  // add another non-existent user to the group, as well as a good one
  // should skip over the non-existent user
  variables = {chatId, userIds: [theirUserId, 'uid-dne1', 'uid-dne2']}
  resp = await ourClient.mutate({mutation: mutations.addToGroupChat, variables})
  expect(resp.data.addToGroupChat.chatId).toBe(chatId)
  expect(resp.data.addToGroupChat.userCount).toBe(2)
  expect(resp.data.addToGroupChat.usersCount).toBe(2)
  expect(resp.data.addToGroupChat.users.items.map((u) => u.userId).sort()).toEqual(
    [ourUserId, theirUserId].sort(),
  )
  expect(resp.data.addToGroupChat.messages.items).toHaveLength(3)
  expect(resp.data.addToGroupChat.messages.items[1].messageId).toBe(messageId)
  expect(resp.data.addToGroupChat.messages.items[2].text).toContain('added')
  expect(resp.data.addToGroupChat.messages.items[2].textTaggedUsers).toHaveLength(2)
})

test('Add someone to a group chat that is already there is a no-op', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {userId: other1UserId} = await loginCache.getCleanLogin()
  const {userId: other2UserId} = await loginCache.getCleanLogin()

  // we create a group chat with both of us in it
  const chatId = uuidv4()
  let variables = {chatId, userIds: [other1UserId], messageId: uuidv4(), messageText: 'm1'}
  let resp = await ourClient.mutate({mutation: mutations.createGroupChat, variables})
  expect(resp.data.createGroupChat.chatId).toBe(chatId)
  expect(resp.data.createGroupChat.userCount).toBe(2)
  expect(resp.data.createGroupChat.usersCount).toBe(2)
  expect(resp.data.createGroupChat.users.items.map((u) => u.userId).sort()).toEqual(
    [ourUserId, other1UserId].sort(),
  )

  // check adding to them to the chat again does nothing
  variables = {chatId, userIds: [other1UserId]}
  resp = await ourClient.mutate({mutation: mutations.addToGroupChat, variables})
  expect(resp.data.addToGroupChat.chatId).toBe(chatId)
  expect(resp.data.addToGroupChat.userCount).toBe(2)
  expect(resp.data.addToGroupChat.usersCount).toBe(2)
  expect(resp.data.addToGroupChat.users.items.map((u) => u.userId).sort()).toEqual(
    [ourUserId, other1UserId].sort(),
  )

  // check adding to them and another user to the chat at the same time adds the other user
  variables = {chatId, userIds: [other1UserId, other2UserId]}
  resp = await ourClient.mutate({mutation: mutations.addToGroupChat, variables})
  expect(resp.data.addToGroupChat.chatId).toBe(chatId)
  expect(resp.data.addToGroupChat.userCount).toBe(3)
  expect(resp.data.addToGroupChat.usersCount).toBe(3)
  expect(resp.data.addToGroupChat.users.items.map((u) => u.userId).sort()).toEqual(
    [ourUserId, other1UserId, other2UserId].sort(),
  )
})

test('Cannot add someone to a chat that DNE, that we are not in or that is a a direct chat', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId} = await loginCache.getCleanLogin()
  const {userId: other2UserId} = await loginCache.getCleanLogin()

  // check we can't add other1 to a chat that DNE
  let variables = {chatId: uuidv4(), userIds: [other1UserId]}
  await expect(ourClient.mutate({mutation: mutations.addToGroupChat, variables})).rejects.toThrow(
    /ClientError: .* is not a member/,
  )

  // other1 creates a group chat with only themselves in it
  const chatId1 = uuidv4()
  variables = {chatId: chatId1, userIds: [], messageId: uuidv4(), messageText: 'm'}
  let resp = await other1Client.mutate({mutation: mutations.createGroupChat, variables})
  expect(resp.data.createGroupChat.chatId).toBe(chatId1)
  expect(resp.data.createGroupChat.chatType).toBe('GROUP')

  // check we cannot add other2 to that group chat
  variables = {chatId: chatId1, userIds: [other2UserId]}
  await expect(ourClient.mutate({mutation: mutations.addToGroupChat, variables})).rejects.toThrow(
    /ClientError: .* is not a member/,
  )

  // we create a direct chat with other2
  const chatId2 = uuidv4()
  variables = {userId: other2UserId, chatId: chatId2, messageId: uuidv4(), messageText: 'lore ipsum'}
  resp = await ourClient.mutate({mutation: mutations.createDirectChat, variables})
  expect(resp.data.createDirectChat.chatId).toBe(chatId2)
  expect(resp.data.createDirectChat.chatType).toBe('DIRECT')

  // check we cannot add other1 to that direct chat
  variables = {chatId: chatId2, userIds: [other1UserId]}
  await expect(ourClient.mutate({mutation: mutations.addToGroupChat, variables})).rejects.toThrow(
    /ClientError: Cannot add users to non-GROUP chat /,
  )
})

test('Cannot leave a chat that DNE, that we are not in, or that is a direct chat', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // check we cannot leave a chat that DNE
  let variables = {chatId: uuidv4()}
  await expect(ourClient.mutate({mutation: mutations.leaveGroupChat, variables})).rejects.toThrow(
    /ClientError: .* is not a member/,
  )

  // they create a group chat with only themselves in it
  const chatId1 = uuidv4()
  variables = {chatId: chatId1, userIds: [], messageId: uuidv4(), messageText: 'm'}
  let resp = await theirClient.mutate({mutation: mutations.createGroupChat, variables})
  expect(resp.data.createGroupChat.chatId).toBe(chatId1)
  expect(resp.data.createGroupChat.chatType).toBe('GROUP')

  // check we cannot leave from that group chat we are not in
  variables = {chatId: chatId1}
  await expect(ourClient.mutate({mutation: mutations.leaveGroupChat, variables})).rejects.toThrow(
    /ClientError: .* is not a member/,
  )

  // we create a direct chat with them
  const chatId2 = uuidv4()
  variables = {userId: theirUserId, chatId: chatId2, messageId: uuidv4(), messageText: 'lore ipsum'}
  resp = await ourClient.mutate({mutation: mutations.createDirectChat, variables})
  expect(resp.data.createDirectChat.chatId).toBe(chatId2)
  expect(resp.data.createDirectChat.chatType).toBe('DIRECT')

  // check we cannot leave that direct chat
  variables = {chatId: chatId2}
  await expect(ourClient.mutate({mutation: mutations.leaveGroupChat, variables})).rejects.toThrow(
    /ClientError: Cannot leave non-GROUP chat /,
  )
})

test('Cannnot edit name of chat that DNE, that we are not in, or that is a direct chat', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // check we cannot edit a chat that DNE
  let variables = {chatId: uuidv4(), name: 'new name'}
  await expect(ourClient.mutate({mutation: mutations.leaveGroupChat, variables})).rejects.toThrow(
    /ClientError: .* is not a member/,
  )

  // they create a group chat with only themselves in it
  const chatId1 = uuidv4()
  variables = {chatId: chatId1, userIds: [], messageId: uuidv4(), messageText: 'm'}
  let resp = await theirClient.mutate({mutation: mutations.createGroupChat, variables})
  expect(resp.data.createGroupChat.chatId).toBe(chatId1)
  expect(resp.data.createGroupChat.chatType).toBe('GROUP')

  // check we cannot edit the name of their group chat
  variables = {chatId: chatId1, name: 'c name'}
  await expect(ourClient.mutate({mutation: mutations.editGroupChat, variables})).rejects.toThrow(
    /ClientError: .* is not a member/,
  )

  // we create a direct chat with them
  const chatId2 = uuidv4()
  variables = {userId: theirUserId, chatId: chatId2, messageId: uuidv4(), messageText: 'lore ipsum'}
  resp = await ourClient.mutate({mutation: mutations.createDirectChat, variables})
  expect(resp.data.createDirectChat.chatId).toBe(chatId2)
  expect(resp.data.createDirectChat.chatType).toBe('DIRECT')

  // check we cannot edit the name of that direct chat
  variables = {chatId: chatId2, name: 'c name'}
  await expect(ourClient.mutate({mutation: mutations.editGroupChat, variables})).rejects.toThrow(
    /ClientError: Cannot edit non-GROUP chat /,
  )
})
