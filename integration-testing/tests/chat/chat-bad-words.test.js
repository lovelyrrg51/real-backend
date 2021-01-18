const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

let anonClient
// https://github.com/real-social-media/bad_words/blob/master/bucket/bad_words.json
const badWord = 'uoiFZP8bjS'

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

test('Create a direct chat with bad word', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

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

  await ourClient
    .mutate({mutation: mutations.createDirectChat, variables})
    .then(({data: {createDirectChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.chatType).toBe('DIRECT')
      expect(chat.name).toBeNull()
      expect(chat.userCount).toBe(2)
      expect(chat.usersCount).toBe(2)
      expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, theirUserId].sort())
      expect(chat.messages.items).toHaveLength(1)
      expect(chat.messages.items[0].messageId).toBe(messageId)
      expect(chat.messages.items[0].text).toBe(messageText)
      expect(chat.messages.items[0].chat.chatId).toBe(chatId)
      expect(chat.messages.items[0].author.userId).toBe(ourUserId)
      expect(chat.messages.items[0].viewedStatus).toBe('VIEWED')
    })

  // check we see the chat in our list of chats
  await ourClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.directChat).toBeNull()
    expect(user.chatCount).toBe(1)
    expect(user.chats.items).toHaveLength(1)
    expect(user.chats.items[0].chatId).toBe(chatId)
  })

  // check they see the chat in their list of chats
  await theirClient.query({query: queries.user, variables: {userId: theirUserId}}).then(({data: {user}}) => {
    expect(user.directChat).toBeNull()
    expect(user.chatCount).toBe(1)
    expect(user.chats.items).toHaveLength(1)
    expect(user.chats.items[0].chatId).toBe(chatId)
  })

  // check we can both see the chat directly
  await ourClient.query({query: queries.chat, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
  })

  await theirClient.query({query: queries.chat, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
  })

  // they add chat message with bad word, verify it's removed
  const [messageId2, messageText2] = [uuidv4(), `msg ${badWord}`]
  variables = {chatId, messageId: messageId2, text: messageText2}
  await theirClient
    .mutate({mutation: mutations.addChatMessage, variables})
    .then(({data: {addChatMessage: chatMessage}}) => {
      expect(chatMessage.messageId).toBe(messageId2)
      expect(chatMessage.text).toBe(messageText2)
      expect(chatMessage.chat.chatId).toBe(chatId)
    })

  // verify the bad word chat is removed
  await misc.sleep(1000)
  await ourClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.chatCount).toBe(1)
    expect(user.chats.items).toHaveLength(1)
    expect(user.chats.items[0].chatId).toBe(chatId)
  })
  await ourClient.query({query: queries.chat, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
    expect(chat.messages.items).toHaveLength(1)
  })

  // edit the message, verify it's removed
  const messageText3 = `msg ${badWord.toUpperCase()}`
  await ourClient
    .mutate({mutation: mutations.editChatMessage, variables: {messageId, text: messageText3}})
    .then(({data: {editChatMessage: chatMessage}}) => {
      expect(chatMessage.messageId).toBe(messageId)
      expect(chatMessage.text).toBe(messageText3)
      expect(chatMessage.chat.chatId).toBe(chatId)
    })

  // verify the bad word chat is removed
  await misc.sleep(2000)
  await ourClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.chatCount).toBe(1)
    expect(user.chats.items).toHaveLength(1)
    expect(user.chats.items[0].chatId).toBe(chatId)
    expect(user.chats.items[0].messageCount).toBe(0)
  })
  await ourClient.query({query: queries.chat, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
    expect(chat.message).toBeUndefined()
  })
})

test('Two way follow, skip bad word detection - direct chat', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we open up a direct chat with them
  const [chatId, messageId] = [uuidv4(), uuidv4()]
  const messageText = 'lore ipsum'
  let variables = {userId: theirUserId, chatId, messageId, messageText}

  await ourClient
    .mutate({mutation: mutations.createDirectChat, variables})
    .then(({data: {createDirectChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.chatType).toBe('DIRECT')
      expect(chat.name).toBeNull()
      expect(chat.userCount).toBe(2)
      expect(chat.usersCount).toBe(2)
      expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, theirUserId].sort())
      expect(chat.messages.items).toHaveLength(1)
      expect(chat.messages.items[0].messageId).toBe(messageId)
      expect(chat.messages.items[0].text).toBe(messageText)
      expect(chat.messages.items[0].chat.chatId).toBe(chatId)
      expect(chat.messages.items[0].author.userId).toBe(ourUserId)
      expect(chat.messages.items[0].viewedStatus).toBe('VIEWED')
    })

  // check we see the chat in our list of chats
  await ourClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.directChat).toBeNull()
    expect(user.chatCount).toBe(1)
    expect(user.chats.items).toHaveLength(1)
    expect(user.chats.items[0].chatId).toBe(chatId)
  })

  // they follow us
  await theirClient
    .mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
    .then(({data}) => expect(data.followUser.followedStatus).toBe('FOLLOWING'))

  // we follow them
  await ourClient
    .mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    .then(({data}) => expect(data.followUser.followedStatus).toBe('FOLLOWING'))

  // they add chat message with bad word, verify chat message is added
  const [messageId2, messageText2] = [uuidv4(), `msg ${badWord}`]
  variables = {chatId, messageId: messageId2, text: messageText2}
  await theirClient
    .mutate({mutation: mutations.addChatMessage, variables})
    .then(({data: {addChatMessage: chatMessage}}) => {
      expect(chatMessage.messageId).toBe(messageId2)
      expect(chatMessage.text).toBe(messageText2)
      expect(chatMessage.chat.chatId).toBe(chatId)
    })

  // check we see all chat messages
  await misc.sleep(1000)
  await ourClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.chatCount).toBe(1)
    expect(user.chats.items).toHaveLength(1)
    expect(user.chats.items[0].chatId).toBe(chatId)
  })
  await ourClient.query({query: queries.chat, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
    expect(chat.messages.items).toHaveLength(2)
    expect(chat.messages.items[0].messageId).toBe(messageId)
    expect(chat.messages.items[0].text).toBe(messageText)
    expect(chat.messages.items[0].chat.chatId).toBe(chatId)
    expect(chat.messages.items[1].messageId).toBe(messageId2)
    expect(chat.messages.items[1].text).toBe(messageText2)
    expect(chat.messages.items[1].chat.chatId).toBe(chatId)
  })
})

test('Create a group chat with bad word', async () => {
  const {client: ourClient, userId: ourUserId, username: ourUsername} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId, username: theirUsername} = await loginCache.getCleanLogin()
  const {client: otherClient, userId: otherUserId, username: otherUsername} = await loginCache.getCleanLogin()

  const [chatId, messageId1] = [uuidv4(), uuidv4()]
  let messageIdSystem0, messageIdSystem1
  let variables = {
    chatId,
    name: 'x',
    userIds: [theirUserId, otherUserId],
    messageId: messageId1,
    messageText: 'm',
  }
  await ourClient
    .mutate({mutation: mutations.createGroupChat, variables})
    .then(({data: {createGroupChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.chatType).toBe('GROUP')
      expect(chat.name).toBe('x')
      expect(chat.userCount).toBe(3)
      expect(chat.usersCount).toBe(3)
      expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, theirUserId, otherUserId].sort())
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
      expect(chat.messages.items[1].text).toContain(theirUsername)
      expect(chat.messages.items[1].text).toContain(otherUsername)
      expect(chat.messages.items[1].textTaggedUsers).toHaveLength(3)
      expect(chat.messages.items[1].textTaggedUsers.map((t) => t.tag).sort()).toEqual(
        [`@${ourUsername}`, `@${theirUsername}`, `@${otherUsername}`].sort(),
      )
      expect(chat.messages.items[1].textTaggedUsers.map((t) => t.user.userId).sort()).toEqual(
        [ourUserId, theirUserId, otherUserId].sort(),
      )
      expect(chat.messages.items[2].messageId).toBe(messageId1)
      expect(chat.messages.items[2].text).toBe('m')

      messageIdSystem0 = chat.messages.items[0].messageId
      messageIdSystem1 = chat.messages.items[1].messageId
    })

  // we add a message
  const messageId2 = uuidv4()
  variables = {chatId, messageId: messageId2, text: 'm2'}
  await ourClient
    .mutate({mutation: mutations.addChatMessage, variables})
    .then(({data: {addChatMessage: chatMessage}}) => {
      expect(chatMessage.messageId).toBe(messageId2)
    })

  // they add a message with bad word
  const messageId3 = uuidv4()
  variables = {chatId, messageId: messageId3, text: `m3 ${badWord}`}
  await theirClient
    .mutate({mutation: mutations.addChatMessage, variables})
    .then(({data: {addChatMessage: chatMessage}}) => {
      expect(chatMessage.messageId).toBe(messageId3)
    })

  // verify bad word chat message is removed
  await misc.sleep(2000)
  await otherClient.query({query: queries.chat, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
    expect(chat.messageCount).toBe(4)
    expect(chat.messagesCount).toBe(4)
    expect(chat.messages.items).toHaveLength(4)
    expect(chat.messages.items[0].messageId).toBe(messageIdSystem0)
    expect(chat.messages.items[1].messageId).toBe(messageIdSystem1)
    expect(chat.messages.items[2].messageId).toBe(messageId1)
    expect(chat.messages.items[3].messageId).toBe(messageId2)
  })
})

test('Create a group chat with bad word - skip if all users follow creator', async () => {
  const {client: ourClient, userId: ourUserId, username: ourUsername} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  const {client: otherClient, userId: otherUserId} = await loginCache.getCleanLogin()

  const [chatId, messageId1] = [uuidv4(), uuidv4()]
  let messageIdSystem0, messageIdSystem1
  let variables = {
    chatId,
    name: 'x',
    userIds: [theirUserId, otherUserId],
    messageId: messageId1,
    messageText: 'm',
  }
  await ourClient
    .mutate({mutation: mutations.createGroupChat, variables})
    .then(({data: {createGroupChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.chatType).toBe('GROUP')
      expect(chat.name).toBe('x')
      expect(chat.userCount).toBe(3)
      expect(chat.usersCount).toBe(3)
      expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, theirUserId, otherUserId].sort())
      expect(chat.messages.items).toHaveLength(3)
      expect(chat.messages.items[0].text).toContain(ourUsername)
      expect(chat.messages.items[0].text).toContain('created the group')
      expect(chat.messages.items[0].text).toContain('x')

      messageIdSystem0 = chat.messages.items[0].messageId
      messageIdSystem1 = chat.messages.items[1].messageId
    })

  // we add a message
  const messageId2 = uuidv4()
  variables = {chatId, messageId: messageId2, text: 'm2'}
  await ourClient
    .mutate({mutation: mutations.addChatMessage, variables})
    .then(({data: {addChatMessage: chatMessage}}) => {
      expect(chatMessage.messageId).toBe(messageId2)
    })

  // we and other follow them
  await ourClient
    .mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    .then(({data}) => expect(data.followUser.followedStatus).toBe('FOLLOWING'))
  await otherClient
    .mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    .then(({data}) => expect(data.followUser.followedStatus).toBe('FOLLOWING'))

  // they add a message with bad word
  const messageId3 = uuidv4()
  variables = {chatId, messageId: messageId3, text: `m3 ${badWord}`}
  await theirClient
    .mutate({mutation: mutations.addChatMessage, variables})
    .then(({data: {addChatMessage: chatMessage}}) => {
      expect(chatMessage.messageId).toBe(messageId3)
    })

  // verify other can see all messages
  await misc.sleep(2000)
  await otherClient.query({query: queries.chat, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
    expect(chat.messageCount).toBe(5)
    expect(chat.messagesCount).toBe(5)
    expect(chat.messages.items).toHaveLength(5)
    expect(chat.messages.items[0].messageId).toBe(messageIdSystem0)
    expect(chat.messages.items[1].messageId).toBe(messageIdSystem1)
    expect(chat.messages.items[2].messageId).toBe(messageId1)
    expect(chat.messages.items[3].messageId).toBe(messageId2)
    expect(chat.messages.items[4].messageId).toBe(messageId3)
  })

  // we add a message with bad word
  const messageId4 = uuidv4()
  variables = {chatId, messageId: messageId4, text: `m4 ${badWord}`}
  await ourClient
    .mutate({mutation: mutations.addChatMessage, variables})
    .then(({data: {addChatMessage: chatMessage}}) => {
      expect(chatMessage.messageId).toBe(messageId4)
    })

  // verify our bad chat message is removed
  await misc.sleep(2000)
  await ourClient.query({query: queries.chat, variables: {chatId}}).then(({data: {chat}}) => {
    expect(chat.chatId).toBe(chatId)
    expect(chat.messageCount).toBe(5)
    expect(chat.messagesCount).toBe(5)
    expect(chat.messages.items).toHaveLength(5)
    expect(chat.messages.items[0].messageId).toBe(messageIdSystem0)
    expect(chat.messages.items[1].messageId).toBe(messageIdSystem1)
    expect(chat.messages.items[2].messageId).toBe(messageId1)
    expect(chat.messages.items[3].messageId).toBe(messageId2)
    expect(chat.messages.items[4].messageId).toBe(messageId3)
  })
})
