const fs = require('fs')
const moment = require('moment')
const path = require('path')
const uuidv4 = require('uuid/v4')
// the aws-appsync-subscription-link pacakge expects WebSocket to be globaly defined, like in the browser
global.WebSocket = require('ws')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, subscriptions} = require('../../schema')

const grantData = fs.readFileSync(path.join(__dirname, '..', '..', 'fixtures', 'grant.jpg'))
const grantDataB64 = new Buffer.from(grantData).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Chat message triggers cannot be called from external graphql client', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // they open up a chat with us
  const [chatId, messageId] = [uuidv4(), uuidv4()]
  await theirClient
    .mutate({
      mutation: mutations.createDirectChat,
      variables: {userId: ourUserId, chatId, messageId, messageText: 'lore ipsum'},
    })
    .then(({data: {createDirectChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.messages.items).toHaveLength(1)
      expect(chat.messages.items[0].messageId).toBe(messageId)
    })

  // create a well-formed valid chat notification object
  // verify niether of us can call the trigger method, even with a valid chat & message id
  const mutation = mutations.triggerChatMessageNotification
  const variables = {
    input: {
      userId: ourUserId,
      messageId,
      chatId,
      authorUserId: ourUserId,
      type: 'ADDED',
      text: 'lore ipsum',
      textTaggedUserIds: [],
      createdAt: moment().toISOString(),
    },
  }
  await expect(ourClient.mutate({mutation, variables})).rejects.toThrow(/ClientError: Access denied/)
  await expect(theirClient.mutate({mutation, variables})).rejects.toThrow(/ClientError: Access denied/)
})

test('Cannot subscribe to other users messages', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // verify we cannot subscribe to their messages
  // Note: there doesn't seem to be any error thrown at the time of subscription, it's just that
  // the subscription next() method is never triggered
  await ourClient
    .subscribe({query: subscriptions.onChatMessageNotification, variables: {userId: theirUserId}})
    .subscribe({
      next: (resp) => expect(`Subscription should not be called: ${resp}`).toBeNull(),
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })

  // they open up a chat with us
  const [chatId, messageId1] = [uuidv4(), uuidv4()]
  await theirClient
    .mutate({
      mutation: mutations.createDirectChat,
      variables: {userId: ourUserId, chatId, messageId: messageId1, messageText: 'hey, msg1'},
    })
    .then(({data: {createDirectChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.messages.items).toHaveLength(1)
      expect(chat.messages.items[0].messageId).toBe(messageId1)
    })

  // we send a messsage to the chat
  await ourClient
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: uuidv4(), text: 'lore'}})
    .then(({data: {addChatMessage: message}}) => expect(message.chat.chatId).toBe(chatId))

  // they send a messsage to the chat
  await theirClient
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: uuidv4(), text: 'ipsum'}})
    .then(({data: {addChatMessage: message}}) => expect(message.chat.chatId).toBe(chatId))

  // wait for some messages to show up, if they do test will fail
  await misc.sleep(5000)

  // we don't unsubscribe from the subscription because
  //  - it's not actually active, although I have yet to find a way to expect() that
  //  - unsubcribing results in the AWS SDK throwing errors
})

test('Messages in multiple chats fire', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  const {client: otherClient, userId: otherUserId} = await loginCache.getCleanLogin()

  // we subscribe to chat messages
  const ourHandlers = []
  const ourSub = await ourClient
    .subscribe({query: subscriptions.onChatMessageNotification, variables: {userId: ourUserId}})
    .subscribe({
      next: ({data: {onChatMessageNotification: notification}}) => {
        const handler = ourHandlers.shift()
        expect(handler).toBeDefined()
        handler(notification)
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  const ourSubInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541

  // they subscribe to chat messages
  const theirHandlers = []
  const theirSub = await theirClient
    .subscribe({query: subscriptions.onChatMessageNotification, variables: {userId: theirUserId}})
    .subscribe({
      next: ({data: {onChatMessageNotification: notification}}) => {
        const handler = theirHandlers.shift()
        expect(handler).toBeDefined()
        handler(notification)
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  const theirSubInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541

  // other subscribes to chat messages
  const otherHandlers = []
  const otherSub = await otherClient
    .subscribe({query: subscriptions.onChatMessageNotification, variables: {userId: otherUserId}})
    .subscribe({
      next: ({data: {onChatMessageNotification: notification}}) => {
        const handler = otherHandlers.shift()
        expect(handler).toBeDefined()
        handler(notification)
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  const otherSubInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscriptions initialize

  // we open a direct chat with them
  let theirNextNotification = new Promise((resolve) => theirHandlers.push(resolve))
  const [chatId, messageId1] = [uuidv4(), uuidv4()]
  await ourClient
    .mutate({
      mutation: mutations.createDirectChat,
      variables: {userId: theirUserId, chatId, messageId: messageId1, messageText: 'm1'},
    })
    .then(({data}) => {
      expect(data.createDirectChat.chatId).toBe(chatId)
      expect(data.createDirectChat.messages.items).toHaveLength(1)
      expect(data.createDirectChat.messages.items[0].messageId).toBe(messageId1)
    })
  await theirNextNotification.then((notification) => {
    expect(notification.message.messageId).toBe(messageId1)
    expect(notification.message.authorUserId).toBe(ourUserId)
  })

  // they post a message to the chat
  let ourNextNotification = new Promise((resolve) => ourHandlers.push(resolve))
  const messageId2 = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: messageId2, text: 'm2'}})
    .then(({data}) => {
      expect(data.addChatMessage.chat.chatId).toBe(chatId)
      expect(data.addChatMessage.messageId).toBe(messageId2)
    })
  await ourNextNotification.then((notification) => {
    expect(notification.message.messageId).toBe(messageId2)
    expect(notification.message.authorUserId).toBe(theirUserId)
  })

  /* Use me to establish order among notifications */
  const notificationCompare = (a, b) => {
    const aTextReversed = a.message.text.split('').reverse().join('')
    const bTextReversed = b.message.text.split('').reverse().join('')
    return aTextReversed.localeCompare(bTextReversed)
  }

  // other opens a group chat with all three of us, verify we each receive two notifications
  // where order is not guaranteed (though the messages in the chat are)
  const [chatId2, messageId3] = [uuidv4(), uuidv4()]
  const ourNextTwoNotifications = new Promise((resolve) => {
    let firstNotification
    ourHandlers.push((notification) => (firstNotification = notification))
    ourHandlers.push((notification) => resolve([firstNotification, notification]))
  })
  const theirNextTwoNotifications = new Promise((resolve) => {
    let firstNotification
    theirHandlers.push((notification) => (firstNotification = notification))
    theirHandlers.push((notification) => resolve([firstNotification, notification]))
  })
  const otherNextTwoNotifications = new Promise((resolve) => {
    let firstNotification
    otherHandlers.push((notification) => (firstNotification = notification))
    otherHandlers.push((notification) => resolve([firstNotification, notification]))
  })
  await otherClient
    .mutate({
      mutation: mutations.createGroupChat,
      variables: {chatId: chatId2, userIds: [ourUserId, theirUserId], messageId: messageId3, messageText: 'm3'},
    })
    .then(({data}) => expect(data.createGroupChat.chatId).toBe(chatId2))
  await ourNextTwoNotifications.then((notifications) => {
    notifications.sort(notificationCompare)
    expect(notifications[0].message.messageId).toBe(messageId3)
    expect(notifications[0].message.authorUserId).toBe(otherUserId)
    expect(notifications[1].message.text).toContain('added')
    expect(notifications[1].message.authorUserId).toBeNull()
  })
  await theirNextTwoNotifications.then((notifications) => {
    notifications.sort(notificationCompare)
    expect(notifications[0].message.messageId).toBe(messageId3)
    expect(notifications[0].message.authorUserId).toBe(otherUserId)
    expect(notifications[1].message.text).toContain('added')
    expect(notifications[1].message.authorUserId).toBeNull()
  })
  await otherNextTwoNotifications.then((notifications) => {
    expect(notifications[0].message.text).toContain('created')
    expect(notifications[0].message.authorUserId).toBeNull()
    expect(notifications[1].message.text).toContain('added')
    expect(notifications[1].message.authorUserId).toBeNull()
  })

  // we post a message to the group chat
  theirNextNotification = new Promise((resolve) => theirHandlers.push(resolve))
  let otherNextNotification = new Promise((resolve) => otherHandlers.push(resolve))
  const messageId4 = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.addChatMessage,
      variables: {chatId: chatId2, messageId: messageId4, text: 'm4'},
    })
    .then(({data}) => {
      expect(data.addChatMessage.chat.chatId).toBe(chatId2)
      expect(data.addChatMessage.messageId).toBe(messageId4)
    })
  await theirNextNotification.then((notification) => {
    expect(notification.message.messageId).toBe(messageId4)
    expect(notification.message.authorUserId).toBe(ourUserId)
  })
  await otherNextNotification.then((notification) => {
    expect(notification.message.messageId).toBe(messageId4)
    expect(notification.message.authorUserId).toBe(ourUserId)
  })

  // shut down the subscriptions
  ourSub.unsubscribe()
  theirSub.unsubscribe()
  otherSub.unsubscribe()
  await ourSubInitTimeout
  await theirSubInitTimeout
  await otherSubInitTimeout
})

test('Format for ADDED, EDITED, DELETED message notifications', async () => {
  const {client: ourClient, userId: ourUserId, username: ourUsername} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId, username: theirUsername} = await loginCache.getCleanLogin()

  // they open up a chat with us
  const [chatId, messageId1] = [uuidv4(), uuidv4()]
  await theirClient
    .mutate({
      mutation: mutations.createDirectChat,
      variables: {userId: ourUserId, chatId, messageId: messageId1, messageText: 'hey m1'},
    })
    .then(({data: {createDirectChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.messages.items).toHaveLength(1)
      expect(chat.messages.items[0].messageId).toBe(messageId1)
    })

  // we subscribe to our chat message notificaitons
  const handlers = []
  const sub = await ourClient
    .subscribe({query: subscriptions.onChatMessageNotification, variables: {userId: ourUserId}})
    .subscribe({
      next: ({data: {onChatMessageNotification: notification}}) => {
        const handler = handlers.shift()
        expect(handler).toBeDefined()
        handler(notification)
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  const subInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscription initialize

  // they add a message to the chat, verify notification
  let nextNotification = new Promise((resolve) => handlers.push(resolve))
  const [messageId2, text2] = [uuidv4(), `hi @${ourUsername}!`]
  const createdAt = await theirClient
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: messageId2, text: text2}})
    .then(({data: {addChatMessage: message}}) => {
      expect(message.messageId).toBe(messageId2)
      expect(message.chat.chatId).toBe(chatId)
      expect(message.createdAt).toBeTruthy()
      return message.createdAt
    })
  await nextNotification.then((notification) => {
    expect(notification.userId).toBe(ourUserId)
    expect(notification.type).toBe('ADDED')
    expect(notification.message.messageId).toBe(messageId2)
    expect(notification.message.chat.chatId).toBe(chatId)
    expect(notification.message.authorUserId).toBe(theirUserId)
    expect(notification.message.author.userId).toBe(theirUserId)
    expect(notification.message.author.username).toBe(theirUsername)
    expect(notification.message.author.photo).toBeNull()
    expect(notification.message.text).toBe(text2)
    expect(notification.message.textTaggedUsers).toHaveLength(1)
    expect(notification.message.textTaggedUsers[0].tag).toBe(`@${ourUsername}`)
    expect(notification.message.textTaggedUsers[0].user.userId).toBe(ourUserId)
    expect(notification.message.createdAt).toBe(createdAt)
    expect(notification.message.lastEditedAt).toBeNull()
  })

  // they add a post they will use as a profile photo
  const postId = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData: grantDataB64, takenInReal: true}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId))

  // they set that post as their profile photo
  await theirClient
    .mutate({mutation: mutations.setUserDetails, variables: {photoPostId: postId}})
    .then(({data: {setUserDetails: user}}) => expect(user.photo.url).toBeTruthy())

  // they edit their message to the chat, verify notification
  nextNotification = new Promise((resolve) => handlers.push(resolve))
  const text3 = `this is @${theirUsername}!`
  const lastEditedAt = await theirClient
    .mutate({mutation: mutations.editChatMessage, variables: {messageId: messageId2, text: text3}})
    .then(({data: {editChatMessage: message}}) => {
      expect(message.messageId).toBe(messageId2)
      expect(message.text).toBe(text3)
      expect(message.lastEditedAt).toBeTruthy()
      return message.lastEditedAt
    })
  await nextNotification.then((notification) => {
    expect(notification.userId).toBe(ourUserId)
    expect(notification.type).toBe('EDITED')
    expect(notification.message.messageId).toBe(messageId2)
    expect(notification.message.chat.chatId).toBe(chatId)
    expect(notification.message.authorUserId).toBe(theirUserId)
    expect(notification.message.author.userId).toBe(theirUserId)
    expect(notification.message.author.username).toBe(theirUsername)
    expect(notification.message.author.photo.url64p).toBeTruthy()
    expect(notification.message.text).toBe(text3)
    expect(notification.message.textTaggedUsers).toHaveLength(1)
    expect(notification.message.textTaggedUsers[0].tag).toBe(`@${theirUsername}`)
    expect(notification.message.textTaggedUsers[0].user.userId).toBe(theirUserId)
    expect(notification.message.createdAt).toBe(createdAt)
    expect(notification.message.lastEditedAt).toBe(lastEditedAt)
  })

  // they delete their message to the chat, verify notification
  nextNotification = new Promise((resolve) => handlers.push(resolve))
  await theirClient
    .mutate({mutation: mutations.deleteChatMessage, variables: {messageId: messageId2}})
    .then(({data: {deleteChatMessage: message}}) => expect(message.messageId).toBe(messageId2))
  await nextNotification.then((notification) => {
    expect(notification.userId).toBe(ourUserId)
    expect(notification.type).toBe('DELETED')
    expect(notification.message.messageId).toBe(messageId2)
    expect(notification.message.chat.chatId).toBe(chatId)
    expect(notification.message.authorUserId).toBe(theirUserId)
    expect(notification.message.author.userId).toBe(theirUserId)
    expect(notification.message.author.username).toBe(theirUsername)
    expect(notification.message.author.photo.url64p).toBeTruthy()
    expect(notification.message.text).toBe(text3)
    expect(notification.message.textTaggedUsers).toHaveLength(1)
    expect(notification.message.textTaggedUsers[0].tag).toBe(`@${theirUsername}`)
    expect(notification.message.textTaggedUsers[0].user.userId).toBe(theirUserId)
    expect(notification.message.createdAt).toBe(createdAt)
    expect(notification.message.lastEditedAt).toBe(lastEditedAt)
  })

  // shut down the subscription
  sub.unsubscribe()
  await subInitTimeout
})

test('Notifications for a group chat', async () => {
  const {client: ourClient, userId: ourUserId, username: ourUsername} = await loginCache.getCleanLogin()
  const {client: other1Client, userId: other1UserId} = await loginCache.getCleanLogin()
  const {client: other2Client, userId: other2UserId} = await loginCache.getCleanLogin()

  // we create a group chat with all of us in it
  const chatId = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.createGroupChat,
      variables: {chatId, userIds: [other1UserId, other2UserId], messageId: uuidv4(), messageText: 'm1'},
    })
    .then(({data: {createGroupChat: chat}}) => expect(chat.chatId).toBe(chatId))

  // we initialize a subscription to new message notifications
  const handlers = []
  const sub = await ourClient
    .subscribe({query: subscriptions.onChatMessageNotification, variables: {userId: ourUserId}})
    .subscribe({
      next: ({data: {onChatMessageNotification: notification}}) => {
        const handler = handlers.shift()
        expect(handler).toBeDefined()
        handler(notification)
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  const subInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscription initialize

  // other1 adds a message to the chat, verify notification
  let nextNotification = new Promise((resolve) => handlers.push(resolve))
  const messageId2 = uuidv4()
  await other1Client
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: messageId2, text: 'text 2'}})
    .then(({data: {addChatMessage: message}}) => expect(message.messageId).toBe(messageId2))
  await nextNotification.then((notification) => {
    expect(notification.message.messageId).toBe(messageId2)
    expect(notification.message.authorUserId).toBe(other1UserId)
  })

  // we edit group name to trigger a system message, verify notification
  nextNotification = new Promise((resolve) => handlers.push(resolve))
  await ourClient
    .mutate({mutation: mutations.editGroupChat, variables: {chatId, name: 'new name'}})
    .then(({data: {editGroupChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.name).toBe('new name')
    })
  await nextNotification.then((notification) => {
    expect(notification.message.messageId).toBeTruthy()
    expect(notification.message.text).toContain(ourUsername)
    expect(notification.message.text).toContain('changed the name of the group')
    expect(notification.message.text).toContain('"new name"')
    expect(notification.message.textTaggedUsers).toHaveLength(1)
    expect(notification.message.textTaggedUsers[0].tag).toContain(ourUsername)
    expect(notification.message.textTaggedUsers[0].user.userId).toContain(ourUserId)
    expect(notification.message.authorUserId).toBeNull()
    expect(notification.message.author).toBeNull()
  })

  // other2 adds a message to the chat, verify notification
  nextNotification = new Promise((resolve) => handlers.push(resolve))
  const messageId3 = uuidv4()
  await other2Client
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: messageId3, text: 'text 3'}})
    .then(({data: {addChatMessage: message}}) => expect(message.messageId).toBe(messageId3))
  await nextNotification.then((notification) => {
    expect(notification.message.messageId).toBe(messageId3)
    expect(notification.message.authorUserId).toBe(other2UserId)
  })

  // shut down our subscription
  sub.unsubscribe()
  await subInitTimeout
})

test('Message notifications from blocke[r|d] users have authorUserId but no author', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we create a group chat with both of us in it
  const chatId = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.createGroupChat,
      variables: {chatId, userIds: [theirUserId], messageId: uuidv4(), messageText: 'm1'},
    })
    .then(({data: {createGroupChat: chat}}) => {
      expect(chat.chatId).toBe(chatId)
      expect(chat.userCount).toBe(2)
      expect(chat.usersCount).toBe(2)
      expect(chat.users.items.map((u) => u.userId).sort()).toEqual([ourUserId, theirUserId].sort())
    })

  // they block us
  await theirClient
    .mutate({mutation: mutations.blockUser, variables: {userId: ourUserId}})
    .then(({data: {blockUser: user}}) => {
      expect(user.userId).toBe(ourUserId)
      expect(user.blockedStatus).toBe('BLOCKING')
    })

  // they listen to message notifciations
  const theirHandlers = []
  const theirSub = await theirClient
    .subscribe({query: subscriptions.onChatMessageNotification, variables: {userId: theirUserId}})
    .subscribe({
      next: ({data: {onChatMessageNotification: notification}}) => {
        const handler = theirHandlers.shift()
        expect(handler).toBeDefined()
        handler(notification)
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  const theirSubInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscription initialize

  // we add a message, verify they received notificaiton received without author
  let theirNextNotification = new Promise((resolve) => theirHandlers.push(resolve))
  const messageId2 = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: messageId2, text: 'lore'}})
    .then(({data: {addChatMessage: message}}) => expect(message.messageId).toBe(messageId2))
  await theirNextNotification.then((notification) => {
    expect(notification.message.messageId).toBe(messageId2)
    expect(notification.message.authorUserId).toBe(ourUserId)
    expect(notification.message.author).toBeNull()
  })

  // we listen to notifciations
  const ourHandlers = []
  const ourSub = await ourClient
    .subscribe({query: subscriptions.onChatMessageNotification, variables: {userId: ourUserId}})
    .subscribe({
      next: ({data: {onChatMessageNotification: notification}}) => {
        const handler = ourHandlers.shift()
        expect(handler).toBeDefined()
        handler(notification)
      },
      error: (resp) => expect(`Subscription error: ${resp}`).toBeNull(),
    })
  const ourSubInitTimeout = misc.sleep(15000) // https://github.com/awslabs/aws-mobile-appsync-sdk-js/issues/541
  await misc.sleep(2000) // let the subscription initialize

  // they add a message, verify we receive notification without author
  let ourNextNotification = new Promise((resolve) => ourHandlers.push(resolve))
  const messageId3 = uuidv4()
  await theirClient
    .mutate({mutation: mutations.addChatMessage, variables: {chatId, messageId: messageId3, text: 'ipsum'}})
    .then(({data: {addChatMessage: chat}}) => expect(chat.messageId).toBe(messageId3))
  await ourNextNotification.then((notification) => {
    expect(notification.message.messageId).toBe(messageId3)
    expect(notification.message.authorUserId).toBe(theirUserId)
    expect(notification.message.author).toBeNull()
  })

  // shut down the subscriptions
  ourSub.unsubscribe()
  theirSub.unsubscribe()
  await ourSubInitTimeout
  await theirSubInitTimeout
})
