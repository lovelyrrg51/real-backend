const fs = require('fs')
const path = require('path')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const grantData = fs.readFileSync(path.join(__dirname, '..', '..', 'fixtures', 'grant.jpg'))
const grantDataB64 = new Buffer.from(grantData).toString('base64')
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Exact match search on username', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // change our username to something random and hopefully unique
  const newUsername = 'TESTER' + misc.shortRandomString()
  let resp = await client.mutate({mutation: mutations.setUsername, variables: {username: newUsername}})

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: newUsername}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)
})

test('Prefix match on username', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // change our username to something random and hopefully unique
  const newUsernameFirstHalf = 'TESTER' + misc.shortRandomString()
  const newUsername = newUsernameFirstHalf + misc.shortRandomString() + 'yesyes'
  let resp = await client.mutate({mutation: mutations.setUsername, variables: {username: newUsername}})

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  // verify exact match works
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: newUsername}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)

  // verify the prefix match works
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: newUsernameFirstHalf}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)
})

test('Exact match search on fullName', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // change our fullName to something random and hopefully unique
  const fullName = 'FIRST' + misc.shortRandomString() + ' LAST' + misc.shortRandomString()
  let resp = await client.mutate({mutation: mutations.setUserDetails, variables: {fullName: fullName}})

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: fullName}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)
})

test('Search works on fullName, case insensitive', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // change our fullName to something random and hopefully unique
  const fullName = 'FIRST' + misc.shortRandomString() + ' LAST' + misc.shortRandomString()
  let resp = await client.mutate({mutation: mutations.setUserDetails, variables: {fullName: fullName}})

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  // search in all upper case, we should show up in the results
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: fullName.toUpperCase()}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)

  // search in all lower case, we should show up in the results
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: fullName.toLowerCase()}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)
})

test('Search works on fullName, searching one name at a time', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // change our fullName to something random and hopefully unique
  const firstName = 'First' + misc.shortRandomString()
  const lastName = 'Last' + misc.shortRandomString()
  const fullName = `${firstName} ${lastName}`
  let resp = await client.mutate({mutation: mutations.setUserDetails, variables: {fullName: fullName}})

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  // search with first name
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: firstName}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)

  // search with last name
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: lastName}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)
})

test('Search works on fullName, omitting middle name', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // change our fullName to something random and hopefully unique
  const firstName = 'First' + misc.shortRandomString()
  const middleName = 'Middle' + misc.shortRandomString()
  const lastName = 'Last' + misc.shortRandomString()
  const fullName = `${firstName} ${middleName} ${lastName}`
  let resp = await client.mutate({mutation: mutations.setUserDetails, variables: {fullName: fullName}})

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  // search with first name + last name
  const simpleName = `${firstName} ${lastName}`
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: simpleName}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)
})

test('Search works on fullName with prefix of name', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // change our fullName to something random and hopefully unique
  const firstName = 'First' + misc.shortRandomString()
  const middleName = 'Middle' + misc.shortRandomString()
  const lastName = 'Last' + misc.shortRandomString()
  const fullName = `${firstName} ${middleName} ${lastName}`
  let resp = await client.mutate({mutation: mutations.setUserDetails, variables: {fullName: fullName}})

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  // search with prefix of first name
  const partOfFirstName = firstName.substring(0, 8)
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: partOfFirstName}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)

  // search with preifx of last name
  const partOfLastName = lastName.substring(0, 8)
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: partOfLastName}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)
})

test('Cant do blank searches', async () => {
  const {client} = await loginCache.getCleanLogin()

  await client
    .query({query: queries.searchUsers, variables: {searchToken: null}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors.length).toBeTruthy()
      expect(data).toBeNull()
    })

  await client
    .query({query: queries.searchUsers, variables: {searchToken: ''}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors.length).toBeTruthy()
      expect(data).toBeNull()
    })

  await client
    .query({query: queries.searchUsers, variables: {searchToken: ' '}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors.length).toBeTruthy()
      expect(data).toBeNull()
    })

  await client
    .query({query: queries.searchUsers, variables: {searchToken: '   '}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors.length).toBeTruthy()
      expect(data).toBeNull()
    })

  await client
    .query({query: queries.searchUsers, variables: {searchToken: '\n'}, errorPolicy: 'all'})
    .then(({data, errors}) => {
      expect(errors.length).toBeTruthy()
      expect(data).toBeNull()
    })
})

test('Doesnt crash on special characters', async () => {
  const {client} = await loginCache.getCleanLogin()

  let resp = await client.query({query: queries.searchUsers, variables: {searchToken: '+'}})
  expect(resp.data.searchUsers.items).toHaveLength(0)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: '*'}})
  expect(resp.data.searchUsers.items).toHaveLength(0)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: '/'}})
  expect(resp.data.searchUsers.items).toHaveLength(0)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: '\\'}})
  expect(resp.data.searchUsers.items).toHaveLength(0)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: '"'}})
  expect(resp.data.searchUsers.items).toHaveLength(0)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: '?'}})
  expect(resp.data.searchUsers.items).toHaveLength(0)
})

test('Special characters do not work as wildcards', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // change our username to something random and hopefully unique
  const newUsername = 'TESTER' + misc.shortRandomString()
  let resp = await client.mutate({mutation: mutations.setUsername, variables: {username: newUsername}})

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  // verify we can see that user in search results
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: newUsername}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: '-'}})
  expect(resp.data.searchUsers.items).toHaveLength(0)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: '_'}})
  expect(resp.data.searchUsers.items).toHaveLength(0)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: '.'}})
  expect(resp.data.searchUsers.items).toHaveLength(0)

  resp = await client.query({query: queries.searchUsers, variables: {searchToken: "'"}})
  expect(resp.data.searchUsers.items).toHaveLength(0)
})

test('User search returns urls for profile pics', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // change our username to something random and hopefully unique
  const newUsername = 'TESTER' + misc.shortRandomString()
  let resp = await client.mutate({mutation: mutations.setUsername, variables: {username: newUsername}})

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  // do a search, and check that we do *not* see a photo
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: newUsername}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)
  expect(resp.data.searchUsers.items[0].photo).toBeNull()

  // add an image post, upload that image
  const postId = uuidv4()
  let variables = {postId, imageData: grantDataB64, takenInReal: true}
  resp = await client.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')

  // set our profile photo to that image
  resp = await client.mutate({mutation: mutations.setUserDetails, variables: {photoPostId: postId}})
  expect(resp.data.setUserDetails.photo.url).toBeTruthy()

  // do a search, and check that we see a photo
  resp = await client.query({query: queries.searchUsers, variables: {searchToken: newUsername}})
  expect(resp.data.searchUsers.items).toHaveLength(1)
  expect(resp.data.searchUsers.items[0].userId).toBe(userId)
  expect(resp.data.searchUsers.items[0].photo.url).toBeTruthy()
})

test('User search prioritizes exact match on username', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  const name = misc.shortRandomString()

  // make our username match that name exactly, fullname not match
  await ourClient
    .mutate({mutation: mutations.setUserDetails, variables: {username: name, fullName: misc.shortRandomString()}})
    .then(({data}) => {
      expect(data.setUserDetails.userId).toBe(ourUserId)
      expect(data.setUserDetails.username).toBe(name)
    })

  // they set their username to have ours as a prefix, and set their full name to contain it
  await theirClient
    .mutate({
      mutation: mutations.setUserDetails,
      variables: {username: name + misc.shortRandomString(), fullName: name + ' ' + misc.shortRandomString()},
    })
    .then(({data}) => expect(data.setUserDetails.userId).toBe(theirUserId))

  // give the search index a good chunk of time to update
  await misc.sleep(3000)

  // do a search with our username, check that we show up as first result
  await ourClient.query({query: queries.searchUsers, variables: {searchToken: name}}).then(({data}) => {
    expect(data.searchUsers.items).toHaveLength(2)
    expect(data.searchUsers.items[0].userId).toBe(ourUserId)
    expect(data.searchUsers.items[1].userId).toBe(theirUserId)
  })
})
