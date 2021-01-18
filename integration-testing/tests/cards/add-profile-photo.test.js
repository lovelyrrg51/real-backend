const fs = require('fs')
const path = require('path')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()
const grantData = fs.readFileSync(path.join(__dirname, '..', '..', 'fixtures', 'grant.jpg'))
const grantDataB64 = new Buffer.from(grantData).toString('base64')

let anonClient, anonUserId
beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => {
  if (anonClient) await anonClient.mutate({mutation: mutations.deleteUser})
  anonClient = null
})

test('New anonymous users do not get the add profile photo card', async () => {
  ;({client: anonClient, userId: anonUserId} = await cognito.getAnonymousAppSyncLogin())

  // verify that new anonymous user do not get this card
  await misc.sleep(2000)
  await anonClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(anonUserId)
    expect(user.email).toBeNull()
    expect(user.userStatus).toBe('ANONYMOUS')
    expect(user.cardCount).toBe(1)
    expect(user.cards.items).toHaveLength(1)
    let card = user.cards.items[0]
    expect(card.title).toBe('Reserve your username & sign up!')
  })
})

test('New normal users without profile photo do get the add profile photo card', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // verify that new normal user without profile photo do get card
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.email).toBeDefined()
    expect(user.userStatus).toBe('ACTIVE')
    expect(user.photoPostId).toBeFalsy()
    expect(user.cards.items.length).toBe(1)

    let card = user.cards.items[0]
    expect(card.cardId).toBe(`${ourUserId}:ADD_PROFILE_PHOTO`)
    expect(card.title).toBe('Add a profile photo')
    expect(card.action).toBe(`https://real.app/user/${ourUserId}/settings/photo`)
  })

  // add a post they will use as a profile photo and verify that card is deleted
  const postId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId, imageData: grantDataB64, takenInReal: true}})
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId))

  await ourClient
    .mutate({mutation: mutations.setUserDetails, variables: {photoPostId: postId}})
    .then(({data: {setUserDetails: user}}) => expect(user.photo.url).toBeTruthy())

  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.userStatus).toBe('ACTIVE')
    expect(user.photo.url).toBeTruthy()
    expect(user.cards.items.length).toBe(0)
  })
})
