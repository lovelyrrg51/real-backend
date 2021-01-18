const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()

let anonClient, anonUserId
beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => {
  if (anonClient) await anonClient.mutate({mutation: mutations.deleteUser})
  anonClient = null
})

test('New normal users do not get the user upsell card', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // verify that new normal user without profile photo do get card
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.email).toBeDefined()
    expect(user.userStatus).toBe('ACTIVE')
    expect(user.cards.items.length).toBe(1)

    let card = user.cards.items[0]
    expect(card.title).toBe('Add a profile photo')
  })
})

test('New anonymous users do get the user upsell card', async () => {
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
    expect(card.action).toBe(`https://real.app/signup/${anonUserId}`)
    expect(card.cardId).toBe(`${anonUserId}:ANONYMOUS_USER_UPSELL`)
  })
})
