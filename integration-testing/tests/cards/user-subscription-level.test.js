const cognito = require('../../utils/cognito')
const misc = require('../../utils/misc')
const {mutations, queries} = require('../../schema')

const loginCache = new cognito.AppSyncLoginCache()

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('User subscription level card: generating, format', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we give ourselves some free diamond
  await ourClient
    .mutate({mutation: mutations.grantUserSubscriptionBonus})
    .then(({data: {grantUserSubscriptionBonus: user}}) => {
      expect(user.userId).toBe(ourUserId)
      expect(user.subscriptionLevel).toBe('DIAMOND')
    })
  await misc.sleep(2000)

  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.cardCount).toBe(2)
    expect(user.cards.items).toHaveLength(2)
    let card = user.cards.items[0]
    expect(card.cardId).toBe(`${ourUserId}:USER_SUBSCRIPTION_LEVEL`)
    expect(card.title).toBe('Welcome to Diamond')
    expect(card.subTitle).toBe('Enjoy exclusive perks of being a subscriber')
    expect(card.action).toBe('https://real.app/diamond')
    // second card is the 'Add a profile photo'
    expect(user.cards.items[1].title).toBe('Add a profile photo')
  })
})
