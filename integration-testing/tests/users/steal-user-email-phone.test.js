const {mutations, queries} = require('../../schema')
const cognito = require('../../utils/cognito')

const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin('+15105551333'))
  loginCache.addCleanLogin(await cognito.getAppSyncLogin('+15105551444'))
})
beforeEach(async () => await loginCache.clean())
afterAll(async () => await loginCache.reset())

test('Steal user email and phoneNumber exception', async () => {
  const {email: ourEmail, client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  const ourPhoneNumber = await ourClient
    .query({query: queries.self})
    .then(({data: {self: user}}) => user.phoneNumber)

  await expect(
    theirClient.mutate({mutation: mutations.startChangeUserEmail, variables: {email: ourEmail}}),
  ).rejects.toThrow(/ClientError: User email is already used by other/)

  await expect(
    theirClient.mutate({
      mutation: mutations.startChangeUserPhoneNumber,
      variables: {phoneNumber: ourPhoneNumber},
    }),
  ).rejects.toThrow(/GraphQL error: ClientError: User phoneNumber is already used by other/)
})
