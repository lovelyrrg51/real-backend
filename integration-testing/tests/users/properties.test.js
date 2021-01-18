const fs = require('fs')
const got = require('got')
const path = require('path')
const uuidv4 = require('uuid/v4')

const cognito = require('../../utils/cognito')
const {mutations, queries} = require('../../schema')

let anonClient
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
afterEach(async () => {
  if (anonClient) await anonClient.mutate({mutation: mutations.deleteUser})
  anonClient = null
})

describe('Read and write properties our our own profile', () => {
  // username is tested in the set-username.test.js

  test('followed/follwer status', async () => {
    const {client, userId} = await loginCache.getCleanLogin()
    let resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.followedStatus).toBe('SELF')
    expect(resp.data.user.followerStatus).toBe('SELF')
  })

  test('privacyStatus', async () => {
    const {client, userId} = await loginCache.getCleanLogin()
    let resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.privacyStatus).toBe('PUBLIC')

    resp = await client.mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
    expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

    resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.privacyStatus).toBe('PRIVATE')

    resp = await client.mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PUBLIC'}})
    expect(resp.data.setUserDetails.privacyStatus).toBe('PUBLIC')

    resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.privacyStatus).toBe('PUBLIC')
  })

  test('fullName and bio', async () => {
    const bio = "truckin'"
    const fullName = 'Hunter S.'
    const {client, userId} = await loginCache.getCleanLogin()

    let resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.bio).toBeNull()
    expect(resp.data.user.fullName).toBeNull()

    // set to some custom values
    resp = await client.mutate({mutation: mutations.setUserDetails, variables: {bio, fullName}})
    expect(resp.data.setUserDetails.bio).toBe(bio)
    expect(resp.data.setUserDetails.fullName).toBe(fullName)

    resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.bio).toBe(bio)
    expect(resp.data.user.fullName).toBe(fullName)

    // clear out the custom values
    resp = await client.mutate({mutation: mutations.setUserDetails, variables: {bio: '', fullName: ''}})
    expect(resp.data.setUserDetails.bio).toBeNull()
    expect(resp.data.setUserDetails.fullName).toBeNull()

    resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.bio).toBeNull()
    expect(resp.data.user.fullName).toBeNull()
  })

  test('displayName', async () => {
    const displayName = 'Hunter S.'
    const {client, userId} = await loginCache.getCleanLogin()

    let resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.displayName).toBeNull()

    // set to some custom values
    resp = await client.mutate({mutation: mutations.setUserDetails, variables: {displayName}})
    expect(resp.data.setUserDetails.displayName).toBe(displayName)

    resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.displayName).toBe(displayName)

    // clear out the custom values
    resp = await client.mutate({mutation: mutations.setUserDetails, variables: {displayName: ''}})
    expect(resp.data.setUserDetails.displayName).toBeNull()

    resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.displayName).toBeNull()
  })

  test('Invalid dateOfBirth contains timezone information', async () => {
    const {client} = await loginCache.getCleanLogin()

    let dateOfBirth = '2020-01-01Z'
    await expect(client.mutate({mutation: mutations.setUserDetails, variables: {dateOfBirth}})).rejects.toThrow(
      /ClientError: dateOfBirth contains timezone information/,
    )

    dateOfBirth = '1970-01-01-07:00'
    await expect(client.mutate({mutation: mutations.setUserDetails, variables: {dateOfBirth}})).rejects.toThrow(
      /ClientError: dateOfBirth contains timezone information/,
    )

    dateOfBirth = '1970-01-01+05:30'
    await expect(client.mutate({mutation: mutations.setUserDetails, variables: {dateOfBirth}})).rejects.toThrow(
      /ClientError: dateOfBirth contains timezone information/,
    )
  })

  test('DateOfBirth and Gender', async () => {
    const dateOfBirth = '2020-12-31'
    const dateOfBirth2 = '1900-01-01'
    const {client, userId} = await loginCache.getCleanLogin()

    // check values start unset
    let resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.dateOfBirth).toBeNull()
    expect(resp.data.user.gender).toBeNull()

    // Set values to something, verify
    resp = await client.mutate({mutation: mutations.setUserDetails, variables: {dateOfBirth, gender: 'MALE'}})
    expect(resp.data.setUserDetails.dateOfBirth).toBe(dateOfBirth)
    expect(resp.data.setUserDetails.gender).toBe('MALE')

    // Set values to something else, verify
    resp = await client.mutate({
      mutation: mutations.setUserDetails,
      variables: {dateOfBirth: dateOfBirth2, gender: 'FEMALE'},
    })
    expect(resp.data.setUserDetails.dateOfBirth).toBe(dateOfBirth2)
    expect(resp.data.setUserDetails.gender).toBe('FEMALE')

    // double check values were saved to DB
    resp = await client.query({query: queries.user, variables: {userId}})
    expect(resp.data.user.dateOfBirth).toBe(dateOfBirth2)
    expect(resp.data.user.gender).toBe('FEMALE')
  })
})

test('Disabled user cannot setUserDetails', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // disable ourselves
  let resp = await client.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(userId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify can't edit our details
  await expect(client.mutate({mutation: mutations.setUserDetails, variables: {bio: 'a dog'}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )
})

test('Anonymous user cannot setUserDetails', async () => {
  ;({client: anonClient} = await cognito.getAnonymousAppSyncLogin())
  await expect(
    anonClient.mutate({mutation: mutations.setUserDetails, variables: {bio: 'a dog'}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)
})

test('setUserDetails without any arguments returns an error', async () => {
  const {client} = await loginCache.getCleanLogin()
  await expect(client.mutate({mutation: mutations.setUserDetails})).rejects.toThrow(
    /ClientError: Called without any arguments/,
  )
})

test('Try to get user that does not exist', async () => {
  const {client} = await loginCache.getCleanLogin()
  const userId = uuidv4()

  let resp = await client.query({query: queries.user, variables: {userId}})
  expect(resp.data.user).toBeNull()
})

test('Various photoPostId failures', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // verify can't set profile photo using post that doesn't exist
  let postId = 'post-id-dne'
  await expect(
    ourClient.mutate({mutation: mutations.setUserDetails, variables: {photoPostId: 'post-id-dne'}}),
  ).rejects.toThrow(/ClientError: .*not found/)

  // create a text-only post
  postId = uuidv4()
  let variables = {postId, text: 'l', postType: 'TEXT_ONLY'}
  let resp = await ourClient.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.postType).toBe('TEXT_ONLY')

  // verify can't set profile photo using text-only post
  await expect(
    ourClient.mutate({mutation: mutations.setUserDetails, variables: {photoPostId: postId}}),
  ).rejects.toThrow(/ClientError: .*does not have type/)

  // create an image post, leave it in pending
  postId = uuidv4()
  resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, postType: 'IMAGE'}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('PENDING')
  expect(resp.data.addPost.postType).toBe('IMAGE')

  // verify can't set profile photo using pending image post
  variables = {photoPostId: postId}
  await expect(ourClient.mutate({mutation: mutations.setUserDetails, variables})).rejects.toThrow(
    /ClientError: .*does not have status/,
  )

  // the other user creates an image post
  postId = uuidv4()
  resp = await theirClient.mutate({mutation: mutations.addPost, variables: {postId, imageData: grantDataB64}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.postType).toBe('IMAGE')

  // verify can't set our profile photo using their post
  variables = {photoPostId: postId}
  await expect(ourClient.mutate({mutation: mutations.setUserDetails, variables})).rejects.toThrow(
    /ClientError: .*does not belong to/,
  )

  // we create an image post that doesn't pass verification
  postId = uuidv4()
  resp = await ourClient.mutate({mutation: mutations.addPost, variables: {postId, imageData: grantDataB64}})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.postType).toBe('IMAGE')
  expect(resp.data.addPost.isVerified).toBe(false)

  // verify can't set our profile photo using non-verified post
  variables = {photoPostId: postId}
  await expect(ourClient.mutate({mutation: mutations.setUserDetails, variables})).rejects.toThrow(
    /ClientError: .*is not verified/,
  )
})

test('Set and delete our profile photo, using postId', async () => {
  const {client} = await loginCache.getCleanLogin()

  // check that it's not already set
  let resp = await client.query({query: queries.self})
  expect(resp.data.self.photo).toBeNull()

  // create a post with an image that we can use as a profile pic
  const postId = uuidv4()
  let variables = {postId, imageData: grantDataB64, takenInReal: true}
  resp = await client.mutate({mutation: mutations.addPost, variables})
  expect(resp.data.addPost.postId).toBe(postId)
  expect(resp.data.addPost.postStatus).toBe('COMPLETED')
  expect(resp.data.addPost.postType).toBe('IMAGE')

  // set our photo
  resp = await client.mutate({mutation: mutations.setUserDetails, variables: {photoPostId: postId}})
  let image = resp.data.setUserDetails.photo
  expect(image.url).toBeTruthy()
  expect(image.url64p).toBeTruthy()
  expect(image.url480p).toBeTruthy()
  expect(image.url1080p).toBeTruthy()
  expect(image.url4k).toBeTruthy()

  // check that it is really set already set, and that root urls are same as before
  resp = await client.query({query: queries.self})
  expect(image.url.split('?')[0]).toBe(resp.data.self.photo.url.split('?')[0])
  expect(image.url64p.split('?')[0]).toBe(resp.data.self.photo.url64p.split('?')[0])
  expect(image.url480p.split('?')[0]).toBe(resp.data.self.photo.url480p.split('?')[0])
  expect(image.url1080p.split('?')[0]).toBe(resp.data.self.photo.url1080p.split('?')[0])
  expect(image.url4k.split('?')[0]).toBe(resp.data.self.photo.url4k.split('?')[0])

  // check we can access those urls
  await got.head(image.url)
  await got.head(image.url4k)
  await got.head(image.url1080p)
  await got.head(image.url480p)
  await got.head(image.url64p)

  // delete our photo
  resp = await client.mutate({mutation: mutations.setUserDetails, variables: {photoPostId: ''}})
  expect(resp.data.setUserDetails.photo).toBeNull()

  // check that it really got deleted
  resp = await client.query({query: queries.self})
  expect(resp.data.self.photo).toBeNull()
})

describe('wrapper to ensure cleanup', () => {
  const theirPhone = '+15105551000'
  let theirClient, theirUserId, theirEmail
  beforeAll(async () => {
    ;({client: theirClient, userId: theirUserId, email: theirEmail} = await cognito.getAppSyncLogin(theirPhone))
  })
  afterAll(async () => {
    if (theirClient) await theirClient.mutate({mutation: mutations.deleteUser})
  })

  test('Read properties of another private user', async () => {
    const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

    // set up another user in cognito, mark them as private
    const theirBio = 'keeping calm and carrying on'
    const theirFullName = 'HG Wells'
    await theirClient
      .mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
      .then(({data: {setUserDetails: user}}) => expect(user.privacyStatus).toBe('PRIVATE'))
    await theirClient.mutate({
      mutation: mutations.setUserDetails,
      variables: {bio: theirBio, fullName: theirFullName},
    })

    // verify they can see all their properties (make sure they're all set correctly)
    await theirClient.query({query: queries.self}).then(({data: {self: user}}) => {
      expect(user.followedStatus).toBe('SELF')
      expect(user.followerStatus).toBe('SELF')
      expect(user.privacyStatus).toBe('PRIVATE')
      expect(user.fullName).toBe(theirFullName)
      expect(user.bio).toBe(theirBio)
      expect(user.email).toBe(theirEmail)
      expect(user.phoneNumber).toBe(theirPhone)
    })

    // verify that we can only see info that is expected of a non-follower
    await ourClient.query({query: queries.user, variables: {userId: theirUserId}}).then(({data: {user}}) => {
      expect(user.followedStatus).toBe('NOT_FOLLOWING')
      expect(user.followerStatus).toBe('NOT_FOLLOWING')
      expect(user.privacyStatus).toBe('PRIVATE')
      expect(user.fullName).toBe(theirFullName)
      expect(user.bio).toBeNull()
      expect(user.email).toBeNull()
      expect(user.phoneNumber).toBeNull()
    })

    // request to follow the user, verify we cannot see anything more
    await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    await ourClient.query({query: queries.user, variables: {userId: theirUserId}}).then(({data: {user}}) => {
      expect(user.followedStatus).toBe('REQUESTED')
      expect(user.fullName).toBe(theirFullName)
      expect(user.bio).toBeNull()
      expect(user.email).toBeNull()
      expect(user.phoneNumber).toBeNull()
    })

    // verify we see the same thing if we access their user profile indirectly
    await ourClient
      .query({query: queries.ourFollowedUsers, variables: {followStatus: 'REQUESTED'}})
      .then(({data: {self: user}}) => {
        expect(user.followedUsers.items).toHaveLength(1)
        expect(user.followedUsers.items[0].followedStatus).toBe('REQUESTED')
        expect(user.followedUsers.items[0].fullName).toBe(theirFullName)
        expect(user.followedUsers.items[0].bio).toBeNull()
        expect(user.followedUsers.items[0].email).toBeNull()
        expect(user.followedUsers.items[0].phoneNumber).toBeNull()
      })

    // accept the user's follow request, verify we can see more
    await theirClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: ourUserId}})
    await ourClient.query({query: queries.user, variables: {userId: theirUserId}}).then(({data: {user}}) => {
      expect(user.followedStatus).toBe('FOLLOWING')
      expect(user.fullName).toBe(theirFullName)
      expect(user.bio).toBe(theirBio)
      expect(user.email).toBeNull()
      expect(user.phoneNumber).toBeNull()
    })

    // verify we see the same thing if we access their user profile indirectly
    await ourClient.query({query: queries.ourFollowedUsers}).then(({data: {self: user}}) => {
      expect(user.followedUsers.items).toHaveLength(1)
      expect(user.followedUsers.items[0].followedStatus).toBe('FOLLOWING')
      expect(user.followedUsers.items[0].fullName).toBe(theirFullName)
      expect(user.followedUsers.items[0].bio).toBe(theirBio)
      expect(user.followedUsers.items[0].email).toBeNull()
      expect(user.followedUsers.items[0].phoneNumber).toBeNull()
    })

    // now deny the user's follow request, verify we can see less
    await theirClient.mutate({mutation: mutations.denyFollowerUser, variables: {userId: ourUserId}})
    await ourClient.query({query: queries.user, variables: {userId: theirUserId}}).then(({data: {user}}) => {
      expect(user.followedStatus).toBe('DENIED')
      expect(user.fullName).toBe(theirFullName)
      expect(user.bio).toBeNull()
      expect(user.email).toBeNull()
      expect(user.phoneNumber).toBeNull()
    })

    // verify we see the same thing if we access their user profile indirectly
    await ourClient
      .query({query: queries.ourFollowedUsers, variables: {followStatus: 'DENIED'}})
      .then(({data: {self: user}}) => {
        expect(user.followedUsers.items).toHaveLength(1)
        expect(user.followedUsers.items[0].followedStatus).toBe('DENIED')
        expect(user.followedUsers.items[0].fullName).toBe(theirFullName)
        expect(user.followedUsers.items[0].bio).toBeNull()
        expect(user.followedUsers.items[0].email).toBeNull()
        expect(user.followedUsers.items[0].phoneNumber).toBeNull()
      })

    // now accept the user's follow request, and then unfollow them
    await theirClient.mutate({mutation: mutations.acceptFollowerUser, variables: {userId: ourUserId}})
    await ourClient.mutate({mutation: mutations.unfollowUser, variables: {userId: theirUserId}})
    await ourClient.query({query: queries.user, variables: {userId: theirUserId}}).then(({data: {user}}) => {
      expect(user.followedStatus).toBe('NOT_FOLLOWING')
      expect(user.fullName).toBe(theirFullName)
      expect(user.bio).toBeNull()
      expect(user.email).toBeNull()
      expect(user.phoneNumber).toBeNull()
    })
  })
})

describe('wrapper to ensure cleanup 2', () => {
  const theirPhone = '+14155551212'
  let theirClient, theirUserId, theirEmail
  beforeAll(async () => {
    ;({client: theirClient, userId: theirUserId, email: theirEmail} = await cognito.getAppSyncLogin(theirPhone))
  })
  afterAll(async () => {
    if (theirClient) await theirClient.mutate({mutation: mutations.deleteUser})
  })

  test('Read properties of another public user', async () => {
    const {client: ourClient} = await loginCache.getCleanLogin()

    // set up another user in cognito, leave them as public
    const theirBio = 'keeping calm and carrying on'
    const theirFullName = 'HG Wells'
    await theirClient.mutate({
      mutation: mutations.setUserDetails,
      variables: {bio: theirBio, fullName: theirFullName},
    })

    // verify they can see all their properties (make sure they're all set correctly)
    await theirClient.query({query: queries.self}).then(({data: {self: user}}) => {
      expect(user.followedStatus).toBe('SELF')
      expect(user.followerStatus).toBe('SELF')
      expect(user.privacyStatus).toBe('PUBLIC')
      expect(user.fullName).toBe(theirFullName)
      expect(user.bio).toBe(theirBio)
      expect(user.email).toBe(theirEmail)
      expect(user.phoneNumber).toBe(theirPhone)
    })

    // verify that we can see info that is expected of a non-follower
    await ourClient.query({query: queries.user, variables: {userId: theirUserId}}).then(({data: {user}}) => {
      expect(user.followedStatus).toBe('NOT_FOLLOWING')
      expect(user.followerStatus).toBe('NOT_FOLLOWING')
      expect(user.privacyStatus).toBe('PUBLIC')
      expect(user.bio).toBe(theirBio)
      expect(user.fullName).toBe(theirFullName)
      expect(user.email).toBeNull()
      expect(user.phoneNumber).toBeNull()
    })

    // follow the user, and verify we still can't see stuff we shouldn't be able to
    await ourClient.mutate({mutation: mutations.followUser, variables: {userId: theirUserId}})
    await ourClient.query({query: queries.user, variables: {userId: theirUserId}}).then(({data: {user}}) => {
      expect(user.email).toBeNull()
      expect(user.phoneNumber).toBeNull()
    })

    // verify we can't see anything more if we access their user profile indirectly
    await ourClient.query({query: queries.ourFollowedUsers}).then(({data: {self: user}}) => {
      expect(user.followedUsers.items).toHaveLength(1)
      expect(user.followedUsers.items[0].followedStatus).toBe('FOLLOWING')
      expect(user.followedUsers.items[0].followerStatus).toBe('NOT_FOLLOWING')
      expect(user.followedUsers.items[0].privacyStatus).toBe('PUBLIC')
      expect(user.followedUsers.items[0].bio).toBe(theirBio)
      expect(user.followedUsers.items[0].fullName).toBe(theirFullName)
      expect(user.followedUsers.items[0].email).toBeNull()
      expect(user.followedUsers.items[0].phoneNumber).toBeNull()
    })
  })
})

test('User language code - get, set, privacy', async () => {
  const {client, userId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we should default to english
  let resp = await client.query({query: queries.user, variables: {userId}})
  expect(resp.data.user.languageCode).toBe('en')

  // we change our language code
  resp = await client.mutate({mutation: mutations.setUserLanguageCode, variables: {languageCode: 'de'}})
  expect(resp.data.setUserDetails.languageCode).toBe('de')

  // check another user can't see our language
  resp = await theirClient.query({query: queries.user, variables: {userId}})
  expect(resp.data.user.languageCode).toBeNull()
})

test('User theme code - get, set, privacy', async () => {
  const {client, userId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we should default to 'black.green'
  let resp = await client.query({query: queries.user, variables: {userId}})
  expect(resp.data.user.themeCode).toBe('black.green')

  // we change our theme code
  resp = await client.mutate({mutation: mutations.setUserThemeCode, variables: {themeCode: 'green.orange'}})
  expect(resp.data.setUserDetails.themeCode).toBe('green.orange')

  // we go to private
  resp = await client.mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
  expect(resp.data.setUserDetails.privacyStatus).toBe('PRIVATE')

  // Check to ensure another rando *can* see our themeCode
  // This is necessary because profile pics are planned to have some styling based on chosen theme
  resp = await theirClient.query({query: queries.user, variables: {userId}})
  expect(resp.data.user.themeCode).toBe('green.orange')
})

test('User accepted EULA version - get, set, privacy', async () => {
  const {client, userId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we should default to null
  let resp = await client.query({query: queries.self})
  expect(resp.data.self.acceptedEULAVersion).toBeNull()

  // we change our accepted version
  resp = await client.mutate({
    mutation: mutations.setUserAcceptedEULAVersion,
    variables: {version: '2019-11-14'},
  })
  expect(resp.data.setUserAcceptedEULAVersion.acceptedEULAVersion).toBe('2019-11-14')

  // check to make sure that version stuck
  resp = await client.query({query: queries.self})
  expect(resp.data.self.acceptedEULAVersion).toBe('2019-11-14')

  // check another user can't see our acepted version
  resp = await theirClient.query({query: queries.user, variables: {userId: userId}})
  expect(resp.data.user.acceptedEULAVersion).toBeNull()

  // check we can null out accepted version
  resp = await client.mutate({mutation: mutations.setUserAcceptedEULAVersion, variables: {version: ''}})
  expect(resp.data.setUserAcceptedEULAVersion.acceptedEULAVersion).toBeNull()

  // disable ourselves
  resp = await client.mutate({mutation: mutations.disableUser})
  expect(resp.data.disableUser.userId).toBe(userId)
  expect(resp.data.disableUser.userStatus).toBe('DISABLED')

  // verify we can no longer edit the EULA
  await expect(
    client.mutate({mutation: mutations.setUserAcceptedEULAVersion, variables: {version: '42'}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)
})

test('User commentsDisabled - get, set, privacy', async () => {
  const {client, userId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we should default to false
  let resp = await client.query({query: queries.self})
  expect(resp.data.self.commentsDisabled).toBe(false)

  // we change it
  let variables = {commentsDisabled: true}
  resp = await client.mutate({mutation: mutations.setUserMentalHealthSettings, variables})
  expect(resp.data.setUserDetails.commentsDisabled).toBe(true)

  // check to make sure that version stuck
  resp = await client.query({query: queries.self})
  expect(resp.data.self.commentsDisabled).toBe(true)

  // check another user can't see values
  resp = await theirClient.query({query: queries.user, variables: {userId: userId}})
  expect(resp.data.user.commentsDisabled).toBeNull()
})

test('User likesDisabled - get, set, privacy', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we should default to false
  await ourClient.query({query: queries.self}).then(({data}) => expect(data.self.likesDisabled).toBe(false))

  // we change it, verify that stuck
  await ourClient
    .mutate({mutation: mutations.setUserMentalHealthSettings, variables: {likesDisabled: true}})
    .then(({data}) => expect(data.setUserDetails.likesDisabled).toBe(true))
  await ourClient.query({query: queries.self}).then(({data}) => expect(data.self.likesDisabled).toBe(true))

  // check another user can't see our values
  await theirClient
    .query({query: queries.user, variables: {userId: ourUserId}})
    .then(({data}) => expect(data.user.likesDisabled).toBeNull())
})

test('User sharingDisabled - get, set, privacy', async () => {
  const {client, userId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we should default to false
  let resp = await client.query({query: queries.self})
  expect(resp.data.self.sharingDisabled).toBe(false)

  // we change it
  let variables = {sharingDisabled: true}
  resp = await client.mutate({mutation: mutations.setUserMentalHealthSettings, variables})
  expect(resp.data.setUserDetails.sharingDisabled).toBe(true)

  // check to make sure that version stuck
  resp = await client.query({query: queries.self})
  expect(resp.data.self.sharingDisabled).toBe(true)

  // check another user can't see values
  resp = await theirClient.query({query: queries.user, variables: {userId}})
  expect(resp.data.user.sharingDisabled).toBeNull()
})

test('User verificationHidden - get, set, privacy', async () => {
  const {client, userId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we should default to false
  let resp = await client.query({query: queries.self})
  expect(resp.data.self.verificationHidden).toBe(false)

  // we change it
  resp = await client.mutate({
    mutation: mutations.setUserMentalHealthSettings,
    variables: {verificationHidden: true},
  })
  expect(resp.data.setUserDetails.verificationHidden).toBe(true)

  // check to make sure that version stuck
  resp = await client.query({query: queries.self})
  expect(resp.data.self.verificationHidden).toBe(true)

  // check another user can't see values
  resp = await theirClient.query({query: queries.user, variables: {userId}})
  expect(resp.data.user.verificationHidden).toBeNull()
})

test('User setUserAPNSToken', async () => {
  const {client, userId} = await loginCache.getCleanLogin()

  // reading the APNS token is purposefully left out of the api, so verification here is limited

  // set it
  let resp = await client.mutate({mutation: mutations.setUserAPNSToken, variables: {token: 'apns-token'}})
  expect(resp.data.setUserAPNSToken.userId).toBe(userId)

  // change it
  resp = await client.mutate({mutation: mutations.setUserAPNSToken, variables: {token: 'apns-token-other'}})
  expect(resp.data.setUserAPNSToken.userId).toBe(userId)

  // delete it
  resp = await client.mutate({mutation: mutations.setUserAPNSToken, variables: {token: ''}})
  expect(resp.data.setUserAPNSToken.userId).toBe(userId)
})

test('Set and read properties(location, matchAgeRange, matchGenders, matchLocationRadius)', async () => {
  const {client: ourClient, userId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // set up another user in cognito, leave them as public
  const location = {latitude: 50.01, longitude: 50.01, accuracy: 50}
  const matchAgeRange = {min: 20, max: 50}
  const matchGenders = ['MALE', 'FEMALE']
  const matchLocationRadius = 20

  // Set match genders and location radius
  await ourClient.mutate({mutation: mutations.setUserDetails, variables: {matchGenders, matchLocationRadius}})

  // Set user age range
  await ourClient.mutate({mutation: mutations.setUserAgeRange, variables: matchAgeRange})

  // Set user location
  await ourClient.mutate({
    mutation: mutations.setUserLocation,
    variables: {latitude: location.latitude, longitude: location.longitude, accuracy: location.accuracy},
  })

  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(userId)
    expect(JSON.stringify(user.matchGenders)).toBe(JSON.stringify(matchGenders))
    expect(user.matchLocationRadius).toBe(matchLocationRadius)
    expect(user.matchAgeRange.min).toBe(matchAgeRange.min)
    expect(user.matchAgeRange.max).toBe(matchAgeRange.max)
    expect(user.location.latitude).toBe(location.latitude)
    expect(user.location.longitude).toBe(location.longitude)
    expect(user.location.accuracy).toBe(location.accuracy)
  })

  // check another user can't see values
  await theirClient.query({query: queries.user, variables: {userId}}).then(({data: {user}}) => {
    expect(user.userId).toBe(userId)
    expect(user.matchGenders).toBeNull()
    expect(user.matchLocationRadius).toBeNull()
    expect(user.location).toBeNull()
    expect(user.matchAgeRange).toBeNull()
  })

  // update location without accuracy, process and check if accuracy is null
  await ourClient.mutate({
    mutation: mutations.setUserLocation,
    variables: {latitude: location.latitude, longitude: location.longitude},
  })

  await ourClient.query({query: queries.user, variables: {userId}}).then(({data: {user}}) => {
    expect(user.userId).toBe(userId)
    expect(user.location.accuracy).toBeNull()
  })
})

test('Validate user properties(location, matchAgeRange, matchGenders, matchLocationRadius)', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const location = {latitude: 100, longitude: 200, accuracy: -10}
  const matchAgeRange = {min: 100, max: 50}
  const matchLocationRadius = 4
  const matchGenders = []

  // Validate match location radius
  await expect(
    ourClient.mutate({mutation: mutations.setUserDetails, variables: {matchLocationRadius}}),
  ).rejects.toThrow(/ClientError: matchLocationRadius should be greater than or equal to 5/)

  // Validate user age range
  await expect(ourClient.mutate({mutation: mutations.setUserAgeRange, variables: matchAgeRange})).rejects.toThrow(
    /ClientError: Invalid matchAgeRange/,
  )

  // Validate user location
  await expect(
    ourClient.mutate({
      mutation: mutations.setUserLocation,
      variables: {latitude: location.latitude, longitude: 50.01},
    }),
  ).rejects.toThrow(/ClientError: latitude should be in \[-90, 90\]/)

  await expect(
    ourClient.mutate({
      mutation: mutations.setUserLocation,
      variables: {latitude: 50.01, longitude: location.longitude},
    }),
  ).rejects.toThrow(/ClientError: longitude should be in \[-180, 180\]/)

  await expect(
    ourClient.mutate({
      mutation: mutations.setUserLocation,
      variables: {longitude: 50.01, latitude: 50.01, accuracy: location.accuracy},
    }),
  ).rejects.toThrow(/ClientError: accuracy should be greater than or equal to zero/)

  // Validate match genders
  await expect(ourClient.mutate({mutation: mutations.setUserDetails, variables: {matchGenders}})).rejects.toThrow(
    /ClientError: matchGenders cannot be empty/,
  )
})

test('Set and read properties(height, matchHeightRange)', async () => {
  const {client: ourClient, userId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // set up another user in cognito, leave them as public
  const height = 90
  const matchHeightRange = {min: 50, max: 110}

  // Set height and match height range
  await ourClient.mutate({mutation: mutations.setUserDetails, variables: {height, matchHeightRange}})

  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.userId).toBe(userId)
    expect(user.height).toBe(height)
    expect(user.matchHeightRange.min).toBe(matchHeightRange.min)
    expect(user.matchHeightRange.max).toBe(matchHeightRange.max)
  })

  // check another user can't see values
  await theirClient.query({query: queries.user, variables: {userId}}).then(({data: {user}}) => {
    expect(user.userId).toBe(userId)
    expect(user.height).toBe(height)
    expect(user.matchHeightRange).toBeNull()
  })
})

test('Validate user properties(height, matchHeightRange)', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const height = -10
  let matchHeightRange = {min: -10, max: 110}

  // Validate height
  await expect(ourClient.mutate({mutation: mutations.setUserDetails, variables: {height}})).rejects.toThrow(
    /ClientError: Invalid height/,
  )

  // Validate matchHeightRange
  await expect(
    ourClient.mutate({mutation: mutations.setUserDetails, variables: {matchHeightRange}}),
  ).rejects.toThrow(/ClientError: Invalid matchHeightRange/)

  matchHeightRange = {min: 50, max: 118}
  await expect(
    ourClient.mutate({mutation: mutations.setUserDetails, variables: {matchHeightRange}}),
  ).rejects.toThrow(/ClientError: Invalid matchHeightRange/)

  matchHeightRange = {min: 110, max: 50}
  await expect(
    ourClient.mutate({mutation: mutations.setUserDetails, variables: {matchHeightRange}}),
  ).rejects.toThrow(/ClientError: Invalid matchHeightRange/)
})
