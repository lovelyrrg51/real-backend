/**
 * This test suite cannot run in parrallel with others because it
 * depends on global state - namely the trending users/posts indexes.
 * Any call to addPost() in general will affect the trending indexes.
 */

const got = require('got')
const uuidv4 = require('uuid/v4')

const cognito = require('../utils/cognito')
const misc = require('../utils/misc')
const {mutations, queries} = require('../schema')

const imageData = misc.generateRandomJpeg(8, 8)
const imageDataB64 = new Buffer.from(imageData).toString('base64')
const imageData2B64 = new Buffer.from(misc.generateRandomJpeg(8, 8)).toString('base64')
const jpgHeaders = {'Content-Type': 'image/jpeg'}
const loginCache = new cognito.AppSyncLoginCache()
jest.retryTimes(1)

beforeAll(async () => {
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
  loginCache.addCleanLogin(await cognito.getAppSyncLogin())
})
beforeEach(async () => {
  await loginCache.clean()
  await misc.sleep(2000) // give dynamo handlers time to clean up trending indexes
})
afterAll(async () => await loginCache.reset())

test('Post lifecycle, visibility and trending', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // verify trending indexes start empty
  await ourClient.query({query: queries.allTrending}).then(({data: {trendingPosts, trendingUsers}}) => {
    expect(trendingPosts.items).toHaveLength(0)
    expect(trendingUsers.items).toHaveLength(0)
  })

  // we add a text-only post
  const postId1 = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: postId1, postType: 'TEXT_ONLY', text: 'lore ipsum'},
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('COMPLETED')
    })

  // they add an image post that will pass verification, but don't complete it yet
  const postId2 = uuidv4()
  const uploadUrl = await theirClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, takenInReal: true}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('PENDING')
      expect(post.image).toBeNull()
      return post.imageUploadUrl
    })

  // we check trending posts
  await ourClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(1)
    expect(trendingPosts.items[0].postId).toBe(postId1)
  })

  // they check trending posts
  await theirClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(1)
    expect(trendingPosts.items[0].postId).toBe(postId1)
  })

  // they upload the image, completing their post
  await got.put(uploadUrl, {headers: jpgHeaders, body: imageData})
  await misc.sleepUntilPostProcessed(theirClient, postId1)
  await misc.sleep(5000) // a bit more time for dynamo trending index converge

  // check that shows up in trending posts, their post should be on top
  await ourClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(2)
    expect(trendingPosts.items[0].postId).toBe(postId2)
    expect(trendingPosts.items[1].postId).toBe(postId1)
  })

  // check trending users still empty since the 'free trending point' doesn't apply to users
  await ourClient
    .query({query: queries.trendingUsers})
    .then(({data: {trendingUsers}}) => expect(trendingUsers.items).toHaveLength(0))

  // they archive their post
  await theirClient
    .mutate({mutation: mutations.archivePost, variables: {postId: postId2}})
    .then(({data: {archivePost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('ARCHIVED')
    })

  // their post should have disappeared from trending
  await theirClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(1)
    expect(trendingPosts.items[0].postId).toBe(postId1)
  })

  // they restore this post (trending score has been cleared)
  await theirClient
    .mutate({mutation: mutations.restoreArchivedPost, variables: {postId: postId2}})
    .then(({data: {restoreArchivedPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('COMPLETED')
    })

  // we delete our post
  await ourClient
    .mutate({mutation: mutations.deletePost, variables: {postId: postId1}})
    .then(({data: {deletePost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('DELETING')
    })

  // our post should have disappeared from trending, and theirs should not have re-appeared
  await ourClient
    .query({query: queries.trendingPosts})
    .then(({data: {trendingPosts}}) => expect(trendingPosts.items).toHaveLength(0))

  // check trending users, should be unaffected by post archiving & deleting
  await theirClient
    .query({query: queries.trendingUsers})
    .then(({data: {trendingUsers}}) => expect(trendingUsers.items).toHaveLength(0))
})

describe('wrapper to ensure cleanup', () => {
  let theirClient, theirUserId // user is deleted in this test case
  beforeAll(async () => {
    ;({client: theirClient, userId: theirUserId} = await cognito.getAppSyncLogin())
  })
  afterAll(async () => {
    if (theirClient) await theirClient.mutate({mutation: mutations.deleteUser})
  })

  test('Non-owner views contribute to trending, filter by viewedStatus, reset & delete clear trending', async () => {
    const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
    const {client: otherClient} = await loginCache.getCleanLogin()

    // verify trending indexes start empty
    await ourClient.query({query: queries.allTrending}).then(({data: {trendingPosts, trendingUsers}}) => {
      expect(trendingPosts.items).toHaveLength(0)
      expect(trendingUsers.items).toHaveLength(0)
    })

    // we add a post
    const postId1 = uuidv4()
    await ourClient
      .mutate({
        mutation: mutations.addPost,
        variables: {postId: postId1, postType: 'TEXT_ONLY', text: 'lore ipsum'},
      })
      .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId1))

    // they add a post
    await misc.sleep(1000) // ordering
    const postId2 = uuidv4()
    await theirClient
      .mutate({
        mutation: mutations.addPost,
        variables: {postId: postId2, postType: 'TEXT_ONLY', text: 'lore ipsum'},
      })
      .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId2))

    // both should show up in trending, in order with ours in the back
    await misc.sleep(2000)
    await otherClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
      expect(trendingPosts.items).toHaveLength(2)
      expect(trendingPosts.items[0].postId).toBe(postId2)
      expect(trendingPosts.items[1].postId).toBe(postId1)
    })

    // verify we can filter trending posts based on viewed status
    await ourClient
      .query({query: queries.trendingPosts, variables: {viewedStatus: 'VIEWED'}})
      .then(({data: {trendingPosts}}) => {
        expect(trendingPosts.items).toHaveLength(1)
        expect(trendingPosts.items[0].postId).toBe(postId1)
      })
    await ourClient
      .query({query: queries.trendingPosts, variables: {viewedStatus: 'NOT_VIEWED'}})
      .then(({data: {trendingPosts}}) => {
        expect(trendingPosts.items).toHaveLength(1)
        expect(trendingPosts.items[0].postId).toBe(postId2)
      })

    // trending users should be empty
    await otherClient
      .query({query: queries.trendingUsers})
      .then(({data: {trendingUsers}}) => expect(trendingUsers.items).toHaveLength(0))

    // we report to have viewed our own post
    await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId1]}})
    await misc.sleep(2000)

    // check no change in trending posts
    await otherClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
      expect(trendingPosts.items).toHaveLength(2)
      expect(trendingPosts.items[0].postId).toBe(postId2)
      expect(trendingPosts.items[1].postId).toBe(postId1)
    })

    // trending users should still be empty
    await otherClient
      .query({query: queries.trendingUsers})
      .then(({data: {trendingUsers}}) => expect(trendingUsers.items).toHaveLength(0))

    // they report to have viewed our post
    await theirClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId1]}})
    await misc.sleep(2000) // dynamo

    // trending posts should have flipped order
    await otherClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
      expect(trendingPosts.items).toHaveLength(2)
      expect(trendingPosts.items[0].postId).toBe(postId1)
      expect(trendingPosts.items[1].postId).toBe(postId2)
    })

    // we should be in trending users
    await otherClient.query({query: queries.trendingUsers}).then(({data: {trendingUsers}}) => {
      expect(trendingUsers.items).toHaveLength(1)
      expect(trendingUsers.items[0].userId).toBe(ourUserId)
    })

    // we report to have viewed our their post
    await ourClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId2]}})
    await misc.sleep(2000) // dynamo

    // trending posts should have flipped order again
    await otherClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
      expect(trendingPosts.items).toHaveLength(2)
      expect(trendingPosts.items[0].postId).toBe(postId2)
      expect(trendingPosts.items[1].postId).toBe(postId1)
    })

    // we should both be in trending users
    await otherClient.query({query: queries.trendingUsers}).then(({data: {trendingUsers}}) => {
      expect(trendingUsers.items).toHaveLength(2)
      expect(trendingUsers.items[0].userId).toBe(theirUserId)
      expect(trendingUsers.items[1].userId).toBe(ourUserId)
    })

    // they delete themselves
    await theirClient.mutate({mutation: mutations.deleteUser}).then(({data: {deleteUser: user}}) => {
      expect(user.userId).toBe(theirUserId)
      expect(user.userStatus).toBe('DELETING')
      theirClient = null
    })
    await misc.sleep(2000) // dynamo

    // verify their post has disappeared from trending
    await otherClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
      expect(trendingPosts.items).toHaveLength(1)
      expect(trendingPosts.items[0].postId).toBe(postId1)
    })

    // verify their user has disappeared from trending
    await otherClient.query({query: queries.trendingUsers}).then(({data: {trendingUsers}}) => {
      expect(trendingUsers.items).toHaveLength(1)
      expect(trendingUsers.items[0].userId).toBe(ourUserId)
    })

    // we reset ourselves
    await ourClient
      .mutate({mutation: mutations.resetUser})
      .then(({data: {resetUser: user}}) => expect(user.userId).toBe(ourUserId))

    // verify our post has disappeared from trending
    await otherClient
      .query({query: queries.trendingPosts})
      .then(({data: {trendingPosts}}) => expect(trendingPosts.items).toHaveLength(0))

    // verify our user has disappeared from trending
    await otherClient
      .query({query: queries.trendingUsers})
      .then(({data: {trendingUsers}}) => expect(trendingUsers.items).toHaveLength(0))
  })
})

test('Blocked, private post & user visibility of posts & users in trending', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()
  const {client: otherClient, userId: otherUserId} = await loginCache.getCleanLogin()

  // verify trending indexes start empty
  await ourClient.query({query: queries.allTrending}).then(({data: {trendingPosts, trendingUsers}}) => {
    expect(trendingPosts.items).toHaveLength(0)
    expect(trendingUsers.items).toHaveLength(0)
  })

  // we add a post
  const postId1 = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: postId1, postType: 'TEXT_ONLY', text: 'lore ipsum'},
    })
    .then(({data: {addPost: post}}) => expect(post.postId).toBe(postId1))

  // they report to have viewed our post
  await theirClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId1]}})
  await misc.sleep(2000) // dynamo

  // they see our post in trending
  await theirClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(1)
    expect(trendingPosts.items[0].postId).toBe(postId1)
  })

  // they see our user in trending
  await theirClient.query({query: queries.trendingUsers}).then(({data: {trendingUsers}}) => {
    expect(trendingUsers.items).toHaveLength(1)
    expect(trendingUsers.items[0].userId).toBe(ourUserId)
  })

  // other starts following us
  await otherClient
    .mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
    .then(({data: {followUser: user}}) => expect(user.followedStatus).toBe('FOLLOWING'))

  // we go private
  await ourClient
    .mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
    .then(({data: {setUserDetails: user}}) => {
      expect(user.userId).toBe(ourUserId)
      expect(user.privacyStatus).toBe('PRIVATE')
    })
  await misc.sleep(2000) // dynamo

  // verify they don't see our post in trending anymore
  await theirClient
    .query({query: queries.trendingPosts})
    .then(({data: {trendingPosts}}) => expect(trendingPosts.items).toHaveLength(0))

  // they see still see our user in trending
  await theirClient.query({query: queries.trendingUsers}).then(({data: {trendingUsers}}) => {
    expect(trendingUsers.items).toHaveLength(1)
    expect(trendingUsers.items[0].userId).toBe(ourUserId)
  })

  // verify other, who is following us, sees our post in trending
  await otherClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(1)
    expect(trendingPosts.items[0].postId).toBe(postId1)
  })

  // other also sees our user in trending
  await otherClient.query({query: queries.trendingUsers}).then(({data: {trendingUsers}}) => {
    expect(trendingUsers.items).toHaveLength(1)
    expect(trendingUsers.items[0].userId).toBe(ourUserId)
  })

  // we block other
  await ourClient
    .mutate({mutation: mutations.blockUser, variables: {userId: otherUserId}})
    .then(({data: {blockUser: user}}) => {
      expect(user.userId).toBe(otherUserId)
      expect(user.blockedStatus).toBe('BLOCKING')
    })
  await misc.sleep(2000) // dynamo

  // verify other no longer sees our post in trending
  await otherClient
    .query({query: queries.trendingPosts})
    .then(({data: {trendingPosts}}) => expect(trendingPosts.items).toHaveLength(0))

  // verify other no longer sees our user in trending
  await otherClient
    .query({query: queries.trendingUsers})
    .then(({data: {trendingUsers}}) => expect(trendingUsers.items).toHaveLength(0))
})

test('Posts that fail verification get lower trending scores, can be filtered', async () => {
  const {client} = await loginCache.getCleanLogin()

  // verify trending indexes start empty
  await client.query({query: queries.allTrending}).then(({data: {trendingPosts, trendingUsers}}) => {
    expect(trendingPosts.items).toHaveLength(0)
    expect(trendingUsers.items).toHaveLength(0)
  })

  // we add a post that is not verified
  const postId0 = uuidv4()
  await client
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: postId0, postType: 'TEXT_ONLY', text: 'lore ipsum'},
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId0)
      expect(post.postStatus).toBe('COMPLETED')
      expect(post.isVerified).toBeNull()
    })

  // we add a post that passes verification
  await misc.sleep(1000) // ordering
  const postId1 = uuidv4()
  await client
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: postId1, imageData: imageDataB64, takenInReal: true},
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('COMPLETED')
      expect(post.isVerified).toBe(true)
    })

  // we add a image post that fails verification
  await misc.sleep(1000) // ordering
  const postId2 = uuidv4()
  await client
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, imageData: imageData2B64}})
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('COMPLETED')
      expect(post.isVerified).toBe(false)
    })

  // check ordering. Even though the post that failed verification was more recent, it should appear
  // after the post that passed verification because it received less trending points
  await misc.sleep(2000)
  await client.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(3)
    expect(trendingPosts.items[0].postId).toBe(postId1)
    expect(trendingPosts.items[1].postId).toBe(postId0)
    expect(trendingPosts.items[2].postId).toBe(postId2)
  })

  // test filtering on verification status
  await client
    .query({query: queries.trendingPosts, variables: {isVerified: true}})
    .then(({data: {trendingPosts}}) => {
      expect(trendingPosts.items).toHaveLength(1)
      expect(trendingPosts.items[0].postId).toBe(postId1)
    })
  await client
    .query({query: queries.trendingPosts, variables: {isVerified: false}})
    .then(({data: {trendingPosts}}) => {
      expect(trendingPosts.items).toHaveLength(1)
      expect(trendingPosts.items[0].postId).toBe(postId2)
    })
})

test('Users with subscription get trending boost', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // verify trending indexes start empty
  await ourClient.query({query: queries.allTrending}).then(({data: {trendingPosts, trendingUsers}}) => {
    expect(trendingPosts.items).toHaveLength(0)
    expect(trendingUsers.items).toHaveLength(0)
  })

  // we give ourselves some free diamond
  await ourClient
    .mutate({mutation: mutations.grantUserSubscriptionBonus})
    .then(({data: {grantUserSubscriptionBonus: user}}) => {
      expect(user.userId).toBe(ourUserId)
      expect(user.subscriptionLevel).toBe('DIAMOND')
    })

  // we upload a post that fails verification (with a subscription)
  await misc.sleep(2000)
  const postId0 = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: postId0, imageData: imageDataB64, takenInReal: false},
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId0)
      expect(post.postStatus).toBe('COMPLETED')
      expect(post.isVerified).toBe(false)
    })

  // they upload a post that passes verification (without a subscription)
  await misc.sleep(1000)
  const postId1 = uuidv4()
  await theirClient
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: postId1, imageData: imageData2B64, takenInReal: true},
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('COMPLETED')
      expect(post.isVerified).toBe(true)
    })

  // check in trending ordering. Even though the 2nd post was more recent, and it passed
  // verification, the subscribers post should be ordered first in trending.
  await misc.sleep(2000)
  await ourClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(2)
    expect(trendingPosts.items[0].postId).toBe(postId0)
    expect(trendingPosts.items[1].postId).toBe(postId1)
  })
})

test('Views of non-original posts contribute to the original post & user in trending', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()
  const {client: otherClient} = await loginCache.getCleanLogin()

  // verify trending indexes start empty
  await ourClient.query({query: queries.allTrending}).then(({data: {trendingPosts, trendingUsers}}) => {
    expect(trendingPosts.items).toHaveLength(0)
    expect(trendingUsers.items).toHaveLength(0)
  })

  // they add an image post that will pass verification
  const postId1 = uuidv4()
  await theirClient
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: postId1, takenInReal: true, imageData: imageDataB64},
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId1)
      expect(post.postStatus).toBe('COMPLETED')
      expect(post.isVerified).toBe(true)
      expect(post.originalPost.postId).toBe(postId1)
    })

  // we add an image post that will have their post as the original post
  const postId2 = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: postId2, takenInReal: true, imageData: imageDataB64},
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId2)
      expect(post.postStatus).toBe('COMPLETED')
      expect(post.isVerified).toBe(true)
      expect(post.originalPost.postId).toBe(postId1)
    })

  // we add another post that will allow us to see changes in trending
  const postId3 = uuidv4()
  await ourClient
    .mutate({
      mutation: mutations.addPost,
      variables: {postId: postId3, postType: 'TEXT_ONLY', text: 'lore ipsum'},
    })
    .then(({data: {addPost: post}}) => {
      expect(post.postId).toBe(postId3)
      expect(post.postStatus).toBe('COMPLETED')
    })

  // the original post and the text post should be in trending, but not the non-original one
  await theirClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(2)
    expect(trendingPosts.items[0].postId).toBe(postId3)
    expect(trendingPosts.items[1].postId).toBe(postId1)
  })

  // no users should be trending yet
  await theirClient
    .query({query: queries.trendingUsers})
    .then(({data: {trendingUsers}}) => expect(trendingUsers.items).toHaveLength(0))

  // they report to have viewed our non-original post
  await theirClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId2]}})
  await misc.sleep(2000) // dynamo

  // trending posts should not have changed, because:
  //  - non-original post can't enter trending
  //  - they own the original post, so their view doesn't count for it
  await theirClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(2)
    expect(trendingPosts.items[0].postId).toBe(postId3)
    expect(trendingPosts.items[1].postId).toBe(postId1)
  })

  // no users should be trending yet
  await theirClient
    .query({query: queries.trendingUsers})
    .then(({data: {trendingUsers}}) => expect(trendingUsers.items).toHaveLength(0))

  // other reports to have viewed our non-original post
  await otherClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId2]}})
  await misc.sleep(2000) // dynamo

  // other's view should have been contributed to the original post moving up in trending
  await theirClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(2)
    expect(trendingPosts.items[0].postId).toBe(postId1)
    expect(trendingPosts.items[1].postId).toBe(postId3)
  })

  // they (who own the original post) should now appear as a trending user
  await theirClient.query({query: queries.trendingUsers}).then(({data: {trendingUsers}}) => {
    expect(trendingUsers.items).toHaveLength(1)
    expect(trendingUsers.items[0].userId).toBe(theirUserId)
  })
})

test('Only first view of a post counts for trending', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()
  const {client: otherClient} = await loginCache.getCleanLogin()

  // verify trending indexes start empty
  await ourClient.query({query: queries.allTrending}).then(({data: {trendingPosts, trendingUsers}}) => {
    expect(trendingPosts.items).toHaveLength(0)
    expect(trendingUsers.items).toHaveLength(0)
  })

  // we add two posts
  const [postId1, postId2] = [uuidv4(), uuidv4()]
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, postType: 'TEXT_ONLY', text: 'first!'}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, postType: 'TEXT_ONLY', text: '2nd!'}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))

  // they view the first post, pause, then view the other
  await theirClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId1]}})
  await misc.sleep(1000)
  await theirClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId2]}})

  // verify they show up in expected order in trending: most recently viewed should come first
  await misc.sleep(2000)
  await ourClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(2)
    expect(trendingPosts.items[0].postId).toBe(postId2)
    expect(trendingPosts.items[1].postId).toBe(postId1)
  })

  // they record another view on the first post, verify that does _not_ change trending order
  await theirClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId1]}})
  await misc.sleep(2000)
  await ourClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(2)
    expect(trendingPosts.items[0].postId).toBe(postId2)
    expect(trendingPosts.items[1].postId).toBe(postId1)
  })

  // other records another view on the first post, verify that does change trending order
  await otherClient.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId1]}})
  await misc.sleep(2000)
  await ourClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(2)
    expect(trendingPosts.items[0].postId).toBe(postId1)
    expect(trendingPosts.items[1].postId).toBe(postId2)
  })
})

test('Report with FOCUS view type, order of posts in the trending index', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: other1Client} = await loginCache.getCleanLogin()
  const {client: other2Client} = await loginCache.getCleanLogin()
  const {client: other3Client} = await loginCache.getCleanLogin()

  // verify trending indexes start empty
  await ourClient.query({query: queries.allTrending}).then(({data: {trendingPosts, trendingUsers}}) => {
    expect(trendingPosts.items).toHaveLength(0)
    expect(trendingUsers.items).toHaveLength(0)
  })

  // we add three posts, with sleeps so we have determinant trending order
  const [postId1, postId2, postId3] = [uuidv4(), uuidv4(), uuidv4()]
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId1, postType: 'TEXT_ONLY', text: 'first!'}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))
  await misc.sleep(1000)
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId2, postType: 'TEXT_ONLY', text: '2nd!'}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))
  await misc.sleep(1000)
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: postId3, postType: 'TEXT_ONLY', text: '3rd!'}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))

  // other1 & other2 view the second post
  await other1Client.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId2]}})
  await other2Client.mutate({mutation: mutations.reportPostViews, variables: {postIds: [postId2]}})

  // verify trending order
  await misc.sleep(2000)
  await ourClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(3)
    expect(trendingPosts.items[0].postId).toBe(postId2)
    expect(trendingPosts.items[1].postId).toBe(postId3)
    expect(trendingPosts.items[2].postId).toBe(postId1)
  })

  // other3 FOCUS views the first post and THUMBNAIL views the third
  await other3Client.mutate({
    mutation: mutations.reportPostViews,
    variables: {postIds: [postId1], viewType: 'FOCUS'},
  })
  await other3Client.mutate({
    mutation: mutations.reportPostViews,
    variables: {postIds: [postId1], viewType: 'THUMBNAIL'},
  })

  // verify the new trending order. Post that got the FOCUS view has jumped ahead of post2
  // while the post that got the THUMBNAIL view didn't get enough points to do so
  await misc.sleep(2000)
  await ourClient.query({query: queries.trendingPosts}).then(({data: {trendingPosts}}) => {
    expect(trendingPosts.items).toHaveLength(3)
    expect(trendingPosts.items[0].postId).toBe(postId1)
    expect(trendingPosts.items[1].postId).toBe(postId2)
    expect(trendingPosts.items[2].postId).toBe(postId3)
  })
})
