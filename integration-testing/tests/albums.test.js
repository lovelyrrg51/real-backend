const got = require('got')
const moment = require('moment')
const uuidv4 = require('uuid/v4')

const cognito = require('../utils/cognito')
const misc = require('../utils/misc')
const {mutations, queries} = require('../schema')

let anonClient
const imageBytes = misc.generateRandomJpeg(8, 8)
const imageData = new Buffer.from(imageBytes).toString('base64')
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

test('Add, read, and delete an album', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // we add an album with minimal options
  const albumId = uuidv4()
  const orgAlbum = await (async () => {
    const before = moment().toISOString()
    const resp = await ourClient.mutate({mutation: mutations.addAlbum, variables: {albumId, name: 'album name'}})
    const after = moment().toISOString()
    return {before, album: resp.data.addAlbum, after}
  })().then(({before, album, after}) => {
    expect(album.albumId).toBe(albumId)
    expect(album.name).toBe('album name')
    expect(album.description).toBeNull()
    expect(album.art.url).toBeTruthy()
    expect(album.art.url4k).toBeTruthy()
    expect(album.art.url1080p).toBeTruthy()
    expect(album.art.url480p).toBeTruthy()
    expect(album.art.url64p).toBeTruthy()
    expect(album.postCount).toBe(0)
    expect(album.postsLastUpdatedAt).toBeNull()
    expect(album.posts.items).toHaveLength(0)
    expect(before <= album.createdAt).toBe(true)
    expect(after >= album.createdAt).toBe(true)
    return album
  })

  // read that album via direct access
  await ourClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album).toEqual(orgAlbum))

  // delete the album
  await ourClient
    .mutate({mutation: mutations.deleteAlbum, variables: {albumId}})
    .then(({data: {deleteAlbum: album}}) => expect(album).toEqual(orgAlbum))

  // check its really gone
  await ourClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album).toBeNull())
})

test('Cannot add, edit or delete an album if we are disabled', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()

  // we add an album with minimal options
  const albumId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId, name: 'n'}})
    .then(({data: {addAlbum: album}}) => expect(album.albumId).toEqual(albumId))

  // we disable ourselves
  await ourClient.mutate({mutation: mutations.disableUser}).then(({data: {disableUser: user}}) => {
    expect(user.userId).toBe(ourUserId)
    expect(user.userStatus).toBe('DISABLED')
  })

  // verify we can't add another album
  await expect(
    ourClient.mutate({mutation: mutations.addAlbum, variables: {albumId: uuidv4(), name: 'n'}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)

  // verify we can't edit or delete the existing album
  await expect(
    ourClient.mutate({mutation: mutations.editAlbum, variables: {albumId, name: 'new'}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)
  await expect(ourClient.mutate({mutation: mutations.deleteAlbum, variables: {albumId}})).rejects.toThrow(
    /ClientError: User .* is not ACTIVE/,
  )
})

test('Anonymous user cannot add an album', async () => {
  ;({client: anonClient} = await cognito.getAnonymousAppSyncLogin())
  await expect(
    anonClient.mutate({mutation: mutations.addAlbum, variables: {albumId: uuidv4(), name: 'name'}}),
  ).rejects.toThrow(/ClientError: User .* is not ACTIVE/)
})

test('Add album with empty string description, treated as null', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const albumId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId, name: 'r', description: ''}})
    .then(({data: {addAlbum: album}}) => {
      expect(album.albumId).toBe(albumId)
      expect(album.description).toBeNull()
    })
})

test('Edit an album', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // we add an album with maximal options
  const albumId = uuidv4()
  const orgAlbum = await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId, name: 'org name', description: 'org desc'}})
    .then(({data: {addAlbum: album}}) => {
      expect(album.albumId).toBe(albumId)
      expect(album.name).toBe('org name')
      expect(album.description).toBe('org desc')
      return album
    })

  // edit the options on that album
  const editedAlbum = await ourClient
    .mutate({mutation: mutations.editAlbum, variables: {albumId, name: 'new name', description: 'new desc'}})
    .then(({data: {editAlbum: album}}) => {
      expect(album.albumId).toBe(albumId)
      expect(album.name).toBe('new name')
      expect(album.description).toBe('new desc')
      expect({
        ...album,
        ...{name: orgAlbum.name, description: orgAlbum.description},
      }).toEqual(orgAlbum)
      return album
    })

  // verify those stuck in the DB
  await ourClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album).toEqual(editedAlbum))

  // delete the options which we can on that album, using empty string
  const clearedAlbum = await ourClient
    .mutate({mutation: mutations.editAlbum, variables: {albumId, description: ''}})
    .then(({data: {editAlbum: album}}) => {
      expect(album.albumId).toBe(albumId)
      expect(album.name).toBe('new name')
      expect(album.description).toBeNull()
      expect({
        ...album,
        ...{description: editedAlbum.description},
      }).toEqual(editedAlbum)
      return album
    })

  // verify those stuck in the DB
  await ourClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album).toEqual(clearedAlbum))

  // verify we can't null out the album name
  await expect(ourClient.mutate({mutation: mutations.editAlbum, variables: {albumId, name: ''}})).rejects.toThrow(
    /ClientError: All albums must have names/,
  )
})

test('Cant create two albums with same id', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add an album
  const albumId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId, name: 'n'}})
    .then(({data: {addAlbum: album}}) => expect(album.albumId).toBe(albumId))

  // verify neither us nor them can add another album with same id
  const operation = {mutation: mutations.addAlbum, variables: {albumId, name: 'r'}}
  await expect(ourClient.mutate(operation)).rejects.toThrow(/ClientError: Album .* already exists/)
  await expect(theirClient.mutate(operation)).rejects.toThrow(/ClientError: Album .* already exists/)
})

test('Cant edit or delete somebody elses album', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const {client: theirClient} = await loginCache.getCleanLogin()

  // we add an album
  const albumId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId, name: 'n'}})
    .then(({data: {addAlbum: album}}) => expect(album.albumId).toBe(albumId))

  // verify they can't edit it nor delete it
  await expect(
    theirClient.mutate({mutation: mutations.editAlbum, variables: {albumId, name: 'name'}}),
  ).rejects.toThrow(/ClientError: Caller .* does not own Album /)
  await expect(theirClient.mutate({mutation: mutations.deleteAlbum, variables: {albumId}})).rejects.toThrow(
    /ClientError: Caller .* does not own Album /,
  )

  // verify it's still there
  await theirClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album.albumId).toBe(albumId))
})

test('Empty album edit raises error', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // we add an album
  const albumId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId, name: 'n'}})
    .then(({data: {addAlbum: album}}) => expect(album.albumId).toBe(albumId))

  // verify calling edit without specifying anything to edit is an error
  await expect(ourClient.mutate({mutation: mutations.editAlbum, variables: {albumId}})).rejects.toThrow(
    /ClientError: Called without any arguments/,
  )
})

test('Cant edit, delete an album that doesnt exist', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()
  const albumId = uuidv4() // doesnt exist

  // cant edit or delete the non-existing album
  await expect(
    ourClient.mutate({mutation: mutations.editAlbum, variables: {albumId, name: 'name'}}),
  ).rejects.toThrow(/ClientError: Album .* does not exist/)
  await expect(ourClient.mutate({mutation: mutations.deleteAlbum, variables: {albumId}})).rejects.toThrow(
    /ClientError: Album .* does not exist/,
  )
})

test('User.albums and Query.album block privacy', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // we add an album
  const albumId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId, name: 'n'}})
    .then(({data: {addAlbum: album}}) => expect(album.albumId).toBe(albumId))

  // check they can see our albums
  await misc.sleep(2000)
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.albumCount).toBe(1)
    expect(user.albums.items).toHaveLength(1)
  })

  // check they can see the album directly
  await theirClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album.albumId).toBe(albumId))

  // we block them
  await ourClient
    .mutate({mutation: mutations.blockUser, variables: {userId: theirUserId}})
    .then(({data: {blockUser: user}}) => {
      expect(user.userId).toBe(theirUserId)
      expect(user.blockedStatus).toBe('BLOCKING')
    })

  // check they cannot see our albums
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.albumCount).toBeNull()
    expect(user.albums).toBeNull()
  })

  // check they cannot see the album directly
  await theirClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album).toBeNull())

  // we unblock them
  await ourClient
    .mutate({mutation: mutations.unblockUser, variables: {userId: theirUserId}})
    .then(({data: {unblockUser: user}}) => {
      expect(user.userId).toBe(theirUserId)
      expect(user.blockedStatus).toBe('NOT_BLOCKING')
    })

  // check they can see our albums
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.albumCount).toBe(1)
    expect(user.albums.items).toHaveLength(1)
  })

  // check they can see the album directly
  await theirClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album.albumId).toBe(albumId))
})

test('User.albums and Query.album private user privacy', async () => {
  const {client: ourClient, userId: ourUserId} = await loginCache.getCleanLogin()
  const {client: theirClient, userId: theirUserId} = await loginCache.getCleanLogin()

  // check they *can* see our albums
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.albumCount).toBe(0)
    expect(user.albums.items).toHaveLength(0)
  })

  // we go private
  await ourClient
    .mutate({mutation: mutations.setUserPrivacyStatus, variables: {privacyStatus: 'PRIVATE'}})
    .then(({data: {setUserDetails: user}}) => expect(user.privacyStatus).toBe('PRIVATE'))

  // we add an album
  const albumId = uuidv4()
  await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId, name: 'n'}})
    .then(({data: {addAlbum: album}}) => expect(album.albumId).toBe(albumId))

  // check they cannot see our albums
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.albumCount).toBeNull()
    expect(user.albums).toBeNull()
  })

  // check they cannot see the album directly
  await theirClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album).toBeNull())

  // they request to follow us
  await theirClient
    .mutate({mutation: mutations.followUser, variables: {userId: ourUserId}})
    .then(({data: {followUser: user}}) => expect(user.followedStatus).toBe('REQUESTED'))

  // check they cannot see our albums
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.albumCount).toBeNull()
    expect(user.albums).toBeNull()
  })

  // check they cannot see the album directly
  await theirClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album).toBeNull())

  // we accept their follow request
  await ourClient
    .mutate({mutation: mutations.acceptFollowerUser, variables: {userId: theirUserId}})
    .then(({data: {acceptFollowerUser: user}}) => expect(user.followerStatus).toBe('FOLLOWING'))

  // check they *can* see our albums
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.albumCount).toBe(1)
    expect(user.albums.items).toHaveLength(1)
  })

  // check they *can* see the album directly
  await theirClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album.albumId).toBe(albumId))

  // now we deny their follow request
  await ourClient
    .mutate({mutation: mutations.denyFollowerUser, variables: {userId: theirUserId}})
    .then(({data: {denyFollowerUser: user}}) => expect(user.followerStatus).toBe('DENIED'))

  // check they cannot see our albums
  await theirClient.query({query: queries.user, variables: {userId: ourUserId}}).then(({data: {user}}) => {
    expect(user.albumCount).toBeNull()
    expect(user.albums).toBeNull()
  })

  // check they cannot see the album directly
  await theirClient
    .query({query: queries.album, variables: {albumId}})
    .then(({data: {album}}) => expect(album).toBeNull())
})

test('User.albums matches direct access, ordering', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // check we have no albums
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.albumCount).toBe(0)
    expect(user.albums.items).toHaveLength(0)
  })

  // we add two albums - one minimal one maximal
  const [albumId1, albumId2] = [uuidv4(), uuidv4()]
  const album1 = await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId: albumId1, name: 'n1'}})
    .then(({data: {addAlbum: album}}) => {
      expect(album.albumId).toBe(albumId1)
      return album
    })
  const album2 = await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId: albumId2, name: 'n2', description: 'd'}})
    .then(({data: {addAlbum: album}}) => {
      expect(album.albumId).toBe(albumId2)
      return album
    })

  // check they appear correctly in User.albums
  await misc.sleep(2000)
  await ourClient.query({query: queries.self}).then(({data: {self: user}}) => {
    expect(user.albumCount).toBe(2)
    expect(user.albums.items).toHaveLength(2)
    expect(user.albums.items[0]).toEqual(album1)
    expect(user.albums.items[1]).toEqual(album2)
  })
  await ourClient.query({query: queries.self, variables: {albumsReverse: true}}).then(({data: {self: user}}) => {
    expect(user.albumCount).toBe(2)
    expect(user.albums.items).toHaveLength(2)
    expect(user.albums.items[0]).toEqual(album2)
    expect(user.albums.items[1]).toEqual(album1)
  })
})

test('Album art generated for 0, 1 and 4 posts in album', async () => {
  const {client: ourClient} = await loginCache.getCleanLogin()

  // we an album
  const albumId = uuidv4()
  const albumNoPosts = await ourClient
    .mutate({mutation: mutations.addAlbum, variables: {albumId, name: 'n1'}})
    .then(async ({data: {addAlbum: album}}) => {
      expect(album.albumId).toBe(albumId)
      expect(album.art.url).toBeTruthy()
      expect(album.art.url4k).toBeTruthy()
      expect(album.art.url1080p).toBeTruthy()
      expect(album.art.url480p).toBeTruthy()
      expect(album.art.url64p).toBeTruthy()
      // check we can access the art urls. these will throw an error if response code is not 2XX
      await got.head(album.art.url)
      await got.head(album.art.url4k)
      await got.head(album.art.url1080p)
      await got.head(album.art.url480p)
      await got.head(album.art.url64p)
      return album
    })

  // add a post to that album
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: uuidv4(), albumId, imageData}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))

  // check album has art urls and they have changed root
  await misc.sleep(2000)
  const albumOnePost = await ourClient
    .query({query: queries.album, variables: {albumId}})
    .then(async ({data: {album}}) => {
      expect(album.albumId).toBe(albumId)
      expect(album.art.url).toBeTruthy()
      expect(album.art.url4k).toBeTruthy()
      expect(album.art.url1080p).toBeTruthy()
      expect(album.art.url480p).toBeTruthy()
      expect(album.art.url64p).toBeTruthy()
      expect(album.art.url.split('?')[0]).not.toBe(albumNoPosts.art.url.split('?')[0])
      expect(album.art.url4k.split('?')[0]).not.toBe(albumNoPosts.art.url4k.split('?')[0])
      expect(album.art.url1080p.split('?')[0]).not.toBe(albumNoPosts.art.url1080p.split('?')[0])
      expect(album.art.url480p.split('?')[0]).not.toBe(albumNoPosts.art.url480p.split('?')[0])
      expect(album.art.url64p.split('?')[0]).not.toBe(albumNoPosts.art.url64p.split('?')[0])
      // check we can access those urls
      await got.head(album.art.url)
      await got.head(album.art.url4k)
      await got.head(album.art.url1080p)
      await got.head(album.art.url480p)
      await got.head(album.art.url64p)
      return album
    })

  // add a second and third post to that album
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: uuidv4(), albumId, imageData}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))

  // add a third post to that album
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: uuidv4(), albumId, imageData}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))

  // check album has art urls that have not changed root
  await misc.sleep(4000)
  await ourClient.query({query: queries.album, variables: {albumId}}).then(({data: {album}}) => {
    expect(album.albumId).toBe(albumId)
    expect(album.art.url).toBeTruthy()
    expect(album.art.url4k).toBeTruthy()
    expect(album.art.url1080p).toBeTruthy()
    expect(album.art.url480p).toBeTruthy()
    expect(album.art.url64p).toBeTruthy()
    expect(album.art.url.split('?')[0]).toBe(albumOnePost.art.url.split('?')[0])
    expect(album.art.url4k.split('?')[0]).toBe(albumOnePost.art.url4k.split('?')[0])
    expect(album.art.url1080p.split('?')[0]).toBe(albumOnePost.art.url1080p.split('?')[0])
    expect(album.art.url480p.split('?')[0]).toBe(albumOnePost.art.url480p.split('?')[0])
    expect(album.art.url64p.split('?')[0]).toBe(albumOnePost.art.url64p.split('?')[0])
  })

  // add a fourth post to that album
  await ourClient
    .mutate({mutation: mutations.addPost, variables: {postId: uuidv4(), albumId, imageData}})
    .then(({data: {addPost: post}}) => expect(post.postStatus).toBe('COMPLETED'))

  // check album has art urls that have changed root
  await misc.sleep(4000)
  await ourClient.query({query: queries.album, variables: {albumId}}).then(async ({data: {album}}) => {
    expect(album.albumId).toBe(albumId)
    expect(album.art.url).toBeTruthy()
    expect(album.art.url4k).toBeTruthy()
    expect(album.art.url1080p).toBeTruthy()
    expect(album.art.url480p).toBeTruthy()
    expect(album.art.url64p).toBeTruthy()
    expect(album.art.url.split('?')[0]).not.toBe(albumOnePost.art.url.split('?')[0])
    expect(album.art.url4k.split('?')[0]).not.toBe(albumOnePost.art.url4k.split('?')[0])
    expect(album.art.url1080p.split('?')[0]).not.toBe(albumOnePost.art.url1080p.split('?')[0])
    expect(album.art.url480p.split('?')[0]).not.toBe(albumOnePost.art.url480p.split('?')[0])
    expect(album.art.url64p.split('?')[0]).not.toBe(albumOnePost.art.url64p.split('?')[0])
    // check we can access those urls
    await got.head(album.art.url)
    await got.head(album.art.url4k)
    await got.head(album.art.url1080p)
    await got.head(album.art.url480p)
    await got.head(album.art.url64p)
  })
})
