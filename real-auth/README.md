# real-auth

## API

The api root url varies by deployment (ie beta, production) and is available as the CloudFormation output `ServiceEndpoint` on this stack. It can be retrieved via `AWS Console -> CloudFormation -> real-XXX-auth -> Outputs -> ServiceEndpoint`

### Authentication

Clients must authenticate by passing an api key in the `x-api-key` header. Ex:

```
curl -H 'x-api-key: <the-api-key>' https://<the-api-root>/some-resource
```

Api keys are available via `AWS Console -> Api Gateway -> API Keys -> real-XXX-auth-XXX -> Show`

### Client Errors

Responses to client-side errors will have status code of `4XX` and json body of the following form:

```
{
  "message": "An explanation of what the client did wrong"
}
```

### Server Errors

Responses to server-side errors will have status code of `5XX`, and body of undefined format.

### Resources

Example request/response cycles are provided below for each resource.

#### POST `user/confirm`

```sh
curl -H 'x-api-key: <the-api-key>' -X POST \
  'https://<the-api-root>/user/confirm?userId=<user-id>&code=<confirmation-code>'
```

```sh
200 OK
{
  "tokens": {
    "AccessToken": <string>,
    "ExpiresIn": 3600,
    "TokenType": "Bearer",
    "RefreshToken": <string>,
    "IdToken": <string>,
  },
  "credentials": {
    "AccessKeyId": <string>,
    "SecretKey": <string>,
    "SessionToken": <string>,
    "Expiration": <string>
  }
}
```

#### GET `username/status`

```sh
curl -H 'x-api-key: <the-api-key>' \
  'https://<the-api-root>/username/status?username=<username-to-check>'
```

```sh
200 OK
{
  "status": "AVAILABLE" | "NOT_AVAILABLE" | "INVALID"
}
```
