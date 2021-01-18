# Backend Scripts

## Getting Started

You will need `yarn` and `node` already installed on your system. Run `yarn install` in this directory (or one level up) to install needed dependencies.

The scripts require a number of access keys and IDs of the target AWS environment (ie beta or production) to be set as environment variables. The easiest way to do this is to create a `.env` file in this directory. Example `.env` file:

```sh
# beta
AWS_REGION=us-east-1
APPSYNC_GRAPHQL_URL=https://somethingsomething
COGNITO_TESTING_CLIENT_ID=something
COGNITO_IDENTITY_POOL_ID=something
COGNITO_USER_POOL_ID=something

# production
# AWS_REGION=us-east-1
# APPSYNC_GRAPHQL_URL=https://somethingelsesomethingelse
# COGNITO_TESTING_CLIENT_ID=somethingelse
# COGNITO_IDENTITY_POOL_ID=somethingelse
# COGNITO_USER_POOL_ID=somethingelse
```

The values for thes env vars are available in the Outputs tab of the `real-main` CloudFormation stack via the AWS console.

- [CloudFormation Outputs tab for beta env](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/outputs?filteringText=&filteringStatus=active&viewNested=true&hideStacks=false&stackId=arn%3Aaws%3Acloudformation%3Aus-east-1%3A128661475706%3Astack%2Freal-dev-main%2Fdbfdbe60-e6cb-11e9-996c-0afcd132dcac)
- [CloudFormation Outputs tab for production env](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/outputs?filteringText=&filteringStatus=active&viewNested=true&hideStacks=false&stackId=arn%3Aaws%3Acloudformation%3Aus-east-1%3A128661475706%3Astack%2Freal-production-main%2Fab64ef30-ffd0-11e9-b3c2-0e8cd4caae34)

Or - ask your friendly backend developer, they have a copy this file already compiled on thier system.

## Scripts

### add-post-cognito-user.js

Add a new image image post. Will prompt the user for credentials and details about the post.
